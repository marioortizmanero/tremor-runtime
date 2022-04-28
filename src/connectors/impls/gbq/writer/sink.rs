// Copyright 2021, The Tremor Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::connectors::impls::gbq::writer::Config;
use crate::connectors::prelude::*;
use async_std::prelude::{FutureExt, StreamExt};
use futures::stream;
use googapis::google::cloud::bigquery::storage::v1::append_rows_request::ProtoData;
use googapis::google::cloud::bigquery::storage::v1::big_query_write_client::BigQueryWriteClient;
use googapis::google::cloud::bigquery::storage::v1::table_field_schema::Type as TableType;
use googapis::google::cloud::bigquery::storage::v1::{
    append_rows_request, table_field_schema, write_stream, AppendRowsRequest,
    CreateWriteStreamRequest, ProtoRows, ProtoSchema, TableFieldSchema, WriteStream,
};
use gouth::Token;
use prost::encoding::WireType;
use prost_types::{field_descriptor_proto, DescriptorProto, FieldDescriptorProto};
use std::collections::HashMap;
use std::time::Duration;
use tonic::codegen::InterceptedService;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::Interceptor;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};
use tonic::{Request, Status};

pub(crate) struct GbqSink {
    client: Option<BigQueryWriteClient<InterceptedService<Channel, AuthInterceptor>>>,
    write_stream: Option<WriteStream>,
    mapping: Option<JsonToProtobufMapping>,
    config: Config,
}

pub(crate) struct AuthInterceptor {
    token: MetadataValue<Ascii>,
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> ::std::result::Result<Request<()>, Status> {
        request
            .metadata_mut()
            .insert("authorization", self.token.clone());

        Ok(request)
    }
}

struct Field {
    table_type: TableType,
    tag: u32,

    // ignored if the table_type is not struct
    subfields: HashMap<String, Field>,
}

struct JsonToProtobufMapping {
    fields: HashMap<String, Field>,
    descriptor: DescriptorProto,
}

fn map_field(
    schema_name: &str,
    raw_fields: &Vec<TableFieldSchema>,
) -> (DescriptorProto, HashMap<String, Field>) {
    // The capacity for nested_types isn't known here, as it depends on the number of fields that have the struct type
    let mut nested_types = vec![];
    let mut proto_fields = Vec::with_capacity(raw_fields.len());
    let mut fields = HashMap::with_capacity(raw_fields.len());
    let mut tag: u16 = 1;

    for raw_field in raw_fields {
        let mut type_name = None;
        let mut subfields = HashMap::with_capacity(raw_field.fields.len());

        let table_type =
            if let Some(table_type) = table_field_schema::Type::from_i32(raw_field.r#type) {
                warn!("Found a field of unknown type: {}", raw_field.name);
                table_type
            } else {
                continue;
            };

        let grpc_type = match table_type {
            table_field_schema::Type::Int64 => field_descriptor_proto::Type::Int64,
            TableType::Double => field_descriptor_proto::Type::Double,
            TableType::Bool => field_descriptor_proto::Type::Bool,
            TableType::Bytes => field_descriptor_proto::Type::Bytes,


            TableType::String
            // YYYY-[M]M-[D]D
            | TableType::Date
            // [H]H:[M]M:[S]S[.DDDDDD|.F]
            | TableType::Time
            // YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.F]]
            | TableType::Datetime
            // The GEOGRAPHY type is based on the OGC Simple Features specification (SFS)
            | TableType::Geography
            // String, because it's a precise, f32/f64 would lose precision
            | TableType::Numeric
            | TableType::Bignumeric
            // [sign]Y-M [sign]D [sign]H:M:S[.F]
            | TableType::Interval
            | TableType::Json
            // YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.F]][time zone]
            | TableType::Timestamp => field_descriptor_proto::Type::String,
            TableType::Struct => {
                let type_name_for_field = format!("struct_{}", raw_field.name);
                let mapped = map_field(&type_name_for_field, &raw_field.fields);
                nested_types.push(mapped.0);
                subfields = mapped.1;

                type_name = Some(type_name_for_field);
                field_descriptor_proto::Type::Message
            }

            TableType::Unspecified => {
                warn!("Found a field of unspecified type: {}", raw_field.name);
                continue;
            }
        };

        proto_fields.push(FieldDescriptorProto {
            name: Some(raw_field.name.to_string()),
            number: Some(i32::from(tag)),
            label: None,
            r#type: Some(i32::from(grpc_type)),
            type_name,
            extendee: None,
            default_value: None,
            oneof_index: None,
            json_name: None,
            options: None,
            proto3_optional: None,
        });

        fields.insert(
            raw_field.name.to_string(),
            Field {
                table_type,
                tag: u32::from(tag),
                subfields,
            },
        );

        tag += 1;
    }

    (
        DescriptorProto {
            name: Some(schema_name.to_string()),
            field: proto_fields,
            extension: vec![],
            nested_type: nested_types,
            enum_type: vec![],
            extension_range: vec![],
            oneof_decl: vec![],
            options: None,
            reserved_range: vec![],
            reserved_name: vec![],
        },
        fields,
    )
}

fn encode_field(val: &Value, field: &Field, result: &mut Vec<u8>) -> Result<()> {
    let tag = field.tag;

    // fixme check which fields are required and fail if they're missing
    // fixme do not panic if the tremor type does not match
    match field.table_type {
        TableType::Double => prost::encoding::double::encode(
            tag,
            &val.as_f64()
                .ok_or_else(|| ErrorKind::BigQueryTypeMismatch("f64", val.value_type()))?,
            result,
        ),
        TableType::Int64 => prost::encoding::int64::encode(
            tag,
            &val.as_i64()
                .ok_or_else(|| ErrorKind::BigQueryTypeMismatch("i64", val.value_type()))?,
            result,
        ),
        TableType::Bool => prost::encoding::bool::encode(
            tag,
            &val.as_bool()
                .ok_or_else(|| ErrorKind::BigQueryTypeMismatch("bool", val.value_type()))?,
            result,
        ),
        TableType::String
        | TableType::Date
        | TableType::Time
        | TableType::Datetime
        | TableType::Timestamp
        | TableType::Numeric
        | TableType::Bignumeric
        | TableType::Geography => {
            prost::encoding::string::encode(
                tag,
                &val.as_str()
                    .ok_or_else(|| ErrorKind::BigQueryTypeMismatch("string", val.value_type()))?
                    .to_string(),
                result,
            );
        }
        TableType::Struct => {
            let mut struct_buf: Vec<u8> = vec![];
            for (k, v) in val
                .as_object()
                .ok_or_else(|| ErrorKind::BigQueryTypeMismatch("object", val.value_type()))?
            {
                let subfield_description = field.subfields.get(&k.to_string());

                if let Some(subfield_description) = subfield_description {
                    encode_field(v, subfield_description, &mut struct_buf)?;
                } else {
                    warn!(
                        "Passed field {} as struct field, not present in definition",
                        k
                    );
                }
            }
            prost::encoding::encode_key(tag, WireType::LengthDelimited, result);
            prost::encoding::encode_varint(struct_buf.len() as u64, result);
            result.append(&mut struct_buf);
        }
        TableType::Bytes => {
            prost::encoding::bytes::encode(
                tag,
                &Vec::from(
                    val.as_bytes().ok_or_else(|| {
                        ErrorKind::BigQueryTypeMismatch("bytes", val.value_type())
                    })?,
                ),
                result,
            );
        }
        TableType::Json => {
            warn!("Found a field of type JSON, this is not supported, ignoring.");
        }
        TableType::Interval => {
            warn!("Found a field of type Interval, this is not supported, ignoring.");
        }

        TableType::Unspecified => {
            warn!("Found a field of unspecified type - ignoring.");
        }
    }

    Ok(())
}

impl JsonToProtobufMapping {
    pub fn new(vec: &Vec<TableFieldSchema>) -> Self {
        let descriptor = map_field("table", vec);

        Self {
            descriptor: descriptor.0,
            fields: descriptor.1,
        }
    }

    pub fn map(&self, value: &Value) -> Result<Vec<u8>> {
        let mut result = vec![];
        if let Some(obj) = value.as_object() {
            for (key, val) in obj {
                if let Some(field) = self.fields.get(&key.to_string()) {
                    encode_field(val, field, &mut result)?;
                }
            }
        }

        Ok(result)
    }

    pub fn descriptor(&self) -> &DescriptorProto {
        &self.descriptor
    }
}
impl GbqSink {
    pub async fn new(config: Config) -> Result<Self> {
        Ok(Self {
            client: None,
            write_stream: None,
            mapping: None,
            config,
        })
    }
}

#[async_trait::async_trait]
impl Sink for GbqSink {
    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        ctx: &SinkContext,
        _serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        let client = self
            .client
            .as_mut()
            .ok_or(ErrorKind::BigQueryClientNotAvailable(
                "The client is not connected",
            ))?;
        let write_stream =
            self.write_stream
                .as_ref()
                .ok_or(ErrorKind::BigQueryClientNotAvailable(
                    "The write stream is not available",
                ))?;
        let mapping = self
            .mapping
            .as_ref()
            .ok_or(ErrorKind::BigQueryClientNotAvailable(
                "The mapping is not available",
            ))?;

        let mut serialized_rows = vec![];

        for data in event.value_iter() {
            serialized_rows.push(mapping.map(data)?);
        }

        let request = AppendRowsRequest {
            write_stream: write_stream.name.clone(),
            offset: None,
            trace_id: "".to_string(),
            rows: Some(append_rows_request::Rows::ProtoRows(ProtoData {
                writer_schema: Some(ProtoSchema {
                    proto_descriptor: Some(mapping.descriptor().clone()),
                }),
                rows: Some(ProtoRows { serialized_rows }),
            })),
        };

        let append_response = client
            .append_rows(stream::iter(vec![request]))
            .timeout(Duration::from_secs(self.config.request_timeout))
            .await;

        let append_response = if let Ok(append_response) = append_response {
            append_response
        } else {
            ctx.notifier.connection_lost().await?;

            return Ok(SinkReply::FAIL);
        };

        if let Ok(x) = append_response?
            .into_inner()
            .next()
            .timeout(Duration::from_secs(self.config.request_timeout))
            .await
        {
            match x {
                Some(Ok(_)) => Ok(SinkReply::ACK),
                Some(Err(e)) => {
                    error!("BigQuery error: {}", e);

                    Ok(SinkReply::FAIL)
                }
                None => Ok(SinkReply::NONE),
            }
        } else {
            ctx.notifier.connection_lost().await?;

            Ok(SinkReply::FAIL)
        }
    }

    async fn connect(&mut self, _ctx: &SinkContext, _attempt: &Attempt) -> Result<bool> {
        error!("Connecting to BigQuery");
        let token = Token::new()?.header_value()?;

        let token_metadata_value = MetadataValue::from_str(token.as_str())?;

        let tls_config = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(googapis::CERTIFICATES))
            .domain_name("bigquerystorage.googleapis.com");

        let channel = Channel::from_static("https://bigquerystorage.googleapis.com")
            .connect_timeout(Duration::from_secs(self.config.connect_timeout))
            .tls_config(tls_config)?
            .connect()
            .await?;

        let mut client = BigQueryWriteClient::with_interceptor(
            channel,
            AuthInterceptor {
                token: token_metadata_value,
            },
        );

        let write_stream = client
            .create_write_stream(CreateWriteStreamRequest {
                parent: self.config.table_id.clone(),
                write_stream: Some(WriteStream {
                    name: "".to_string(),
                    r#type: i32::from(write_stream::Type::Committed),
                    create_time: None,
                    commit_time: None,
                    table_schema: None,
                }),
            })
            .await?
            .into_inner();

        let mapping = JsonToProtobufMapping::new(
            &write_stream
                .table_schema
                .as_ref()
                .ok_or(ErrorKind::GbqSinkFailed("Table schema was not provided"))?
                .clone()
                .fields,
        );

        self.mapping = Some(mapping);
        self.write_stream = Some(write_stream);
        self.client = Some(client);

        Ok(true)
    }

    fn auto_ack(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use value_trait::StaticNode;

    #[test]
    fn interceptor_will_add_the_authorization_header() {
        let metadata_value = MetadataValue::from_str("test").unwrap();
        let mut interceptor = AuthInterceptor {
            token: metadata_value,
        };

        let request = Request::new(());
        let request = interceptor.call(request).unwrap();

        assert_eq!(
            MetadataValue::from_str("test").unwrap(),
            request.metadata().get("authorization").unwrap()
        )
    }

    #[test]
    fn encode_fails_on_type_mismatch() {
        let data = [
            (
                Value::String("asdf".into()),
                Field {
                    table_type: TableType::Int64,
                    tag: 1,
                    subfields: Default::default(),
                },
            ),
            (
                Value::Static(StaticNode::F64(1.243)),
                Field {
                    table_type: TableType::String,
                    tag: 2,
                    subfields: Default::default(),
                },
            ),
        ];

        for (value, field) in data {
            let mut result_data = vec![];

            let result = encode_field(&value, &field, &mut result_data);

            assert!(result.is_err());
        }
    }

    #[test]
    pub fn test_can_encode_stringy_types() {
        // NOTE: This test always passes the string "I" as the value to encode, this is not correct for some of the types (e.g. datetime),
        // but we still allow it, leaving the validation to BigQuery
        let data = [
            TableType::String,
            TableType::Date,
            TableType::Time,
            TableType::Datetime,
            TableType::Geography,
            TableType::Numeric,
            TableType::Bignumeric,
            TableType::Timestamp,
        ];

        for item in data {
            let mut result = vec![];
            assert!(
                encode_field(
                    &Value::String("I".into()),
                    &Field {
                        table_type: item,
                        tag: 123,
                        subfields: Default::default()
                    },
                    &mut result
                )
                .is_ok(),
                "TableType: {:?} did not encode correctly",
                item
            );

            assert_eq!([218u8, 7u8, 1u8, 73u8], result[..]);
        }
    }

    #[test]
    pub fn test_can_encode_a_struct() {
        let mut values = halfbrown::HashMap::new();
        values.insert("a".into(), Value::Static(StaticNode::I64(1)));
        values.insert("b".into(), Value::Static(StaticNode::I64(1024)));
        let input = Value::Object(Box::new(values));

        let mut subfields = HashMap::new();
        subfields.insert(
            "a".into(),
            Field {
                table_type: TableType::Int64,
                tag: 1,
                subfields: Default::default(),
            },
        );
        subfields.insert(
            "b".into(),
            Field {
                table_type: TableType::Int64,
                tag: 2,
                subfields: Default::default(),
            },
        );

        let field = Field {
            table_type: TableType::Struct,
            tag: 1024,
            subfields,
        };

        let mut result = Vec::new();
        assert!(encode_field(&input, &field, &mut result).is_ok());

        assert_eq!([130u8, 64u8, 5u8, 8u8, 1u8, 16u8, 128u8, 8u8], result[..])
    }
}