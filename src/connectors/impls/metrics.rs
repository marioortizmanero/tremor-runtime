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

use crate::connectors::prelude::*;
use crate::connectors::utils::metrics::METRICS_CHANNEL;
use async_broadcast::{broadcast, Receiver, Sender, TryRecvError, TrySendError};
use beef::Cow;
use tremor_script::utils::hostname;

use crate::pdk::RError;
use crate::ttry;
use abi_stable::{
    prefix_type::PrefixTypeTrait,
    rstr, rvec, sabi_extern_fn,
    std_types::{
        ROption::{self, RNone, RSome},
        RResult::{RErr, ROk},
        RStr, RString,
    },
    type_level::downcasting::TD_Opaque,
};
use async_ffi::{BorrowingFfiFuture, FfiFuture, FutureExt};
use std::future;
use tremor_pipeline::pdk::PdkEvent;
use tremor_value::pdk::PdkValue;

const MEASUREMENT: Cow<'static, str> = Cow::const_str("measurement");
const TAGS: Cow<'static, str> = Cow::const_str("tags");
const FIELDS: Cow<'static, str> = Cow::const_str("fields");
const TIMESTAMP: Cow<'static, str> = Cow::const_str("timestamp");

/// Note that since it's a built-in plugin, `#[export_root_module]` can't be
/// used or it would conflict with other plugins.
pub fn instantiate_root_module() -> ConnectorMod_Ref {
    ConnectorMod {
        connector_type,
        from_config,
    }
    .leak_into_prefix()
}

#[sabi_extern_fn]
fn connector_type() -> ConnectorType {
    "metrics".into()
}
#[sabi_extern_fn]
pub fn from_config(
    _id: TremorUrl,
    _raw_config: ROption<PdkValue<'static>>,
) -> FfiFuture<RResult<BoxedRawConnector>> {
    let connector = BoxedRawConnector::from_value(MetricsConnector::new(), TD_Opaque);
    future::ready(ROk(connector)).into_ffi()
}

#[derive(Clone, Debug)]
pub(crate) struct MetricsChannel {
    tx: Sender<Msg>,
    rx: Receiver<Msg>,
}

impl MetricsChannel {
    pub(crate) fn new(qsize: usize) -> Self {
        let (mut tx, rx) = broadcast(qsize);
        // We user overflow so that non collected messages can be removed
        // Ffor Metrics it should be good enough we consume them quickly
        // and if not we got bigger problems
        tx.set_overflow(true);
        Self { tx, rx }
    }

    pub(crate) fn tx(&self) -> Sender<Msg> {
        self.tx.clone()
    }
    pub(crate) fn rx(&self) -> Receiver<Msg> {
        self.rx.clone()
    }
}
#[derive(Debug, Clone)]
pub struct Msg {
    payload: EventPayload,
    origin_uri: Option<EventOriginUri>,
}

impl Msg {
    /// creates a new message
    pub fn new(payload: EventPayload, origin_uri: Option<EventOriginUri>) -> Self {
        Self {
            payload,
            origin_uri,
        }
    }
}

/// This is a system connector to collect and forward metrics.
/// System metrics are fed to this connector and can be received by binding this connector's `out` port to a pipeline to handle metrics events.
/// It can also be used to send custom metrics and have them handled the same way as system metrics.
/// Custom metrics need to be sent as events to the `in` port of this connector.
///
/// TODO: describe metrics event format and write stdlib function to help with that
///
/// There should be only one instance around all the time, identified by `tremor://localhost/connector/system::metrics/system`
///
pub(crate) struct MetricsConnector {
    tx: Sender<Msg>,
    rx: Receiver<Msg>,
}

impl MetricsConnector {
    pub(crate) fn new() -> Self {
        Self {
            tx: METRICS_CHANNEL.tx(),
            rx: METRICS_CHANNEL.rx(),
        }
    }
}

impl RawConnector for MetricsConnector {
    fn is_structured(&self) -> bool {
        true
    }

    fn connect<'a>(
        &'a mut self,
        _ctx: &'a ConnectorContext,
        _attempt: &'a Attempt,
    ) -> BorrowingFfiFuture<'a, RResult<bool>> {
        future::ready(ROk(!self.tx.is_closed())).into_ffi()
    }

    fn create_source(
        &mut self,
        _ctx: SourceContext,
        _qsize: usize,
    ) -> BorrowingFfiFuture<'_, RResult<ROption<BoxedRawSource>>> {
        let source = MetricsSource::new(self.rx.clone());
        // We don't need to be able to downcast the connector back to the original
        // type, so we just pass it as an opaque type.
        let source = BoxedRawSource::from_value(source, TD_Opaque);
        future::ready(ROk(RSome(source))).into_ffi()
    }

    fn create_sink(
        &mut self,
        _ctx: SinkContext,
        _qsize: usize,
        _reply_tx: BoxedContraflowSender,
    ) -> BorrowingFfiFuture<'_, RResult<ROption<BoxedRawSink>>> {
        let sink = MetricsSink::new(self.tx.clone());
        // We don't need to be able to downcast the connector back to the original
        // type, so we just pass it as an opaque type.
        let sink = BoxedRawSink::from_value(sink, TD_Opaque);
        future::ready(ROk(RSome(sink))).into_ffi()
    }

    fn default_codec(&self) -> RStr<'_> {
        rstr!("json")
    }
}

pub(crate) struct MetricsSource {
    rx: Receiver<Msg>,
    origin_uri: EventOriginUri,
}

impl MetricsSource {
    pub(crate) fn new(rx: Receiver<Msg>) -> Self {
        Self {
            rx,
            origin_uri: EventOriginUri {
                scheme: RString::from("tremor-metrics"),
                host: RString::from(hostname()),
                port: RNone,
                path: rvec![],
            },
        }
    }
}

impl RawSource for MetricsSource {
    fn pull_data<'a>(
        &'a mut self,
        _pull_id: u64,
        _ctx: &'a SourceContext,
    ) -> BorrowingFfiFuture<'a, RResult<SourceReply>> {
        let reply = match self.rx.try_recv() {
            Ok(msg) => ROk(SourceReply::Structured {
                payload: msg.payload.into(),
                origin_uri: msg.origin_uri.unwrap_or_else(|| self.origin_uri.clone()),
                stream: DEFAULT_STREAM_ID,
                port: RNone,
            }),
            Err(TryRecvError::Closed) => RErr(RError::new(Error::from(TryRecvError::Closed))),
            Err(TryRecvError::Empty) => ROk(SourceReply::Empty(10)),
        };

        future::ready(reply).into_ffi()
    }

    fn is_transactional(&self) -> bool {
        false
    }
}

pub(crate) struct MetricsSink {
    tx: Sender<Msg>,
}

impl MetricsSink {
    pub(crate) fn new(tx: Sender<Msg>) -> Self {
        Self { tx }
    }
}

/// verify a value for conformance with the required metrics event format
pub(crate) fn verify_metrics_value(value: &Value<'_>) -> Result<()> {
    value
        .as_object()
        .and_then(|obj| {
            // check presence of fields
            obj.get(&MEASUREMENT)
                .zip(obj.get(&TAGS))
                .zip(obj.get(&FIELDS))
                .zip(obj.get(&TIMESTAMP))
        })
        .and_then(|(((measurement, tags), fields), timestamp)| {
            // check correct types
            if measurement.is_str()
                && tags.is_object()
                && fields.is_object()
                && timestamp.is_integer()
            {
                Some(())
            } else {
                None
            }
        })
        .ok_or_else(|| ErrorKind::InvalidMetricsData.into())
}

/// passing events through to the source channel
impl RawSink for MetricsSink {
    fn auto_ack(&self) -> bool {
        true
    }

    /// entrypoint for custom metrics events
    fn on_event<'a>(
        &'a mut self,
        _input: RStr<'a>,
        event: PdkEvent,
        _ctx: &'a SinkContext,
        _serializer: &'a mut MutEventSerializer,
        _start: u64,
    ) -> BorrowingFfiFuture<'a, RResult<SinkReply>> {
        // Conversion to use the full functionality of `Event`
        let event = Event::from(event);

        async move {
            // verify event format
            for (value, _meta) in event.value_meta_iter() {
                // if it fails here an error event is sent to the ERR port of this connector
                ttry!(verify_metrics_value(value));
            }

            let Event {
                origin_uri, data, ..
            } = event;

            let metrics_msg = Msg::new(data, origin_uri);
            let ack_or_fail = match self.tx.try_broadcast(metrics_msg) {
                Err(TrySendError::Closed(_)) => {
                    // channel is closed
                    SinkReply {
                        ack: SinkAck::Fail,
                        cb: CbAction::Close,
                    }
                }
                Err(TrySendError::Full(_)) => SinkReply::FAIL,
                _ => SinkReply::ACK,
            };

            ROk(ack_or_fail)
        }
        .into_ffi()
    }
}
