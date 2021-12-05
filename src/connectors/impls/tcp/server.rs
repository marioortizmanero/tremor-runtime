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
use super::{TcpReader, TcpWriter};
use crate::connectors::prelude::*;
use crate::connectors::utils::tls::{load_server_config, TLSServerConfig};
use crate::errors::{Error, ErrorKind};
use crate::ttry;
use async_std::net::TcpListener;
use async_std::task::{self, JoinHandle};
use async_tls::TlsAcceptor;
use futures::io::AsyncReadExt;
use rustls::ServerConfig;
use simd_json::ValueAccess;
use std::future;
use std::net::SocketAddr;
use std::sync::Arc;

use abi_stable::{
    export_root_module,
    prefix_type::PrefixTypeTrait,
    rstr, rvec, sabi_extern_fn,
    std_types::{
        ROption::{self, RSome},
        RResult::{RErr, ROk},
        RStr, RString,
    },
    type_level::downcasting::TD_Opaque,
};
use async_ffi::{BorrowingFfiFuture, FfiFuture, FutureExt};

const URL_SCHEME: &str = "tremor-tcp-server";

#[export_root_module]
pub fn instantiate_root_module() -> ConnectorMod_Ref {
    ConnectorMod {
        connector_type,
        from_config,
    }
    .leak_into_prefix()
}

#[sabi_extern_fn]
fn connector_type() -> ConnectorType {
    "tcp_server".into()
}

#[sabi_extern_fn]
pub fn from_config(
    id: TremorUrl,
    raw_config: ROption<RString>,
) -> FfiFuture<RResult<BoxedRawConnector>> {
    async move {
        if let RSome(raw_config) = raw_config {
            let config = ttry!(Config::from_str(&raw_config));

            let tls_server_config = if let Some(tls_config) = config.tls.as_ref() {
                Some(ttry!(load_server_config(tls_config)))
            } else {
                None
            };
            let server = TcpServer {
                url: id.clone(),
                config,
                accept_task: None,  // not yet started
                sink_runtime: None, // replaced in create_sink()
                source_runtime: None,
                tls_server_config,
            };
            ROk(BoxedRawConnector::from_value(server, TD_Opaque))
        } else {
            RErr(ErrorKind::MissingConfiguration(String::from("TcpServer")).into())
        }
    }
    .into_ffi()
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Config {
    // kept as a str, so it is re-resolved upon each connect
    host: String,
    port: u16,
    // TCP: receive buffer size
    tls: Option<TLSServerConfig>,
    #[serde(default = "default_buf_size")]
    buf_size: usize,
}

impl ConfigImpl for Config {}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct ConnectionMeta {
    host: String,
    port: u16,
}

impl From<SocketAddr> for ConnectionMeta {
    fn from(sa: SocketAddr) -> Self {
        Self {
            host: sa.ip().to_string(),
            port: sa.port(),
        }
    }
}

#[allow(clippy::module_name_repetitions)]
pub struct TcpServer {
    url: TremorUrl,
    config: Config,
    accept_task: Option<JoinHandle<Result<()>>>,
    sink_runtime: Option<ChannelSinkRuntime<ConnectionMeta>>,
    source_runtime: Option<ChannelSourceRuntime>,
    tls_server_config: Option<ServerConfig>,
}

fn resolve_connection_meta(meta: &Value) -> Option<ConnectionMeta> {
    let peer = meta.get("peer");
    peer.get_u16("port")
        .zip(peer.get_str("host"))
        .map(|(port, host)| -> ConnectionMeta {
            ConnectionMeta {
                host: host.to_string(),
                port,
            }
        })
}

impl RawConnector for TcpServer {
    fn on_stop(&mut self, _ctx: &ConnectorContext) -> BorrowingFfiFuture<'_, RResult<()>> {
        async move {
            if let Some(accept_task) = self.accept_task.take() {
                // stop acceptin' new connections
                accept_task.cancel().await;
            }
            ROk(())
        }
        .into_ffi()
    }

    fn create_source(
        &mut self,
        ctx: SourceContext,
        qsize: usize,
    ) -> BorrowingFfiFuture<'_, RResult<ROption<BoxedRawSource>>> {
        let source = ChannelSource::new(ctx.clone(), qsize);
        self.source_runtime = Some(source.runtime());
        // We don't need to be able to downcast the connector back to the original
        // type, so we just pass it as an opaque type.
        let source = BoxedRawSource::from_value(source, TD_Opaque);
        future::ready(ROk(RSome(source))).into_ffi()
    }

    fn create_sink(
        &mut self,
        _ctx: SinkContext,
        qsize: usize,
        reply_tx: BoxedContraflowSender,
    ) -> BorrowingFfiFuture<'_, RResult<ROption<BoxedRawSink>>> {
        let sink = ChannelSink::new_no_meta(qsize, resolve_connection_meta, reply_tx);
        self.sink_runtime = Some(sink.runtime());
        // We don't need to be able to downcast the connector back to the original
        // type, so we just pass it as an opaque type.
        let sink = BoxedRawSink::from_value(sink, TD_Opaque);
        future::ready(ROk(RSome(sink))).into_ffi()
    }

    #[allow(clippy::too_many_lines)]
    fn connect<'a>(
        &'a mut self,
        ctx: &'a ConnectorContext,
        _attempt: &'a Attempt,
    ) -> BorrowingFfiFuture<'a, RResult<bool>> {
        async move {
            let path = rvec![RString::from(self.config.port.to_string())];
            let accept_url = self.url.clone();

            let source_runtime = ttry!(self
                .source_runtime
                .clone()
                .ok_or(Error::from("Source runtime not initialized")));
            let sink_runtime = ttry!(self
                .sink_runtime
                .clone()
                .ok_or(Error::from("sink runtime not initialized")));
            let buf_size = self.config.buf_size;

            // cancel last accept task if necessary, this will drop the previous listener
            if let Some(previous_handle) = self.accept_task.take() {
                previous_handle.cancel().await;
            }

            let listener = ttry!(
                TcpListener::bind((self.config.host.as_str(), self.config.port))
                    .await
                    .map_err(Error::from)
            );

            let ctx = ctx.clone();
            let tls_server_config = self.tls_server_config.clone();

            // accept task
            self.accept_task = Some(task::spawn(async move {
                let mut stream_id_gen = StreamIdGen::default();
                while let (true, Ok((stream, peer_addr))) = (
                    ctx.quiescence_beacon.continue_reading().await,
                    listener.accept().await,
                ) {
                    debug!(
                        "[Connector::{}] new connection from {}",
                        &accept_url, peer_addr
                    );
                    let stream_id: u64 = stream_id_gen.next_stream_id();
                    let connection_meta: ConnectionMeta = peer_addr.into();
                    // Async<T> allows us to read in one thread and write in another concurrently - see its documentation
                    // So we don't need no BiLock like we would when using `.split()`
                    let origin_uri = EventOriginUri {
                        scheme: RString::from(URL_SCHEME),
                        host: RString::from(peer_addr.ip().to_string()),
                        port: RSome(peer_addr.port()),
                        path: path.clone(), // captures server port
                    };

                    let tls_acceptor: Option<TlsAcceptor> = tls_server_config
                        .clone()
                        .map(|sc| TlsAcceptor::from(Arc::new(sc)));
                    if let Some(acceptor) = tls_acceptor {
                        let tls_stream = acceptor.accept(stream.clone()).await?; // TODO: this should live in its own task, as it requires rome roundtrips :()
                        let (tls_read_stream, tls_write_sink) = tls_stream.split();
                        let meta = ctx.meta(literal!({
                            "tls": true,
                            "peer": {
                                "host": peer_addr.ip().to_string(),
                                "port": peer_addr.port()
                            }
                        }));
                        let tls_reader = TcpReader::tls_server(
                            tls_read_stream,
                            stream.clone(),
                            vec![0; buf_size],
                            ctx.url.clone(),
                            origin_uri.clone(),
                            meta,
                        );
                        source_runtime.register_stream_reader(stream_id, &ctx, tls_reader);

                        sink_runtime.register_stream_writer(
                            stream_id,
                            Some(connection_meta.clone()),
                            &ctx,
                            TcpWriter::tls_server(tls_write_sink, stream),
                        );
                    } else {
                        let meta = ctx.meta(literal!({
                            "tls": false,
                            "peer": {
                                "host": peer_addr.ip().to_string(),
                                "port": peer_addr.port()
                            }
                        }));
                        let tcp_reader = TcpReader::new(
                            stream.clone(),
                            vec![0; buf_size],
                            ctx.url.clone(),
                            origin_uri.clone(),
                            meta,
                        );
                        source_runtime.register_stream_reader(stream_id, &ctx, tcp_reader);

                        sink_runtime.register_stream_writer(
                            stream_id,
                            Some(connection_meta.clone()),
                            &ctx,
                            TcpWriter::new(stream),
                        );
                    }
                }

                // notify connector task about disconnect
                // of the listening socket
                Result::from(ctx.notifier.notify().await.map_err(Error::from))?;
                Ok(())
            }));

            ROk(true)
        }
        .into_ffi()
    }

    fn default_codec(&self) -> RStr<'_> {
        rstr!("json")
    }
}
