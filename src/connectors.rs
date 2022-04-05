// Copyright 2020-2021, The Tremor Team
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

pub(crate) mod impls;
/// prelude with commonly needed stuff imported
pub(crate) mod prelude;

/// sink parts
pub(crate) mod sink;

/// source parts
pub(crate) mod source;

#[macro_use]
pub(crate) mod utils;

#[cfg(test)]
mod tests;

use self::metrics::{SinkReporter, SourceReporter};
use self::sink::{SinkAddr, SinkContext, SinkMsg};
use self::source::{SourceAddr, SourceContext, SourceMsg};
use self::utils::quiescence::QuiescenceBeacon;
pub(crate) use crate::config::Connector as ConnectorConfig;
use crate::instance::State;
use crate::pipeline;
use crate::system::World;
use crate::{
    errors::{Error, Kind as ErrorKind, Result},
    log_error,
};
use async_std::task::{self};
use async_std::{
    channel::{bounded, Sender},
    task::JoinHandle,
};
use beef::Cow;
use futures::Future;
use halfbrown::HashMap;
use std::{fmt::Display, sync::atomic::Ordering, time::Duration};
use tremor_common::ids::ConnectorIdGen;
use tremor_common::ports::{ERR, IN, OUT};
use tremor_pipeline::METRICS_CHANNEL;
use tremor_script::ast::DeployEndpoint;
use tremor_value::Value;
use utils::reconnect::{Attempt, ConnectionLostNotifier, ReconnectRuntime};
use value_trait::{Builder, Mutable, ValueAccess};

/// quiescence stuff
pub(crate) use utils::{metrics, reconnect};

/// Accept timeout
pub(crate) const ACCEPT_TIMEOUT: Duration = Duration::from_millis(100);

use crate::{
    connectors::prelude::*,
    pdk::{
        self,
        utils::{conv_cow_str, conv_cow_str_inv},
        RResult, DEFAULT_PLUGIN_PATH,
    },
};
use abi_stable::{
    std_types::{
        RBox, RCowStr,
        ROption::{self, RNone, RSome},
        RResult::{RErr, ROk},
        RStr, RString, RVec,
    },
    type_level::downcasting::{TD_CanDowncast, TD_Opaque},
    StableAbi,
};
use async_ffi::{BorrowingFfiFuture, FutureExt};
use std::{env, future};

// FIXME: make prettier or avoid duplication in pdk mod? It's a bit out of place
// for now.
fn conv_cow_str(cow: RCow<str>) -> beef::Cow<str> {
    let cow: std::borrow::Cow<str> = cow.into();
    cow.into()
}
// fn conv_value(value: serde_yaml::Value) -> PdkValue<'static> {
//     let value: Value = value.into();
//     value.into()
// }

/// connector address
#[derive(Clone, Debug)]
pub struct Addr {
    /// connector instance url
    pub(crate) alias: String,
    sender: Sender<Msg>,
    source: Option<SourceAddr>,
    pub(crate) sink: Option<SinkAddr>,
}

impl Display for Addr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.alias.fmt(f)
    }
}

impl Addr {
    /// send a message
    ///
    /// # Errors
    ///  * If sending failed
    pub(crate) async fn send(&self, msg: Msg) -> Result<()> {
        Ok(self.sender.send(msg).await?)
    }

    /// send a message to the sink part of the connector.
    /// Results in a no-op if the connector has no sink part.
    ///
    /// # Errors
    ///   * if sending failed
    pub(crate) async fn send_sink(&self, msg: SinkMsg) -> Result<()> {
        if let Some(sink) = self.sink.as_ref() {
            sink.addr.send(msg).await?;
        }
        Ok(())
    }

    /// Send a message to the source part of the connector.
    /// Results in a no-op if the connector has no source part.
    ///
    /// # Errors
    ///   * if sending failed
    pub(crate) async fn send_source(&self, msg: SourceMsg) -> Result<()> {
        if let Some(source) = self.source.as_ref() {
            source.addr.send(msg).await?;
        }
        Ok(())
    }

    fn has_source(&self) -> bool {
        self.source.is_some()
    }

    fn has_sink(&self) -> bool {
        self.sink.is_some()
    }

    /// stops the connector
    ///
    /// # Errors
    ///   * if sending failed
    pub(crate) async fn stop(&self, sender: Sender<ConnectorResult<()>>) -> Result<()> {
        self.send(Msg::Stop(sender)).await
    }
    /// starts the connector
    ///
    /// # Errors
    ///   * if sending failed
    pub(crate) async fn start(&self, sender: Sender<ConnectorResult<()>>) -> Result<()> {
        self.send(Msg::Start(sender)).await
    }
    /// drains the connector
    ///
    /// # Errors
    ///   * if sending failed
    pub(crate) async fn drain(&self, sender: Sender<ConnectorResult<()>>) -> Result<()> {
        self.send(Msg::Drain(sender)).await
    }
    /// pauses the connector
    ///
    /// # Errors
    ///   * if sending failed
    pub async fn pause(&self) -> Result<()> {
        self.send(Msg::Pause).await
    }
    /// resumes the connector
    ///
    /// # Errors
    ///   * if sending failed
    pub async fn resume(&self) -> Result<()> {
        self.send(Msg::Resume).await
    }

    /// report status of the connector instance
    ///
    /// # Errors
    ///   * if sending or receiving failed
    pub async fn report_status(&self) -> Result<StatusReport> {
        let (tx, rx) = bounded(1);
        self.send(Msg::Report(tx)).await?;
        Ok(rx.recv().await?)
    }
}

/// Messages a Connector instance receives and acts upon
pub(crate) enum Msg {
    /// connect 1 or more pipelines to a port
    LinkInput {
        /// port to which to connect
        port: Cow<'static, str>,
        /// pipelines to connect
        pipelines: Vec<(DeployEndpoint, pipeline::Addr)>,
        /// result receiver
        result_tx: Sender<Result<()>>,
    },
    /// connect 1 or more pipelines to a port
    LinkOutput {
        /// port to which to connect
        port: Cow<'static, str>,
        /// pipelines to connect
        pipelines: Vec<(DeployEndpoint, pipeline::Addr)>,
        /// result receiver
        result_tx: Sender<Result<()>>,
    },

    /// notification from the connector implementation that connectivity is lost and should be reestablished
    ConnectionLost,
    /// initiate a reconnect attempt
    Reconnect,
    // TODO: fill as needed
    /// start the connector
    Start(Sender<ConnectorResult<()>>),
    /// pause the connector
    ///
    /// source part is not polling for new data
    /// sink part issues a CB trigger
    /// until Resume is called, sink is restoring the CB again
    Pause,
    /// resume the connector after a pause
    Resume,
    /// Drain events from this connector
    ///
    /// - stop reading events from external connections
    /// - decline events received via the sink part
    /// - wait for drainage to be finished
    Drain(Sender<ConnectorResult<()>>),
    /// notify this connector that its source part has been drained
    SourceDrained,
    /// notify this connector that its sink part has been drained
    SinkDrained,
    /// stop the connector
    Stop(Sender<ConnectorResult<()>>),
    /// request a status report
    Report(Sender<StatusReport>),
}

#[derive(Debug)]
/// result of an async operation of the connector.
/// bears a `url` to identify the connector who finished the operation
pub(crate) struct ConnectorResult<T: std::fmt::Debug> {
    /// the connector url
    pub(crate) alias: String,
    /// the actual result
    pub(crate) res: Result<T>,
}

impl ConnectorResult<()> {
    fn ok(ctx: &ConnectorContext) -> Self {
        Self {
            alias: ctx.alias.clone().into(),
            res: Ok(()),
        }
    }

    fn err(ctx: &ConnectorContext, err_msg: &'static str) -> Self {
        Self {
            alias: ctx.alias.clone().into(),
            res: Err(Error::from(err_msg)),
        }
    }
}

/// context for a Connector or its parts
pub(crate) trait Context: Display + Clone {
    /// provide the url of the connector
    fn alias(&self) -> &str;

    /// get the quiescence beacon for checking if we should continue reading/writing
    fn quiescence_beacon(&self) -> &BoxedQuiescenceBeacon;

    /// get the notifier to signal to the runtime that we are disconnected
    fn notifier(&self) -> &BoxedConnectionLostNotifier;

    /// get the connector type
    fn connector_type(&self) -> &ConnectorType;

    /// only log an error and swallow the result
    #[inline]
    fn swallow_err<T, E, M>(&self, expr: std::result::Result<T, E>, msg: &M)
    where
        E: std::error::Error,
        M: Display + ?Sized,
    {
        if let Err(e) = expr {
            error!("{self} {msg}: {e}");
        }
    }

    /// log an error and return the result
    #[inline]
    fn bail_err<T, E, M>(
        &self,
        expr: std::result::Result<T, E>,
        msg: &M,
    ) -> std::result::Result<T, E>
    where
        E: std::error::Error,
        M: Display + ?Sized,
    {
        if let Err(e) = &expr {
            error!("{self} {msg}: {e}");
        }
        expr
    }

    /// enclose the given meta in the right connector namespace
    ///
    /// Namespace: "connector.<connector-type>"
    #[must_use]
    fn meta(&self, inner: Value<'static>) -> Value<'static> {
        let mut map = Value::object_with_capacity(1);
        map.try_insert(self.connector_type().to_string(), inner);
        map
    }

    /// extract the connector specific metadata
    fn extract_meta<'ct, 'event>(
        &'ct self,
        event_meta: &'ct Value<'event>,
    ) -> Option<&'ct Value<'event>>
    where
        'ct: 'event,
    {
        let t: &str = self.connector_type().into();
        event_meta.get(&Cow::borrowed(t))
    }
}

/// connector context
#[derive(Clone)]
pub(crate) struct ConnectorContext {
    /// url of the connector
    pub alias: RString,
    /// type of the connector
    connector_type: ConnectorType,
    /// The Quiescence Beacon
    quiescence_beacon: BoxedQuiescenceBeacon,
    /// Notifier
    notifier: reconnect::BoxedConnectionLostNotifier,
}

impl Display for ConnectorContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[Connector::{}]", &self.alias)
    }
}

impl Context for ConnectorContext {
    fn alias(&self) -> &str {
        &self.alias
    }

    fn connector_type(&self) -> &ConnectorType {
        &self.connector_type
    }

    fn quiescence_beacon(&self) -> &BoxedQuiescenceBeacon {
        &self.quiescence_beacon
    }

    fn notifier(&self) -> &BoxedConnectionLostNotifier {
        &self.notifier
    }
}

/// Connector instance status report
#[derive(Debug, Serialize)]
pub struct StatusReport {
    /// connector instance url
    pub(crate) alias: String,
    /// state of the connector
    pub status: State,
    /// current connectivity
    pub(crate) connectivity: Connectivity,
    /// connected pipelines
    pub(crate) pipelines: HashMap<Cow<'static, str>, Vec<DeployEndpoint>>,
}

/// Stream id generator
#[derive(Debug, Default)]
pub(crate) struct StreamIdGen(u64);

impl StreamIdGen {
    /// get the next stream id and increment the internal state
    pub(crate) fn next_stream_id(&mut self) -> u64 {
        let res = self.0;
        self.0 = self.0.wrapping_add(1);
        res
    }
}

/// How should we treat a stream being done
///
/// * `StreamClosed` -> Only this stream is closed
/// * `ConnectorClosed` -> The entire connector is closed, notify that we are disconnected, reconnect according to chosen reconnect config
#[derive(Debug, Clone, PartialEq, Copy)]
pub(crate) enum StreamDone {
    /// Only this stream is closed, (only one of many)
    StreamClosed,
    /// With this stream being closed, the whole connector can be considered done/closed
    ConnectorClosed,
}

/// Lookup table for known connectors
pub(crate) type Known =
    std::collections::HashMap<ConnectorType, Box<dyn ConnectorBuilder + 'static>>;

/// Spawns a connector
///
/// # Errors
/// if the connector can not be built or the config is invalid
pub(crate) async fn spawn(
    alias: &str,
    connector_id_gen: &mut ConnectorIdGen,
    known_connectors: &Known,
    config: ConnectorConfig,
) -> Result<Addr> {
    // lookup and instantiate connector
    let builder = known_connectors
        .get(&config.connector_type)
        .ok_or_else(|| ErrorKind::UnknownConnectorType(config.connector_type.to_string()))?;
    let connector = builder.from_config()(alias.as_str().into(), &config).await;
    let connector = Result::from(connector.map_err(Error::from))?;
    let connector = Connector(connector);

    Ok(connector_task(
        alias.to_string(),
        connector,
        config,
        connector_id_gen.next_id(),
    )
    .await?)
}

#[allow(clippy::too_many_lines)]
// instantiates the connector and starts listening for control plane messages
async fn connector_task(
    alias: String,
    mut connector: Connector,
    config: ConnectorConfig,
    uid: u64,
) -> Result<Addr> {
    let qsize = crate::QSIZE.load(Ordering::Relaxed);
    // channel for connector-level control plane communication
    let (msg_tx, msg_rx) = bounded(qsize);

    let mut connectivity = Connectivity::Disconnected;
    let mut quiescence_beacon = QuiescenceBeacon::default();
    let notifier = ConnectionLostNotifier::new(msg_tx.clone());
    let notifier = BoxedConnectionLostNotifier::from_value(notifier, TD_Opaque);

    let source_metrics_reporter = SourceReporter::new(
        alias.clone(),
        METRICS_CHANNEL.tx(),
        config.metrics_interval_s.into(),
    );

    let codec_requirement = connector.codec_requirements();
    if connector.codec_requirements() == CodecReq::Structured && (config.codec.is_some()) {
        return Err(format!(
            "[Connector::{}] is a structured connector and can't be configured with a codec",
            alias
        )
        .into());
    }
    let source_builder = source::builder(
        uid,
        &config,
        codec_requirement,
        qsize,
        source_metrics_reporter,
    )?;
    let source_ctx = SourceContext {
        alias: alias.clone(),
        uid,
        connector_type: config.connector_type.clone(),
        quiescence_beacon: quiescence_beacon.clone(),
        notifier: notifier.clone(),
    };

    let sink_metrics_reporter = SinkReporter::new(
        alias.clone(),
        METRICS_CHANNEL.tx(),
        config.metrics_interval_s.into(),
    );
    let sink_builder = sink::builder(
        &config,
        codec_requirement,
        &alias,
        qsize,
        sink_metrics_reporter,
    )?;
    let sink_ctx = SinkContext {
        uid,
        alias: alias.clone(),
        connector_type: config.connector_type.clone(),
        quiescence_beacon: quiescence_beacon.clone(),
        notifier: notifier.clone(),
    };
    // create source instance
    let source_addr = connector.create_source(source_ctx, source_builder).await?;

    // create sink instance
    let sink_addr = connector.create_sink(sink_ctx, sink_builder).await?;

    let connector_addr = Addr {
        alias: alias.clone(),
        sender: msg_tx,
        source: source_addr,
        sink: sink_addr,
    };

    let mut reconnect: ReconnectRuntime =
        ReconnectRuntime::new(&connector_addr, notifier.clone(), &config.reconnect);
    let notifier = reconnect.notifier();

    let ctx = ConnectorContext {
        alias: alias.clone(),
        connector_type: config.connector_type.clone(),
        quiescence_beacon: quiescence_beacon.clone(),
        notifier,
    };

    let send_addr = connector_addr.clone();
    let mut connector_state = State::Initializing;
    let mut drainage = None;
    let mut start_sender: Option<Sender<ConnectorResult<()>>> = None;

    // TODO: add connector metrics reporter (e.g. for reconnect attempts, cb's received, uptime, etc.)
    task::spawn::<_, Result<()>>(async move {
        // typical 1 pipeline connected to IN, OUT, ERR
        let mut connected_pipelines: HashMap<
            Cow<'static, str>,
            Vec<(DeployEndpoint, pipeline::Addr)>,
        > = HashMap::with_capacity(3);
        // connector control plane loop
        while let Ok(msg) = msg_rx.recv().await {
            match msg {
                Msg::Report(tx) => {
                    // request a status report from this connector
                    let pipes: HashMap<Cow<'static, str>, Vec<DeployEndpoint>> =
                        connected_pipelines
                            .iter()
                            .map(|(port, connected)| {
                                (
                                    port.clone(),
                                    connected
                                        .iter()
                                        .map(|(endpoint, _)| endpoint)
                                        .cloned()
                                        .collect::<Vec<_>>(),
                                )
                            })
                            .collect();
                    if let Err(e) = tx
                        .send(StatusReport {
                            alias: alias.clone(),
                            status: connector_state,
                            connectivity,
                            pipelines: pipes,
                        })
                        .await
                    {
                        error!("{ctx} Error sending status report {e}.");
                    }
                }
                Msg::LinkInput {
                    port,
                    pipelines: pipelines_to_link,
                    result_tx,
                } => {
                    for (url, _) in &pipelines_to_link {
                        info!("{ctx} Connecting {url} via port {port}");
                    }

                    if let Some(port_pipes) = connected_pipelines.get_mut(&port) {
                        port_pipes.extend(pipelines_to_link.iter().cloned());
                    } else {
                        connected_pipelines.insert(port.clone(), pipelines_to_link.clone());
                    }
                    let res = if connector.is_valid_input_port(&port) {
                        // connect to sink part
                        if let Some(sink) = connector_addr.sink.as_ref() {
                            sink.addr
                                .send(SinkMsg::Link {
                                    port,
                                    pipelines: pipelines_to_link,
                                })
                                .await
                                .map_err(Into::into)
                        } else {
                            Err(ErrorKind::InvalidConnect(
                                connector_addr.alias.to_string(),
                                port.clone(),
                            )
                            .into())
                        }
                    } else {
                        error!("{ctx} Tried to connect to unsupported port: \"{port}\"");
                        Err(ErrorKind::InvalidConnect(
                            connector_addr.alias.to_string(),
                            port.clone(),
                        )
                        .into())
                    };
                    // send back the connect result
                    if let Err(e) = result_tx.send(res).await {
                        error!("{ctx} Error sending connect result: {e}");
                    }
                }
                Msg::LinkOutput {
                    port,
                    pipelines: pipelines_to_link,
                    result_tx,
                } => {
                    for (url, _) in &pipelines_to_link {
                        info!("{ctx} Connecting {url} via port {port}");
                    }

                    if let Some(port_pipes) = connected_pipelines.get_mut(&port) {
                        port_pipes.extend(pipelines_to_link.iter().cloned());
                    } else {
                        connected_pipelines.insert(port.clone(), pipelines_to_link.clone());
                    }
                    let res = if connector.is_valid_output_port(&port) {
                        // connect to source part
                        if let Some(source) = connector_addr.source.as_ref() {
                            source
                                .addr
                                .send(SourceMsg::Link {
                                    port,
                                    pipelines: pipelines_to_link,
                                })
                                .await
                                .map_err(Into::into)
                        } else {
                            Err(ErrorKind::InvalidConnect(
                                connector_addr.alias.to_string(),
                                port.clone(),
                            )
                            .into())
                        }
                    } else {
                        error!("{ctx} Tried to connect to unsupported port: \"{port}\"");
                        Err(ErrorKind::InvalidConnect(
                            connector_addr.alias.to_string(),
                            port.clone(),
                        )
                        .into())
                    };
                    // send back the connect result
                    log_error!(
                        result_tx.send(res).await,
                        "{ctx} Error sending connect result: {e}"
                    );
                }
                Msg::ConnectionLost => {
                    // react on the connection being lost
                    // immediately try to reconnect if we are not in draining state.
                    //
                    // TODO: this might lead to very fast retry loops if the connection is established as connector.connect() returns successful
                    //       but in the next instant fails and sends this message.
                    connectivity = Connectivity::Disconnected;
                    info!("{} Connection lost.", &ctx);
                    connector_addr.send_sink(SinkMsg::ConnectionLost).await?;
                    connector_addr
                        .send_source(SourceMsg::ConnectionLost)
                        .await?;

                    // reconnect if running - wait with reconnect if paused (until resume)
                    if connector_state == State::Running {
                        // ensure we don't reconnect in a hot loop
                        // ensure we adhere to the reconnect strategy, waiting and possibly not reconnecting at all
                        reconnect.enqueue_retry(&ctx).await;
                    }
                }
                Msg::Reconnect => {
                    // reconnect if we are below max_retries, otherwise bail out and fail the connector
                    info!("{} Connecting...", &ctx);
                    let (new, will_retry) = reconnect.attempt(connector.as_mut(), &ctx).await?;
                    match (&connectivity, &new) {
                        (Connectivity::Disconnected, Connectivity::Connected) => {
                            info!("{} Connected.", &ctx);
                            // notify sink
                            connector_addr
                                .send_sink(SinkMsg::ConnectionEstablished)
                                .await?;
                            connector_addr
                                .send_source(SourceMsg::ConnectionEstablished)
                                .await?;
                            if let Some(start_sender) = start_sender.take() {
                                ctx.swallow_err(
                                    start_sender.send(ConnectorResult::ok(&ctx)).await,
                                    "Error sending start response.",
                                );
                            }
                        }
                        (Connectivity::Connected, Connectivity::Disconnected) => {
                            info!("{} Disconnected.", &ctx);
                            connector_addr.send_sink(SinkMsg::ConnectionLost).await?;
                            connector_addr
                                .send_source(SourceMsg::ConnectionLost)
                                .await?;
                        }
                        _ => {
                            debug!("{} No change after reconnect: {:?}", &ctx, &new);
                        }
                    }
                    // ugly extra check
                    if new == Connectivity::Disconnected && !will_retry {
                        // if we weren't able to connect and gave up retrying, we are failed. That's life.
                        connector_state = State::Failed;
                        if let Some(start_sender) = start_sender.take() {
                            ctx.swallow_err(
                                start_sender
                                    .send(ConnectorResult::err(&ctx, "Connect failed."))
                                    .await,
                                "Error sending start response",
                            );
                        }
                    }
                    connectivity = new;
                }
                Msg::Start(sender) if connector_state == State::Initializing => {
                    info!("{ctx} Starting...");
                    start_sender = Some(sender);

                    // start connector
                    connector_state = match connector.on_start(&ctx).await {
                        Ok(()) => State::Running,
                        Err(e) => {
                            error!("{ctx} on_start Error: {e}");
                            State::Failed
                        }
                    };
                    info!("{ctx} Started. New state: {connector_state}",);
                    // forward to source/sink if available
                    connector_addr.send_source(SourceMsg::Start).await?;
                    connector_addr.send_sink(SinkMsg::Start).await?;

                    // initiate connect asynchronously
                    connector_addr.sender.send(Msg::Reconnect).await?;
                }
                Msg::Start(sender) => {
                    info!("{ctx} Ignoring Start Msg. Current state: {connector_state}",);
                    if connector_state == State::Running && connectivity == Connectivity::Connected
                    {
                        // sending an answer if we are connected
                        ctx.swallow_err(
                            sender.send(ConnectorResult::ok(&ctx)).await,
                            "Error sending Start result",
                        );
                    }
                }

                Msg::Pause if connector_state == State::Running => {
                    info!("{ctx} Pausing...");

                    // TODO: in implementations that don't really support pausing
                    //       issue a warning/error message
                    //       e.g. UDP, TCP, Rest
                    //
                    ctx.swallow_err(connector.on_pause(&ctx).await, "Error during on_pause");
                    connector_state = State::Paused;
                    quiescence_beacon.pause();

                    connector_addr.send_source(SourceMsg::Pause).await?;
                    connector_addr.send_sink(SinkMsg::Pause).await?;

                    info!("{ctx} Paused.");
                }
                Msg::Pause => {
                    info!("{ctx} Ignoring Pause Msg. Current state: {connector_state}",);
                }
                Msg::Resume if connector_state == State::Paused => {
                    info!("{ctx} Resuming...");
                    ctx.swallow_err(connector.on_resume(&ctx).await, "Error during on_resume");
                    connector_state = State::Running;
                    quiescence_beacon.resume();

                    connector_addr.send_source(SourceMsg::Resume).await?;
                    connector_addr.send_sink(SinkMsg::Resume).await?;

                    if connectivity == Connectivity::Disconnected {
                        info!("{ctx} Triggering reconnect as part of resume.",);
                        connector_addr.send(Msg::Reconnect).await?;
                    }

                    info!("{ctx} Resumed.");
                }
                Msg::Resume => {
                    info!("{ctx} Ignoring Resume Msg. Current state: {connector_state}",);
                }
                Msg::Drain(_) if connector_state == State::Draining => {
                    info!("{ctx} Ignoring Drain Msg. Current state: {connector_state}",);
                }
                Msg::Drain(tx) => {
                    info!("{ctx} Draining...");

                    // notify connector that it should stop reading - so no more new events arrive at its source part
                    quiescence_beacon.stop_reading();
                    // let connector stop emitting anything to its source part - if possible here
                    ctx.swallow_err(connector.on_drain(&ctx).await, "Error during on_drain");
                    connector_state = State::Draining;

                    // notify source to drain the source channel and then send the drain signal
                    if let Some(source) = connector_addr.source.as_ref() {
                        source
                            .addr
                            .send(SourceMsg::Drain(connector_addr.sender.clone()))
                            .await?;
                    } else {
                        // proceed to the next step, even without source
                        connector_addr.send(Msg::SourceDrained).await?;
                    }

                    let d = Drainage::new(&connector_addr, tx);
                    if d.all_drained() {
                        info!("{ctx} Drained.");
                        log_error!(
                            d.send_all_drained().await,
                            "{ctx} error signalling being fully drained: {e}"
                        );
                    }
                    drainage = Some(d);
                }
                Msg::SourceDrained if connector_state == State::Draining => {
                    info!("{ctx} Source-part is drained.",);
                    if let Some(drainage) = drainage.as_mut() {
                        drainage.set_source_drained();
                        if drainage.all_drained() {
                            info!("{ctx} Drained.");
                            log_error!(
                                drainage.send_all_drained().await,
                                "{ctx} Error signalling being fully drained: {e}"
                            );
                        } else {
                            // notify sink to go into DRAIN state
                            // flush all events until we received a drain signal from all inputs
                            if let Some(sink) = connector_addr.sink.as_ref() {
                                sink.addr
                                    .send(SinkMsg::Drain(connector_addr.sender.clone()))
                                    .await?;
                            } else {
                                // proceed to the next step, even without sink
                                connector_addr.send(Msg::SinkDrained).await?;
                            }
                        }
                    }
                }
                Msg::SinkDrained if connector_state == State::Draining => {
                    info!("{ctx} Sink-part is drained.",);
                    if let Some(drainage) = drainage.as_mut() {
                        drainage.set_sink_drained();
                        quiescence_beacon.full_stop(); // TODO: maybe this should be done in the SinkManager?
                        if drainage.all_drained() {
                            info!("{ctx} Drained.");
                            log_error!(
                                drainage.send_all_drained().await,
                                "{ctx} Error signalling being fully drained: {e}"
                            );
                        }
                    }
                }
                Msg::SourceDrained => {
                    info!("{ctx} Ignoring SourceDrained Msg. Current state: {connector_state}",);
                }
                Msg::SinkDrained => {
                    info!("{ctx} Ignoring SourceDrained Msg. Current state: {connector_state}",);
                }
                Msg::Stop(sender) => {
                    info!("{ctx} Stopping...");
                    ctx.swallow_err(connector.on_stop(&ctx).await, "Error during on_stop");
                    connector_state = State::Stopped;
                    quiescence_beacon.full_stop();
                    let (stop_tx, stop_rx) = bounded(2);
                    let mut expect = usize::from(connector_addr.has_source())
                        + usize::from(connector_addr.has_sink());
                    ctx.swallow_err(
                        connector_addr
                            .send_source(SourceMsg::Stop(stop_tx.clone()))
                            .await,
                        "Error sending Stop msg to Source",
                    );
                    ctx.swallow_err(
                        connector_addr.send_sink(SinkMsg::Stop(stop_tx)).await,
                        "Error sending Stop msg to Sink",
                    );
                    while expect > 0 {
                        log_error!(
                            stop_rx.recv().await,
                            "{ctx} Error in stopping sink and source part: {e}"
                        );

                        expect -= 1;
                    }
                    log_error!(
                        sender.send(ConnectorResult::ok(&ctx)).await,
                        "{ctx} Error sending Stop result: {e}"
                    );

                    info!("{ctx} Stopped.");
                    break;
                }
            } // match
        } // while
        info!("{ctx} Connector Stopped. Reason: {connector_state}");
        // TODO: inform registry that this instance is gone now
        Ok(())
    });
    Ok(send_addr)
}

#[derive(Debug, PartialEq)]
enum DrainState {
    None,
    Expect,
    Drained,
}

struct Drainage {
    tx: Sender<ConnectorResult<()>>,
    alias: String,
    source_drained: DrainState,
    sink_drained: DrainState,
}

impl Drainage {
    fn new(addr: &Addr, tx: Sender<ConnectorResult<()>>) -> Self {
        Self {
            tx,
            alias: addr.alias.clone(),
            source_drained: if addr.has_source() {
                DrainState::Expect
            } else {
                DrainState::None
            },
            sink_drained: if addr.has_sink() {
                DrainState::Expect
            } else {
                DrainState::None
            },
        }
    }

    fn set_sink_drained(&mut self) {
        self.sink_drained = DrainState::Drained;
    }

    fn set_source_drained(&mut self) {
        self.source_drained = DrainState::Drained;
    }

    fn all_drained(&self) -> bool {
        // None and Drained are valid here
        self.source_drained != DrainState::Expect && self.sink_drained != DrainState::Expect
    }

    async fn send_all_drained(&self) -> Result<()> {
        self.tx
            .send(ConnectorResult {
                alias: self.alias.clone(),
                res: Ok(()),
            })
            .await?;
        Ok(())
    }
}

/// describes connectivity state of the connector
#[derive(Debug, Serialize, Copy, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Connectivity {
    /// connector is connected
    Connected,
    /// connector is disconnected
    Disconnected,
}

const IN_PORTS: [Cow<'static, str>; 1] = [IN];
const IN_PORTS_REF: &[Cow<'static, str>; 1] = &IN_PORTS;
const OUT_PORTS: [Cow<'static, str>; 2] = [OUT, ERR];
const OUT_PORTS_REF: &[Cow<'static, str>; 2] = &OUT_PORTS;

/// A Connector connects the tremor runtime to the outside world.
///
/// It can be a source of events, as such it is polled for new data.
/// It can also be a sink for events, as such events are sent to it from pipelines.
/// A connector can act as sink and source or just as one of those.
///
/// A connector encapsulates the establishment and maintenance of connections to the outside world,
/// such as tcp connections, file handles etc. etc.
///
/// It is a meta entity on top of the sink and source part.
/// The connector has its own control plane and is an artefact in the tremor repository.
/// It controls the sink and source parts which are connected to the rest of the runtime via links to pipelines.
#[abi_stable::sabi_trait]
pub trait RawConnector: Send {
    /// Valid input ports for the connector, by default this is `in`
    fn input_ports(&self) -> RVec<RCowStr<'static>> {
        IN_PORTS_REF
            .into_iter()
            .map(|port| conv_cow_str_inv(port.clone()))
            .collect()
    }
    /// Valid output ports for the connector, by default this is `out` and `err`
    fn output_ports(&self) -> RVec<RCowStr<'static>> {
        OUT_PORTS_REF
            .into_iter()
            .map(|port| conv_cow_str_inv(port.clone()))
            .collect()
    }

    /// Tests if a input port is valid, by default does a case insensitive search against
    /// `self.input_ports()`
    fn is_valid_input_port(&self, port: &str) -> bool {
        for valid in self.input_ports() {
            if port.eq_ignore_ascii_case(valid.as_ref()) {
                return true;
            }
        }
        false
    }

    /// Tests if a input port is valid, by default does a case insensitive search against
    /// `self.output_ports()`
    fn is_valid_output_port(&self, port: &str) -> bool {
        for valid in self.output_ports() {
            if port.eq_ignore_ascii_case(valid.as_ref()) {
                return true;
            }
        }
        false
    }

    /// create a source part for this connector if applicable
    ///
    /// This function is called exactly once upon connector creation.
    /// If this connector does not act as a source, return `Ok(None)`.
    async fn create_source(
        &mut self,
        _source_context: SourceContext,
        _builder: source::SourceManagerBuilder,
    ) -> BorrowingFfiFuture<'_, Result<Option<source::SourceAddr>>> {
        futures::ready(Ok(None)).into_ffi()
    }

    /// Create a sink part for this connector if applicable
    ///
    /// This function is called exactly once upon connector creation.
    /// If this connector does not act as a sink, return `Ok(None)`.
    async fn create_sink(
        &mut self,
        _sink_context: SinkContext,
        _builder: sink::SinkManagerBuilder,
    ) -> Result<Option<sink::SinkAddr>> {
        Ok(None)
    }

    /// Attempt to connect to the outside world.
    /// Return `Ok(true)` if a connection could be established.
    /// This method will be retried if it fails or returns `Ok(false)`.
    ///
    /// To notify the runtime of the main connectivity being lost, a `notifier` is passed in.
    /// Call `notifier.notify().await` as the last thing when you notice the connection is lost.
    /// This is well suited when handling the connection in another task.
    ///
    /// The attempt is the number of the connection attempt, the number this method has been called on this connector.
    /// The very first attempt to establish a connection will be `0`.
    /// All further attempts will be
    ///
    /// To know when to stop reading new data from the external connection, the `quiescence` beacon
    /// can be used. Call `.reading()` and `.writing()` to see if you should continue doing so, if not, just stop and rest.
    fn connect<'a>(
        &'a mut self,
        _ctx: &'a ConnectorContext,
        _attempt: &'a Attempt,
    ) -> BorrowingFfiFuture<'a, RResult<bool>> {
        future::ready(ROk(true)).into_ffi()
    }

    /// called once when the connector is started
    /// `connect` will be called after this for the first time, leave connection attempts in `connect`.
    async fn on_start(&mut self, _ctx: &ConnectorContext) -> Result<()> {
        Ok(())
    }

    /// called when the connector pauses
    async fn on_pause(&mut self, _ctx: &ConnectorContext) -> Result<()> {
        Ok(())
    }
    /// called when the connector resumes
    async fn on_resume(&mut self, _ctx: &ConnectorContext) -> Result<()> {
        Ok(())
    }

    /// Drain
    ///
    /// Ensure no new events arrive at the source part of this connector when this function returns
    /// So we can safely send the `Drain` signal.
    async fn on_drain(&mut self, _ctx: &ConnectorContext) -> Result<()> {
        Ok(())
    }

    /// called when the connector is stopped
    async fn on_stop(&mut self, _ctx: &ConnectorContext) -> Result<()> {
        Ok(())
    }

    /// Returns the codec requirements for the connector
    fn codec_requirements(&self) -> CodecReq;
}

/// Specifeis if a connector requires a codec
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum CodecReq {
    /// No codec can be provided for this connector it always returns structured data
    Structured,
    /// A codec must be provided for this connector
    Required,
    /// A codec can be provided for this connector otherwise the default is used
    Optional(RStr<'static>),
}

/// Alias for the FFI-safe dynamic connector type
pub type BoxedRawConnector = RawConnector_TO<'static, RBox<()>>;

/// The higher level connector interface, which wraps the raw connector from the
/// plugin. This should always be used for maximum usability and readability,
/// instead of the underlying `BoxedRawConnector`.
///
/// Note that it may hurt performance in some parts of the connector interface,
/// so some of the functionality may not be fully wrapped.
pub(crate) struct Connector(pub BoxedRawConnector);
impl Connector {
    #[inline]
    pub fn input_ports(&self) -> Vec<Cow<'static, str>> {
        self.0.input_ports().into_iter().map(conv_cow_str).collect()
    }
    #[inline]
    pub fn output_ports(&self) -> Vec<Cow<'static, str>> {
        self.0
            .output_ports()
            .into_iter()
            .map(conv_cow_str)
            .collect()
    }

    #[inline]
    pub fn is_valid_input_port(&self, port: &str) -> bool {
        self.0.is_valid_input_port(port.into())
    }

    #[inline]
    pub fn is_valid_output_port(&self, port: &str) -> bool {
        self.0.is_valid_output_port(port.into())
    }

    #[inline]
    pub async fn create_source(
        &mut self,
        source_context: SourceContext,
        builder: source::SourceManagerBuilder,
    ) -> Result<Option<source::SourceAddr>> {
        match self
            .0
            .create_source(source_context.clone(), builder.qsize())
            .await
        {
            ROk(RSome(source)) => builder.spawn(source, source_context).map(Some),
            ROk(RNone) => Ok(None),
            RErr(err) => Err(err.into()),
        }
    }

    #[inline]
    pub async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: sink::SinkManagerBuilder,
    ) -> Result<Option<sink::SinkAddr>> {
        // Note that we actually want to be able to downcast back to the
        // original type here, so that it's easier to manage in the runtime.
        let reply_tx = BoxedContraflowSender::from_value(builder.reply_tx(), TD_CanDowncast);
        match self
            .0
            .create_sink(sink_context.clone(), builder.qsize(), reply_tx)
            .await
        {
            ROk(RSome(sink)) => builder.spawn(sink, sink_context).map(Some),
            ROk(RNone) => Ok(None),
            RErr(err) => Err(err.into()),
        }
    }

    #[inline]
    pub async fn connect(&mut self, ctx: &ConnectorContext, attempt: &Attempt) -> Result<bool> {
        self.0
            .connect(ctx, attempt)
            .await
            .map_err(Into::into) // RBoxError -> Box<dyn Error>
            .into() // RResult -> Result
    }

    #[inline]
    pub async fn on_start(&mut self, ctx: &ConnectorContext) -> Result<()> {
        self.0
            .on_start(ctx)
            .await
            .map_err(Into::into) // RBoxError -> Box<dyn Error>
            .into() // RResult -> Result
    }

    #[inline]
    pub async fn on_pause(&mut self, ctx: &ConnectorContext) -> Result<()> {
        self.0
            .on_pause(ctx)
            .await
            .map_err(Into::into) // RBoxError -> Box<dyn Error>
            .into() // RResult -> Result
    }

    #[inline]
    pub async fn on_resume(&mut self, ctx: &ConnectorContext) -> Result<()> {
        self.0
            .on_resume(ctx)
            .await
            .map_err(Into::into) // RBoxError -> Box<dyn Error>
            .into() // RResult -> Result
    }

    #[inline]
    pub async fn on_drain(&mut self, ctx: &ConnectorContext) -> Result<()> {
        self.0
            .on_drain(ctx)
            .await
            .map_err(Into::into) // RBoxError -> Box<dyn Error>
            .into() // RResult -> Result
    }

    #[inline]
    pub async fn on_stop(&mut self, ctx: &ConnectorContext) -> Result<()> {
        self.0
            .on_stop(ctx)
            .await
            .map_err(Into::into) // RBoxError -> Box<dyn Error>
            .into() // RResult -> Result
    }

    #[inline]
    pub fn codec_requirements(&self) -> CodecReq {
        self.0.codec_requirements()
    }
}

/// the type of a connector
#[repr(C)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Default, StableAbi)]
pub struct ConnectorType(RString);

impl From<ConnectorType> for String {
    fn from(ct: ConnectorType) -> Self {
        ct.0
    }
}

impl From<ConnectorType> for RString {
    fn from(ct: ConnectorType) -> Self {
        ct.0
    }
}

impl<'t> From<&'t ConnectorType> for &'t str {
    fn from(ct: &'t ConnectorType) -> Self {
        ct.0.as_str()
    }
}

impl Display for ConnectorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.as_str())
    }
}

impl From<String> for ConnectorType {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl<T> From<&T> for ConnectorType
where
    T: ToString + ?Sized,
{
    fn from(s: &T) -> Self {
        Self(s.to_string())
    }
}

/// builtin connector types
#[cfg(not(tarpaulin_include))]
#[must_use]
pub fn builtin_connector_types() -> Vec<ConnectorMod_Ref> {
    // FIXME: implement basic connectors
    vec![
        impls::file::instantiate_root_module(),
        impls::metrics::instantiate_root_module(),
        impls::tcp::server::instantiate_root_module(),
    ]
}

/// debug connector types
#[cfg(not(tarpaulin_include))]
#[must_use]
pub fn debug_connector_types() -> Vec<ConnectorMod_Ref> {
    vec![
        // Box::new(impls::cb::Builder::default()),
        impls::bench::instantiate_root_module(),
        // Box::new(impls::null::Builder::default()),
    ]
}

/// registering builtin and debug connector types
///
/// # Errors
///  * If a builtin connector couldn't be registered
#[cfg(not(tarpaulin_include))]
pub(crate) async fn register_builtin_connector_types(world: &World, debug: bool) -> Result<()> {
    for builder in builtin_connector_types() {
        world.register_builtin_connector_type(builder).await?;
    }
    if debug {
        for builder in debug_connector_types() {
            world.register_builtin_connector_type(builder).await?;
        }
    }

    // After loading all the in-tree connectors, we try to find all the
    // available plugins and we load them dynamically. For now, plugins are
    // loaded from the path defined by `TREMOR_PLUGIN_PATH`.
    let plugin_path =
        env::var("TREMOR_PLUGIN_PATH").unwrap_or_else(|_| String::from(DEFAULT_PLUGIN_PATH));
    for path in plugin_path.split(':') {
        log::info!("Dynamically loading plugins in directory '{}'", path);
        for plugin in pdk::find_recursively(&path) {
            log::info!("Found and loaded plugin '{}'", plugin.connector_type()());
            world.register_builtin_connector_type(plugin).await?;
        }
    }

    Ok(())
}

/// Function to spawn an acceptor task in the runtime, this forbids returning Result so
/// that `?` can't be used in those tasks and silent errors with acceptor tasks dying
/// are eliminated.
pub(crate) fn spawn_task<F, C>(ctx: C, t: F) -> JoinHandle<()>
where
    F: Future<Output = Result<()>> + Send + 'static,
    C: Context + Send + 'static,
{
    task::spawn(async move {
        log_error!(t.await, "{ctx} Connector loop error: {e}");
        // notify connector task about disconnect
        // of the listening socket
        let n = ctx.notifier();
        log_error!(
            n.connection_lost().await,
            "{ctx} Failed to notify on connection lost: {e}"
        );
    })
}
