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

// FIXME: uncomment
pub(crate) mod impls;

/// prelude with commonly needed stuff imported
// FIXME: this should be reorganized after the pdk is moved to a separate crate
// (it used to be `pub(crate)`).
pub mod prelude;

/// sink parts
pub mod sink;

/// source parts
// FIXME: this should be reorganized after the pdk is moved to a separate crate
// (it used to be `pub(crate)`).
pub mod source;
// FIXME: this should be reorganized after the pdk is moved to a separate crate
// (it used to be `pub(crate)`).
#[macro_use]
pub mod utils;

use std::{env, fmt::Display, future, path::Path};

/// quiescence stuff
pub(crate) use utils::{metrics, quiescence, reconnect};

use async_std::task::{self, JoinHandle};
use beef::Cow;

use self::metrics::{MetricsSender, SinkReporter, SourceReporter};
use self::sink::{BoxedRawSink, Sink, SinkAddr, SinkContext, SinkMsg};
use self::source::{BoxedRawSource, Source, SourceAddr, SourceContext, SourceMsg};
use self::utils::quiescence::{
    BoxedQuiescenceBeacon, QuiescenceBeacon, QuiescenceBeaconOpaque, QuiescenceBeaconOpaque_TO,
};
use crate::config::Connector as ConnectorConfig;
use crate::errors::{Error, ErrorKind, Result};
use crate::pdk::{self, connectors::ConnectorMod_Ref, RResult};
use crate::pipeline;
use crate::system::World;
use crate::OpConfig;
use abi_stable::{
    library::RootModule,
    rslice,
    std_types::{
        RBox, RCow,
        ROption::{self, RNone, RSome},
        RResult::{RErr, ROk},
        RSlice, RStr, RString, RVec,
    },
    type_level::downcasting::TD_Opaque,
    StableAbi,
};
use async_ffi::{BorrowingFfiFuture, FutureExt};
use async_std::channel::{bounded, Sender};
use async_std::task::{self};
use beef::Cow;
use halfbrown::HashMap;
use std::{fmt::Display, sync::atomic::Ordering};
use tremor_common::ids::ConnectorIdGen;
use tremor_common::url::{
    ports::{ERR, IN, OUT},
    TremorUrl,
};
use tremor_value::{pdk::PdkValue, Value};
use utils::reconnect::{Attempt, ReconnectRuntime};
use value_trait::{Builder, Mutable};

/// sender for connector manager messages
pub type ManagerSender = Sender<ManagerMsg>;

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
    uid: u64,
    /// connector instance url
    pub alias: String,
    sender: Sender<Msg>,
    source: Option<SourceAddr>,
    pub(crate) sink: Option<SinkAddr>,
}

impl Addr {
    /// send a message
    ///
    /// # Errors
    ///  * If sending failed
    pub async fn send(&self, msg: Msg) -> Result<()> {
        Ok(self.sender.send(msg).await?)
    }

    /// send a message to the sink part of the connector.
    /// Results in a no-op if the connector has no sink part.
    ///
    /// # Errors
    ///   * if sending failed
    pub async fn send_sink(&self, msg: SinkMsg) -> Result<()> {
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
    pub async fn send_source(&self, msg: SourceMsg) -> Result<()> {
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
    pub async fn stop(&self, sender: Sender<ConnectorResult<()>>) -> Result<()> {
        self.send(Msg::Stop(sender)).await
    }
    /// starts the connector
    pub async fn start(&self, sender: Sender<ConnectorResult<()>>) -> Result<()> {
        self.send(Msg::Start(sender)).await
    }
    /// drains the connector
    pub async fn drain(&self, sender: Sender<ConnectorResult<()>>) -> Result<()> {
        self.send(Msg::Drain(sender)).await
    }
    /// pauses the connector
    pub async fn pause(&self) -> Result<()> {
        self.send(Msg::Pause).await
    }
    /// resumes the connector
    pub async fn resume(&self) -> Result<()> {
        self.send(Msg::Resume).await
    }
}

/// Messages a Connector instance receives and acts upon
pub enum Msg {
    /// connect 1 or more pipelines to a port
    Link {
        /// port to which to connect
        port: Cow<'static, str>,
        /// pipelines to connect
        pipelines: Vec<(DeployEndpoint, pipeline::Addr)>,
        /// result receiver
        result_tx: Sender<Result<()>>,
    },
    /// disconnect pipeline `id` from the given `port`
    Unlink {
        /// port from which to disconnect
        port: Cow<'static, str>,
        /// id of the pipeline to disconnect
        id: DeployEndpoint,
        /// sender to receive a boolean whether this connector is not connected to anything
        tx: Sender<Result<bool>>,
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
pub struct ConnectorResult<T: std::fmt::Debug> {
    /// the connector url
    pub alias: String,
    /// the actual result
    pub res: Result<T>,
}

impl ConnectorResult<()> {
    fn ok(ctx: &ConnectorContext) -> Self {
        Self {
            alias: ctx.alias.clone(),
            res: Ok(()),
        }
    }

    fn err(ctx: &ConnectorContext, err_msg: &'static str) -> Self {
        Self {
            alias: ctx.alias.clone(),
            res: Err(Error::from(err_msg)),
        }
    }
}

/// context for a Connector or its parts
pub trait Context: Display + Clone {
    /// provide the url of the connector
    fn alias(&self) -> &str;

    /// get the quiescence beacon for checking if we should continue reading/writing
    fn quiescence_beacon(&self) -> &QuiescenceBeacon;

    /// get the notifier to signal to the runtime that we are disconnected
    fn notifier(&self) -> &reconnect::ConnectionLostNotifier;

    /// get the connector type
    fn connector_type(&self) -> &ConnectorType;

    /// only log an error and swallow the result
    fn log_err<T, E, M>(&self, expr: std::result::Result<T, E>, msg: &M)
    where
        E: std::error::Error,
        M: Display + ?Sized,
    {
        if let Err(e) = expr {
            error!("{} {}: {}", self, msg, e);
        }
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
}

/// connector context
#[repr(C)]
#[derive(Clone, StableAbi)]
pub struct ConnectorContext {
    /// unique identifier
    pub uid: u64,
    /// url of the connector
    pub alias: String,
    /// type of the connector
    connector_type: ConnectorType,
    /// The Quiescence Beacon
    pub quiescence_beacon: BoxedQuiescenceBeacon,
    /// Notifier
    pub notifier: reconnect::BoxedConnectionLostNotifier,
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

    fn quiescence_beacon(&self) -> &QuiescenceBeacon {
        &self.quiescence_beacon
    }

    fn notifier(&self) -> &reconnect::ConnectionLostNotifier {
        &self.notifier
    }
}

/// Connector instance status report
#[derive(Debug, Serialize)]
pub struct StatusReport {
    /// connector instance url
    pub alias: String,
    /// state of the connector
    pub status: InstanceState,
    /// current connectivity
    pub connectivity: Connectivity,
    /// connected pipelines
    pub pipelines: HashMap<Cow<'static, str>, Vec<DeployEndpoint>>,
}

/// msg used for connector creation
#[derive(Debug)]
pub struct Create {
    /// instance id
    pub servant_id: TremorUrl,
    /// config
    pub config: ConnectorConfig,
}

impl Create {
    /// constructor
    #[must_use]
    pub fn new(servant_id: TremorUrl, config: ConnectorConfig) -> Self {
        Self { servant_id, config }
    }
}

/// Stream id generator
#[derive(Debug, Default)]
pub struct StreamIdGen(u64);

impl StreamIdGen {
    /// get the next stream id and increment the internal state
    pub fn next_stream_id(&mut self) -> u64 {
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
pub enum StreamDone {
    /// Only this stream is closed, (only one of many)
    StreamClosed,
    /// With this stream being closed, the whole connector can be considered done/closed
    ConnectorClosed,
}

/// msg for the `ConnectorManager` handling all connectors
pub enum ManagerMsg {
    /// register a new connector type
    Register {
        /// the type of connector
        connector_type: ConnectorType,
        /// the builder
        builder: ConnectorMod_Ref,
        /// if this one is a builtin connector
        builtin: bool,
    },
    /// unregister a connector type
    Unregister(ConnectorType),
    /// create a new connector
    Create {
        /// sender to send the create result to
        tx: Sender<Result<Addr>>,
        /// the create command
        create: Box<Create>,
    },
    /// stop the connector manager
    Stop {
        /// reason
        reason: String,
    },
}

#[allow(clippy::too_many_lines)]
// instantiates the connector and starts listening for control plane messages
async fn connector_task(
    alias: String,
    mut connector: Box<dyn Connector>,
    config: ConnectorConfig,
    uid: u64,
) -> Result<Addr> {
    let qsize = crate::QSIZE.load(Ordering::Relaxed);
    // channel for connector-level control plane communication
    let (msg_tx, msg_rx) = bounded(qsize);

    let mut connectivity = Connectivity::Disconnected;
    let mut quiescence_beacon = QuiescenceBeacon::default();
    let notifier = ConnectionLostNotifier::new(msg_tx.clone());

    let source_metrics_reporter = SourceReporter::new(
        alias.clone(),
        METRICS_CHANNEL.tx(),
        config.metrics_interval_s,
    );

    let default_codec = connector.default_codec();
    if connector.is_structured() && (config.codec.is_some() || config.codec_map.is_some()) {
        return Err(format!(
            "[Connector::{}] is a structured connector and can't be configured with a codec",
            alias
        )
        .into());
    }
    let source_builder =
        source::builder(uid, &config, default_codec, qsize, source_metrics_reporter)?;
    let source_ctx = SourceContext {
        alias: alias.clone(),
        uid,
        connector_type: config.connector_type.clone(),
        quiescence_beacon: quiescence_beacon.clone(),
        notifier: notifier.clone(),
    };

    /// start the manager
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn start(self) -> (JoinHandle<Result<()>>, ManagerSender) {
        let (tx, rx) = bounded(self.qsize);
        let h = task::spawn(async move {
            info!("Connector manager started.");
            let mut connector_id_gen = ConnectorIdGen::new();
            let mut known_connectors: HashMap<ConnectorType, (ConnectorMod_Ref, bool)> =
                HashMap::with_capacity(16);

            loop {
                match rx.recv().await {
                    Ok(ManagerMsg::Create { tx, create }) => {
                        let url = create.servant_id.to_instance();
                        // lookup and instantiate connector
                        let connector = if let Some((builder, _)) =
                            known_connectors.get(&create.config.binding_type)
                        {
                            // The config is converted to a string so that it
                            // can be passed through FFI.
                            let config = create
                                .config
                                .config
                                .as_ref()
                                .and_then(|conf| serde_yaml::to_string(conf).ok())
                                .map(RString::from)
                                .into();
                            let connector_res = builder.from_config()(url.clone(), config).await;
                            match connector_res {
                                ROk(connector) => Connector(connector),
                                RErr(e) => {
                                    let e = ErrorKind::PluginError(e).into();
                                    error!(
                                        "[Connector] Error instantiating connector {}: {}",
                                        &url, e
                                    );
                                    tx.send(Err(e)).await?;
                                    continue;
                                }
                            }
                        } else {
                            error!(
                                "[Connector] Connector Type '{}' unknown",
                                &create.config.binding_type
                            );
                            tx.send(Err(ErrorKind::UnknownConnectorType(
                                create.config.binding_type.to_string(),
                            )
                            .into()))
                                .await?;
                            continue;
                        };
                        if let Err(e) = self
                            .connector_task(
                                tx.clone(),
                                url.clone(),
                                connector,
                                create.config,
                                connector_id_gen.next_id(),
                            )
                            .await
                        {
                            error!(
                                "[Connector] Error spawning task for connector {}: {}",
                                &url, e
                            );
                            tx.send(Err(e)).await?;
                        }
                    }
                    Ok(ManagerMsg::Register {
                        connector_type,
                        builder,
                        builtin,
                    }) => {
                        debug!("Registering {} Connector Type.", &connector_type);
                        match known_connectors.entry(connector_type) {
                            Entry::Occupied(e) => {
                                warn!("Connector Type {} already registered.", e.key());
                            }
                            Entry::Vacant(e) => {
                                debug!(
                                    "Connector Type {} registered{}.",
                                    e.key(),
                                    if builtin { " as builtin" } else { " " }
                                );
                                e.insert((builder, builtin));
                            }
                        }
                    }
                    Ok(ManagerMsg::Unregister(connector_type)) => {
                        debug!("Unregistering {} Connector Type.", &connector_type);
                        match known_connectors.entry(connector_type) {
                            Entry::Occupied(e) => {
                                let (_, builtin) = e.get();
                                if *builtin {
                                    error!("Cannot unregister builtin Connector Type {}", e.key());
                                } else {
                                    debug!("Connector Type {} unregistered.", e.key());
                                    e.remove_entry();
                                }
                            }
                            Entry::Vacant(e) => {
                                error!("Connector Type {} not registered", e.key());
                            }
                        }
                    }
                    Ok(ManagerMsg::Stop { reason }) => {
                        info!("Stopping Connector Manager... {}", reason);
                        break;
                    }
                    Err(e) => {
                        info!("Error! Stopping Connector Manager... {}", e);
                        break;
                    }
                }
            }
            info!("Connector Manager stopped.");
            Ok(())
        });
        (h, tx)
    }

    #[allow(clippy::too_many_lines)]
    // instantiates the connector and starts listening for control plane messages
    async fn connector_task(
        &self,
        addr_tx: Sender<Result<Addr>>,
        url: TremorUrl,
        mut connector: Connector,
        config: ConnectorConfig,
        uid: u64,
    ) -> Result<()> {
        // channel for connector-level control plane communication
        let (msg_tx, msg_rx) = bounded(self.qsize);

    let mut reconnect: ReconnectRuntime =
        ReconnectRuntime::new(&connector_addr, notifier.clone(), &config.reconnect);
    let notifier = reconnect.notifier();

    let ctx = ConnectorContext {
        uid,
        alias: alias.clone(),
        connector_type: config.connector_type.clone(),
        quiescence_beacon: quiescence_beacon.clone(),
        notifier: notifier,
    };

        let default_codec = connector.default_codec();
        if connector.is_structured() && (config.codec.is_some() || config.codec_map.is_some()) {
            return Err(format!(
                "[Connector::{}] is a structured connector and can't be configured with a codec",
                url
            )
            .into());
        }
        let source_builder = source::builder(
            uid,
            &config,
            default_codec,
            self.qsize,
            source_metrics_reporter,
        )?;
        let source_ctx = SourceContext {
            uid,
            url: url.clone(),
            quiescence_beacon: BoxedQuiescenceBeacon::from_value(
                quiescence_beacon.clone(),
                TD_Opaque,
            ),
        };

        let sink_metrics_reporter = MetricsSinkReporter::new(
            url.clone(),
            self.metrics_sender.clone(),
            config.metrics_interval_s,
        );
        let sink_builder =
            sink::builder(&config, default_codec, self.qsize, sink_metrics_reporter)?;
        let sink_ctx = SinkContext {
            uid,
            url: url.clone(),
        };
        // create source instance
        let source_addr = connector.create_source(source_ctx, source_builder).await?;

        // create sink instance
        let sink_addr = connector.create_sink(sink_ctx, sink_builder).await?;

        let connector_addr = Addr {
            uid,
            url: url.clone(),
            sender: msg_tx,
            source: source_addr,
            sink: sink_addr,
        };

        let mut reconnect: ReconnectRuntime =
            ReconnectRuntime::new(&connector_addr, &config.reconnect);
        let notifier = reconnect.notifier();

        let ctx = ConnectorContext {
            uid,
            url: url.clone(),
            type_name: config.binding_type.clone().into(),
            quiescence_beacon: BoxedQuiescenceBeacon::from_value(
                quiescence_beacon.clone(),
                TD_Opaque,
            ),
            notifier: reconnect::BoxedConnectionLostNotifier::from_value(
                notifier.clone(),
                TD_Opaque,
            ),
        };

        let send_addr = connector_addr.clone();
        let mut connector_state = ConnectorState::Initialized;
        let mut drainage = None;
        // TODO: add connector metrics reporter (e.g. for reconnect attempts, cb's received, uptime, etc.)
        task::spawn::<_, Result<()>>(async move {
            // typical 1 pipeline connected to IN, OUT, ERR
            let mut connected_pipelines: HashMap<
                Cow<'static, str>,
                Vec<(TremorUrl, pipeline::Addr)>,
            > = HashMap::with_capacity(3);
            // connector control plane loop
            while let Ok(msg) = msg_rx.recv().await {
                match msg {
                    Msg::Report(tx) => {
                        // request a status report from this connector
                        let pipes: HashMap<Cow<'static, str>, Vec<TremorUrl>> = connected_pipelines
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
                        error!("[Connector::{}] Error sending status report {}.", &alias, e);
                    }
                }
                Msg::Link {
                    port,
                    pipelines: pipelines_to_link,
                    result_tx,
                } => {
                    for (url, _) in &pipelines_to_link {
                        info!(
                            "[Connector::{}] Connecting {} via port {}",
                            &connector_addr.alias, &url, port
                        );
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
                                .map_err(|e| e.into())
                        } else {
                            Err(ErrorKind::InvalidConnect(
                                connector_addr.alias.to_string(),
                                port.clone(),
                            )
                            .into())
                        }
                    }
                    Msg::Unlink { port, id, tx } => {
                        let delete =
                            connected_pipelines
                                .get_mut(&port)
                                .map_or(false, |port_pipes| {
                                    port_pipes.retain(|(url, _)| url != &id);
                                    port_pipes.is_empty()
                                });
                        // make sure we can simply use `is_empty` for checking for emptiness
                        if delete {
                            connected_pipelines.remove(&port);
                        }
                        let res: Result<()> = if port.eq_ignore_ascii_case(IN.as_ref()) {
                            // disconnect from source part
                            match connector_addr.source.as_ref() {
                                Some(source) => source
                                    .addr
                                    .send(SourceMsg::Unlink { port, id })
                                    .await
                                    .map_err(Error::from),
                                None => Err(ErrorKind::InvalidDisconnect(
                                    connector_addr.url.to_string(),
                                    id.to_string(),
                                    port.clone(),
                                )
                                .into()),
                            }
                        } else {
                            // disconnect from sink part
                            match connector_addr.sink.as_ref() {
                                Some(sink) => sink
                                    .addr
                                    .send(SinkMsg::Disconnect { port, id })
                                    .await
                                    .map_err(Error::from),
                                None => Err(ErrorKind::InvalidDisconnect(
                                    connector_addr.url.to_string(),
                                    id.to_string(),
                                    port.clone(),
                                )
                                .into()),
                            }
                        };
                        // TODO: work out more fine grained "empty" semantics
                        tx.send(res.map(|_| connected_pipelines.is_empty())).await?;
                    }
                    Msg::ConnectionLost => {
                        // FIXME: we don't always want to reconnect - add a flag that determines if a reconnect is appropriate
                        // react on the connection being lost
                        // immediately try to reconnect if we are not in draining state.
                        //
                        // TODO: this might lead to very fast retry loops if the connection is established as connector.connect returns successful
                        //       but in the next instant fails and sends this message.
                        connectivity = Connectivity::Disconnected;
                        info!("[Connector::{}] Disconnected.", &connector_addr.url);
                        connector_addr.send_sink(SinkMsg::ConnectionLost).await?;
                        connector_addr
                            .send_source(SourceMsg::ConnectionLost)
                            .await?;

                        // reconnect if running - wait with reconnect if paused (until resume)
                        if connector_state == InstanceState::Running {
                            info!("[Connector::{}] Triggering reconnect.", &connector_addr.url);
                            connector_addr.sender.send(Msg::Reconnect).await?;
                        }
                    }
                    Msg::Reconnect => {
                        // reconnect if we are below max_retries, otherwise bail out and fail the connector
                        info!("[Connector::{}] Connecting...", &connector_addr.url);
                        let new = reconnect.attempt(&mut connector, &ctx).await?;
                        match (&connectivity, &new) {
                            (Connectivity::Disconnected, Connectivity::Connected) => {
                                info!("[Connector::{}] Connected.", &connector_addr.url);
                                // notify sink
                                connector_addr
                                    .send_sink(SinkMsg::ConnectionEstablished)
                                    .await?;
                                connector_addr
                                    .send_source(SourceMsg::ConnectionEstablished)
                                    .await?;
                            }
                            (Connectivity::Connected, Connectivity::Disconnected) => {
                                info!("[Connector::{}] Disconnected.", &connector_addr.url);
                                connector_addr.send_sink(SinkMsg::ConnectionLost).await?;
                                connector_addr
                                    .send_source(SourceMsg::ConnectionLost)
                                    .await?;
                            }
                            _ => {
                                debug!(
                                    "[Connector::{}] No change: {:?}",
                                    &connector_addr.url, &new
                                );
                            }
                        }
                        connectivity = new;
                    }
                    Msg::Start if connector_state == InstanceState::Initialized => {
                        info!("[Connector::{}] Starting...", &connector_addr.url);
                        // start connector
                        connector_state = match connector.on_start(&ctx).await {
                            Ok(()) => InstanceState::Running,
                            Err(e) => {
                                error!(
                                    "[Connector::{}] on_start Error: {}",
                                    &connector_addr.url, e
                                );
                                InstanceState::Failed
                            }
                        };
                        info!(
                            "[Connector::{}] Started. New state: {:?}",
                            &connector_addr.url, &connector_state
                        );
                        // forward to source/sink if available
                        connector_addr.send_source(SourceMsg::Start).await?;
                        connector_addr.send_sink(SinkMsg::Start).await?;

                        // initiate connect asynchronously
                        connector_addr.sender.send(Msg::Reconnect).await?;
                    }
                    Msg::Start => {
                        info!(
                            "[Connector::{}] Ignoring Start Msg. Current state: {:?}",
                            &connector_addr.url, &connector_state
                        );
                    }

                    Msg::Pause if connector_state == InstanceState::Running => {
                        info!("[Connector::{}] Pausing...", &connector_addr.url);

                        // TODO: in implementations that don't really support pausing
                        //       issue a warning/error message
                        //       e.g. UDP, TCP, Rest
                        //
                        ctx.log_err(connector.on_pause(&ctx).await, "Error during on_pause");
                        connector_state = InstanceState::Paused;
                        quiescence_beacon.pause();

                        connector_addr.send_source(SourceMsg::Pause).await?;
                        connector_addr.send_sink(SinkMsg::Pause).await?;

                        info!("[Connector::{}] Paused.", &connector_addr.url);
                    }
                    Msg::Pause => {
                        info!(
                            "[Connector::{}] Ignoring Pause Msg. Current state: {:?}",
                            &connector_addr.url, &connector_state
                        );
                    }
                    Msg::Resume if connector_state == InstanceState::Paused => {
                        info!("[Connector::{}] Resuming...", &connector_addr.url);
                        ctx.log_err(connector.on_resume(&ctx).await, "Error during on_resume");
                        connector_state = InstanceState::Running;
                        quiescence_beacon.resume();

                        connector_addr.send_source(SourceMsg::Resume).await?;
                        connector_addr.send_sink(SinkMsg::Resume).await?;

                        if connectivity == Connectivity::Disconnected {
                            info!(
                                "[Connector::{}] Triggering reconnect as part of resume.",
                                &connector_addr.url
                            );
                            connector_addr.send(Msg::Reconnect).await?;
                        }

                        info!("[Connector::{}] Resumed.", &connector_addr.url);
                    }
                    Msg::Resume => {
                        info!(
                            "[Connector::{}] Ignoring Resume Msg. Current state: {:?}",
                            &connector_addr.url, &connector_state
                        );
                    }
                    Msg::Drain(_) if connector_state == InstanceState::Draining => {
                        info!(
                            "[Connector::{}] Ignoring Drain Msg. Current state: {:?}",
                            &connector_addr.url, &connector_state
                        );
                    }
                    Msg::Drain(tx) => {
                        info!("[Connector::{}] Draining...", &connector_addr.url);

                        // notify connector that it should stop reading - so no more new events arrive at its source part
                        quiescence_beacon.stop_reading();
                        // FIXME: add stop_writing()
                        // let connector stop emitting anything to its source part - if possible here
                        ctx.log_err(connector.on_drain(&ctx).await, "Error during on_drain");
                        connector_state = InstanceState::Draining;

                        // notify source to drain the source channel and then send the drain signal
                        if let Some(source) = connector_addr.source.as_ref() {
                            source
                                .addr
                                .send(SourceMsg::Link {
                                    port,
                                    pipelines: pipelines_to_link,
                                })
                                .await
                                .map_err(|e| e.into())
                        } else {
                            Err(ErrorKind::InvalidConnect(
                                connector_addr.alias.to_string(),
                                port.clone(),
                            )
                            .into())
                        }
                    } else {
                        error!(
                            "[Connector::{}] Tried to connect to unsupported port: \"{}\"",
                            &connector_addr.alias, &port
                        );
                        Err(ErrorKind::InvalidConnect(
                            connector_addr.alias.to_string(),
                            port.clone(),
                        )
                        .into())
                    };
                    // send back the connect result
                    if let Err(e) = result_tx.send(res).await {
                        error!(
                            "[Connector::{}] Error sending connect result: {}",
                            &connector_addr.alias, e
                        );
                    }
                }
                Msg::Unlink { port, id, tx } => {
                    let delete = connected_pipelines
                        .get_mut(&port)
                        .map_or(false, |port_pipes| {
                            port_pipes.retain(|(url, _)| url != &id);
                            port_pipes.is_empty()
                        });
                    // make sure we can simply use `is_empty` for checking for emptiness
                    if delete {
                        connected_pipelines.remove(&port);
                    }
                    let res: Result<()> = if port.eq_ignore_ascii_case(IN.as_ref()) {
                        // disconnect from source part
                        match connector_addr.source.as_ref() {
                            Some(source) => source
                                .addr
                                .send(SourceMsg::Unlink { port, id })
                                .await
                                .map_err(Error::from),
                            None => Err(ErrorKind::InvalidDisconnect(
                                connector_addr.alias.to_string(),
                                id.to_string(),
                                port.clone(),
                            )
                            .into()),
                        }
                    } else {
                        // disconnect from sink part
                        match connector_addr.sink.as_ref() {
                            Some(sink) => sink
                                .addr
                                .send(SinkMsg::Unlink { port, id })
                                .await
                                .map_err(Error::from),
                            None => Err(ErrorKind::InvalidDisconnect(
                                connector_addr.alias.to_string(),
                                id.to_string(),
                                port.clone(),
                            )
                            .into()),
                        }
                    };
                    // TODO: work out more fine grained "empty" semantics
                    tx.send(res.map(|_| connected_pipelines.is_empty())).await?;
                }
                Msg::ConnectionLost => {
                    // FIXME: we don't always want to reconnect - add a flag that determines if a reconnect is appropriate
                    // react on the connection being lost
                    // immediately try to reconnect if we are not in draining state.
                    //
                    // TODO: this might lead to very fast retry loops if the connection is established as connector.connect returns successful
                    //       but in the next instant fails and sends this message.
                    connectivity = Connectivity::Disconnected;
                    info!("[Connector::{}] Disconnected.", &connector_addr.alias);
                    connector_addr.send_sink(SinkMsg::ConnectionLost).await?;
                    connector_addr
                        .send_source(SourceMsg::ConnectionLost)
                        .await?;

                    // reconnect if running - wait with reconnect if paused (until resume)
                    if connector_state == InstanceState::Running {
                        info!(
                            "[Connector::{}] Triggering reconnect.",
                            &connector_addr.alias
                        );
                        connector_addr.sender.send(Msg::Reconnect).await?;
                    }
                }
                Msg::Reconnect => {
                    // reconnect if we are below max_retries, otherwise bail out and fail the connector
                    info!("[Connector::{}] Connecting...", &connector_addr.alias);
                    let (new, will_retry) = reconnect.attempt(connector.as_mut(), &ctx).await?;
                    match (&connectivity, &new) {
                        (Connectivity::Disconnected, Connectivity::Connected) => {
                            info!("[Connector::{}] Connected.", &connector_addr.alias);
                            // notify sink
                            connector_addr
                                .send_sink(SinkMsg::ConnectionEstablished)
                                .await?;
                            connector_addr
                                .send_source(SourceMsg::ConnectionEstablished)
                                .await?;
                            if let Some(start_sender) = start_sender.take() {
                                ctx.log_err(
                                    start_sender.send(ConnectorResult::ok(&ctx)).await,
                                    "Error sending start response.",
                                );
                            }
                        }
                        (Connectivity::Connected, Connectivity::Disconnected) => {
                            info!("[Connector::{}] Disconnected.", &connector_addr.alias);
                            connector_addr.send_sink(SinkMsg::ConnectionLost).await?;
                            connector_addr
                                .send_source(SourceMsg::ConnectionLost)
                                .await?;
                        }
                        _ => {
                            debug!(
                                "[Connector::{}] No change: {:?}",
                                &connector_addr.alias, &new
                            );
                        }
                    }
                    // ugly extra check
                    if new == Connectivity::Disconnected && !will_retry && start_sender.is_some() {
                        if let Some(start_sender) = start_sender.take() {
                            ctx.log_err(
                                start_sender
                                    .send(ConnectorResult::err(&ctx, "Connect failed."))
                                    .await,
                                "Error sending start response",
                            )
                        }
                    }
                    connectivity = new;
                }
                Msg::Start(sender) if connector_state == InstanceState::Initialized => {
                    info!("[Connector::{}] Starting...", &connector_addr.alias);
                    start_sender = Some(sender);

                    // start connector
                    connector_state = match connector.on_start(&ctx).await {
                        Ok(()) => InstanceState::Running,
                        Err(e) => {
                            error!(
                                "[Connector::{}] on_start Error: {}",
                                &connector_addr.alias, e
                            );
                            InstanceState::Failed
                        }
                    };
                    info!(
                        "[Connector::{}] Started. New state: {:?}",
                        &connector_addr.alias, &connector_state
                    );
                    // forward to source/sink if available
                    connector_addr.send_source(SourceMsg::Start).await?;
                    connector_addr.send_sink(SinkMsg::Start).await?;

                    // initiate connect asynchronously
                    connector_addr.sender.send(Msg::Reconnect).await?;
                }
                Msg::Start(sender) => {
                    info!(
                        "[Connector::{}] Ignoring Start Msg. Current state: {:?}",
                        &connector_addr.alias, &connector_state
                    );
                    if connector_state == InstanceState::Running
                        && connectivity == Connectivity::Connected
                    {
                        // sending an answer if we are connected
                        ctx.log_err(
                            sender.send(ConnectorResult::ok(&ctx)).await,
                            "Error sending Start result",
                        );
                    }
                }

                Msg::Pause if connector_state == InstanceState::Running => {
                    info!("[Connector::{}] Pausing...", &connector_addr.alias);

                    // TODO: in implementations that don't really support pausing
                    //       issue a warning/error message
                    //       e.g. UDP, TCP, Rest
                    //
                    ctx.log_err(connector.on_pause(&ctx).await, "Error during on_pause");
                    connector_state = InstanceState::Paused;
                    quiescence_beacon.pause();

                    connector_addr.send_source(SourceMsg::Pause).await?;
                    connector_addr.send_sink(SinkMsg::Pause).await?;

                    info!("[Connector::{}] Paused.", &connector_addr.alias);
                }
                Msg::Pause => {
                    info!(
                        "[Connector::{}] Ignoring Pause Msg. Current state: {:?}",
                        &connector_addr.alias, &connector_state
                    );
                }
                Msg::Resume if connector_state == InstanceState::Paused => {
                    info!("[Connector::{}] Resuming...", &connector_addr.alias);
                    ctx.log_err(connector.on_resume(&ctx).await, "Error during on_resume");
                    connector_state = InstanceState::Running;
                    quiescence_beacon.resume();

                    connector_addr.send_source(SourceMsg::Resume).await?;
                    connector_addr.send_sink(SinkMsg::Resume).await?;

                    if connectivity == Connectivity::Disconnected {
                        info!(
                            "[Connector::{}] Triggering reconnect as part of resume.",
                            &connector_addr.alias
                        );
                        connector_addr.send(Msg::Reconnect).await?;
                    }

                    info!("[Connector::{}] Resumed.", &connector_addr.alias);
                }
                Msg::Resume => {
                    info!(
                        "[Connector::{}] Ignoring Resume Msg. Current state: {:?}",
                        &connector_addr.alias, &connector_state
                    );
                }
                Msg::Drain(_) if connector_state == InstanceState::Draining => {
                    info!(
                        "[Connector::{}] Ignoring Drain Msg. Current state: {:?}",
                        &connector_addr.alias, &connector_state
                    );
                }
                Msg::Drain(tx) => {
                    info!("[Connector::{}] Draining...", &connector_addr.alias);

                    // notify connector that it should stop reading - so no more new events arrive at its source part
                    quiescence_beacon.stop_reading();
                    // let connector stop emitting anything to its source part - if possible here
                    ctx.log_err(connector.on_drain(&ctx).await, "Error during on_drain");
                    connector_state = InstanceState::Draining;

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
                        info!("[Connector::{}] Drained.", &connector_addr.alias);
                        if let Err(e) = d.send_all_drained().await {
                            error!(
                                "[Connector::{}] error signalling being fully drained: {}",
                                &connector_addr.alias, e
                            );
                        }
                    }
                    drainage = Some(d);
                }
                Msg::SourceDrained if connector_state == InstanceState::Draining => {
                    info!(
                        "[Connector::{}] Source-part is drained.",
                        &connector_addr.alias
                    );
                    if let Some(drainage) = drainage.as_mut() {
                        drainage.set_source_drained();
                        if drainage.all_drained() {
                            info!("[Connector::{}] Drained.", &connector_addr.alias);
                            if let Err(e) = drainage.send_all_drained().await {
                                error!(
                                    "[Connector::{}] Error signalling being fully drained: {}",
                                    &connector_addr.alias, e
                                );
                            }
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
                Msg::SinkDrained if connector_state == InstanceState::Draining => {
                    info!(
                        "[Connector::{}] Sink-part is drained.",
                        &connector_addr.alias
                    );
                    if let Some(drainage) = drainage.as_mut() {
                        drainage.set_sink_drained();
                        quiescence_beacon.full_stop(); // TODO: maybe this should be done in the SinkManager?
                        if drainage.all_drained() {
                            info!("[Connector::{}] Drained.", &connector_addr.alias);
                            if let Err(e) = drainage.send_all_drained().await {
                                error!(
                                    "[Connector::{}] Error signalling being fully drained: {}",
                                    &connector_addr.alias, e
                                );
                            }
                        }
                    }
                }
                Msg::SourceDrained => {
                    info!(
                        "[Connector::{}] Ignoring SourceDrained Msg. Current state: {}",
                        &connector_addr.alias, &connector_state
                    );
                }
                Msg::SinkDrained => {
                    info!(
                        "[Connector::{}] Ignoring SourceDrained Msg. Current state: {}",
                        &connector_addr.alias, &connector_state
                    );
                }
                Msg::Stop(sender) => {
                    info!("{} Stopping...", &ctx);
                    ctx.log_err(connector.on_stop(&ctx).await, "Error during on_stop");
                    connector_state = InstanceState::Stopped;
                    quiescence_beacon.full_stop();
                    let (stop_tx, stop_rx) = bounded(2);
                    let mut expect = 0_usize
                        + connector_addr.has_source() as usize
                        + connector_addr.has_sink() as usize;
                    ctx.log_err(
                        connector_addr
                            .send_source(SourceMsg::Stop(stop_tx.clone()))
                            .await,
                        "Error sending Stop msg to Source",
                    );
                    ctx.log_err(
                        connector_addr.send_sink(SinkMsg::Stop(stop_tx)).await,
                        "Error sending Stop msg to Sink",
                    );
                    while expect > 0 {
                        if let Err(e) = stop_rx.recv().await {
                            error!("{} Error in stopping sink and source part: {}", &ctx, e);
                        }
                        expect -= 1;
                    }
                    if let Err(_) = sender.send(ConnectorResult::ok(&ctx)).await {
                        error!("{} Error sending Stop result.", &ctx)
                    }
                    info!("[Connector::{}] Stopped.", &connector_addr.alias);
                    break;
                }
            } // match
        } // while
        info!(
            "[Connector::{}] Connector Stopped. Reason: {:?}",
            &connector_addr.alias, &connector_state
        );
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

/// state of a connector
#[repr(C)]
#[derive(Debug, PartialEq, Serialize, Deserialize, Copy, Clone, StableAbi)]
#[serde(rename_all = "lowercase")]
pub enum ConnectorState {
    /// connector has been initialized, but not yet started
    Initialized,
    /// connector is running
    Running,
    /// connector has been paused
    Paused,
    /// connector was stopped
    Stopped,
    /// Draining - getting rid of in-flight events and avoid emitting new ones
    Draining,
    /// connector failed to start
    Failed,
}

impl Display for ConnectorState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Initialized => "initialized",
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Stopped => "stopped",
            Self::Draining => "draining",
            Self::Failed => "failed",
        })
    }
}

/// connector context
#[repr(C)]
#[derive(Clone, StableAbi)]
pub struct ConnectorContext {
    /// unique identifier
    pub uid: u64,
    /// url of the connector
    pub url: TremorUrl,
    /// type name of the connector
    pub type_name: RString,
    /// The Quiescence Beacon
    pub quiescence_beacon: BoxedQuiescenceBeacon,
    /// Notifier
    pub notifier: reconnect::BoxedConnectionLostNotifier,
}

impl ConnectorContext {
    /// enclose the given meta in the right connector namespace
    ///
    /// Namespace: "connector.<connector-type>"
    #[must_use]
    pub fn meta(&self, inner: Value<'static>) -> Value<'static> {
        let mut map = Value::object_with_capacity(1);
        let mut type_map = Value::object_with_capacity(1);
        let type_name: String = self.type_name.clone().into();
        type_map.try_insert(type_name, inner);
        map.try_insert("connector", type_map);
        map
    }
}

/// describes connectivity state of the connector
#[derive(Debug, Serialize, Copy, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Connectivity {
    /// connector is connected
    Connected,
    /// connector is disconnected
    Disconnected,
}

const IN_PORTS: [&str; 1] = [IN];
const IN_PORTS_REF: &'static [&str; 1] = &IN_PORTS;
const OUT_PORTS: [&str; 2] = [OUT, ERR];
const OUT_PORTS_REF: &'static [&str; 2] = &OUT_PORTS;

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
///
/// This trait specifically is the low-level connector interface used for the
/// plugin system. The `Connector` type wraps it up for ease of use.
#[abi_stable::sabi_trait]
pub trait RawConnector: Send {
    /// Valid input ports for the connector, by default this is `in`
    fn input_ports(&self) -> RVec<RCow<'static, str>> {
        // FIXME: make static? `RCow` can't be const, I think.
        IN_PORTS_REF.into_iter().map(|s| RCow::from(*s)).collect()
    }
    /// Valid output ports for the connector, by default this is `out` and `err`
    fn output_ports(&self) -> RVec<RCow<'static, str>> {
        // FIXME: make static? `RCow` can't be const, I think.
        OUT_PORTS_REF.into_iter().map(|s| RCow::from(*s)).collect()
    }

    /// Tests if a input port is valid, by default does a case insensitive search against
    /// `self.input_ports()`
    fn is_valid_input_port(&self, port: RStr<'_>) -> bool {
        for valid in self.input_ports() {
            if port.eq_ignore_ascii_case(valid.as_ref()) {
                return true;
            }
        }
        false
    }

    /// Tests if a input port is valid, by default does a case insensitive search against
    /// `self.output_ports()`
    fn is_valid_output_port(&self, port: RStr<'_>) -> bool {
        for valid in self.output_ports() {
            if port.eq_ignore_ascii_case(valid.as_ref()) {
                return true;
            }
        }
        false
    }

    /// This connector works with structured data and does not allow the use
    /// of codecs.
    fn is_structured(&self) -> bool {
        false
    }

    /// create a source part for this connector if applicable
    ///
    /// This function is called exactly once upon connector creation.
    /// If this connector does not act as a source, return `Ok(None)`.
    /* async */
    fn create_source(
        &mut self,
        _source_context: SourceContext,
    ) -> BorrowingFfiFuture<'_, RResult<ROption<BoxedRawSource>>> {
        future::ready(ROk(RNone)).into_ffi()
    }

    /// Create a sink part for this connector if applicable
    ///
    /// This function is called exactly once upon connector creation.
    /// If this connector does not act as a sink, return `Ok(None)`.
    fn create_sink(
        &mut self,
        _sink_context: SinkContext,
    ) -> BorrowingFfiFuture<'_, RResult<ROption<BoxedRawSink>>> {
        future::ready(ROk(RNone)).into_ffi()
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
    fn connect(
        &mut self,
        ctx: &ConnectorContext,
        attempt: &Attempt,
    ) -> BorrowingFfiFuture<'_, RResult<bool>>;

    /// called once when the connector is started
    /// `connect` will be called after this for the first time, leave connection attempts in `connect`.
    fn on_start(&mut self, _ctx: &ConnectorContext) -> BorrowingFfiFuture<'_, RResult<()>> {
        future::ready(ROk(())).into_ffi()
    }

    /// called when the connector pauses
    fn on_pause(&mut self, _ctx: &ConnectorContext) -> BorrowingFfiFuture<'_, RResult<()>> {
        future::ready(ROk(())).into_ffi()
    }
    /// called when the connector resumes
    fn on_resume(&mut self, _ctx: &ConnectorContext) -> BorrowingFfiFuture<'_, RResult<()>> {
        future::ready(ROk(())).into_ffi()
    }

    /// Drain
    ///
    /// Ensure no new events arrive at the source part of this connector when this function returns
    /// So we can safely send the `Drain` signal.
    fn on_drain(&mut self, _ctx: &ConnectorContext) -> BorrowingFfiFuture<'_, RResult<()>> {
        future::ready(ROk(())).into_ffi()
    }

    /// called when the connector is stopped
    fn on_stop(&mut self, _ctx: &ConnectorContext) -> BorrowingFfiFuture<'_, RResult<()>> {
        future::ready(ROk(())).into_ffi()
    }

    /// returns the default codec for this connector
    fn default_codec(&self) -> RStr<'_>;
}
/// Alias for the FFI-safe dynamic connector type
pub type BoxedRawConnector = RawConnector_TO<'static, RBox<()>>;

// The higher level connector interface, which wraps the raw connector from the
// plugin.
pub struct Connector(pub BoxedRawConnector);
impl Connector {
    #[inline]
    fn input_ports(&self) -> Vec<Cow<'static, str>> {
        self.0.input_ports().into_iter().map(conv_cow_str).collect()
    }
    #[inline]
    fn output_ports(&self) -> Vec<Cow<'static, str>> {
        self.0
            .output_ports()
            .into_iter()
            .map(conv_cow_str)
            .collect()
    }

    #[inline]
    fn is_valid_input_port(&self, port: &str) -> bool {
        self.0.is_valid_input_port(port.into())
    }

    #[inline]
    fn is_valid_output_port(&self, port: &str) -> bool {
        self.0.is_valid_output_port(port.into())
    }

    #[inline]
    fn is_structured(&self) -> bool {
        self.0.is_structured()
    }

    #[inline]
    pub async fn create_source(
        &mut self,
        source_context: SourceContext,
        builder: source::SourceManagerBuilder,
    ) -> Result<Option<source::SourceAddr>> {
        match self.0.create_source(source_context.clone()) {
            ROk(RSome(raw_source)) => {
                let wrapper = Source(raw_source);
                builder.spawn(wrapper, source_context).map(Some)
            }
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
        match self.0.create_sink(sink_context.clone()) {
            ROk(RSome(raw_sink)) => {
                let wrapper = Sink(raw_sink);
                builder.spawn(wrapper, sink_context).map(Some)
            }
            ROk(RNone) => Ok(None),
            RErr(err) => Err(err.into()),
        }
    }

    #[inline]
    pub async fn connect(&mut self, ctx: &ConnectorContext, attempt: &Attempt) -> Result<bool> {
        self.0
            .connect(ctx, attempt)
            .map_err(Into::into) // RBoxError -> Box<dyn Error>
            .into() // RResult -> Result
    }

    #[inline]
    pub async fn on_start(&mut self, ctx: &ConnectorContext) -> Result<()> {
        self.0
            .on_start(ctx)
            .map_err(Into::into) // RBoxError -> Box<dyn Error>
            .into() // RResult -> Result
    }

    #[inline]
    pub async fn on_pause(&mut self, ctx: &ConnectorContext) -> Result<()> {
        self.0
            .on_pause(ctx)
            .map_err(Into::into) // RBoxError -> Box<dyn Error>
            .into() // RResult -> Result
    }

    #[inline]
    pub async fn on_resume(&mut self, ctx: &ConnectorContext) -> Result<()> {
        self.0
            .on_resume(ctx)
            .map_err(Into::into) // RBoxError -> Box<dyn Error>
            .into() // RResult -> Result
    }

    #[inline]
    pub async fn on_drain(&mut self, ctx: &ConnectorContext) -> Result<()> {
        self.0
            .on_drain(ctx)
            .map_err(Into::into) // RBoxError -> Box<dyn Error>
            .into() // RResult -> Result
    }

    #[inline]
    pub async fn on_stop(&mut self, ctx: &ConnectorContext) -> Result<()> {
        self.0
            .on_stop(ctx)
            .map_err(Into::into) // RBoxError -> Box<dyn Error>
            .into() // RResult -> Result
    }

    #[inline]
    pub fn default_codec(&self) -> &str {
        self.0.default_codec().into()
    }
}

/// the type of a connector
#[repr(C)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, StableAbi)]
pub struct ConnectorType(RString);

impl From<ConnectorType> for RString {
    fn from(ct: ConnectorType) -> Self {
        ct.0
    }
}

impl From<ConnectorType> for String {
    fn from(ct: ConnectorType) -> Self {
        ct.0.into()
    }
}

impl Display for ConnectorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.as_str())
    }
}

impl From<RString> for ConnectorType {
    fn from(s: RString) -> Self {
        Self(s)
    }
}

impl From<String> for ConnectorType {
    fn from(s: String) -> Self {
        Self(s.into())
    }
}

impl<T> From<&T> for ConnectorType
where
    T: ToString + ?Sized,
{
    fn from(s: &T) -> Self {
        Self(s.to_string().into())
    }
}

/// something that is able to create a connector instance
#[async_trait::async_trait]
pub trait ConnectorBuilder: Sync + Send + std::fmt::Debug {
    /// the type of the connector
    fn connector_type(&self) -> ConnectorType;

    /// create a connector from the given `id` and `config`
    ///
    /// # Errors
    ///  * If the config is invalid for the connector
    async fn from_config(
        &self,
        alias: &str,
        config: &Option<OpConfig>,
    ) -> Result<Box<Connector>>;
}

/// builtin connector types
#[cfg(not(tarpaulin_include))]
pub fn builtin_connector_types() -> Vec<Box<dyn ConnectorBuilder + 'static>> {
    vec![
        Box::new(impls::file::Builder::default()),
        Box::new(impls::metrics::Builder::default()),
        Box::new(impls::stdio::Builder::default()),
        Box::new(impls::tcp::client::Builder::default()),
        Box::new(impls::tcp::server::Builder::default()),
        Box::new(impls::udp::client::Builder::default()),
        Box::new(impls::udp::server::Builder::default()),
        Box::new(impls::kv::Builder::default()),
        Box::new(impls::metronome::Builder::default()),
        Box::new(impls::wal::Builder::default()),
        Box::new(impls::dns::client::Builder::default()),
        Box::new(impls::discord::Builder::default()),
        Box::new(impls::ws::client::Builder::default()),
        Box::new(impls::ws::server::Builder::default()),
        Box::new(impls::elastic::Builder::default()),
        Box::new(impls::crononome::Builder::default()),
        Box::new(impls::s3::Builder::default()),
        Box::new(impls::kafka::consumer::Builder::default()),
    ]
}

/// debug connector types
#[cfg(not(tarpaulin_include))]
pub fn debug_connector_types() -> Vec<Box<dyn ConnectorBuilder + 'static>> {
    vec![
        Box::new(impls::cb::Builder::default()),
        Box::new(impls::bench::Builder::default()),
    ]
}

/// registering builtin and debug connector types
///
/// # Errors
///  * If a builtin connector couldn't be registered
#[cfg(not(tarpaulin_include))]
pub async fn register_builtin_connector_types(world: &World) -> Result<()> {
    // First we load the hardcoded connectors
    // world
    //     .register_builtin_connector_type(ConnectorMod_Ref::load_from_file(&Path::new(path))?)
    //     .await?;

    // Finally, we try to find all the available plugins and we load them
    // dynamically. For now, plugins are loaded from the path defined by
    // `TREMOR_PLUGIN_PATH`.
    //
    // FIXME: the `plugins` fallback is only for development, this should have a
    // proper default value.
    let base_dir = env::var("TREMOR_PLUGIN_PATH").unwrap_or_else(|_| String::from("plugins"));
    let dynamic_plugins = pdk::find_recursively(&base_dir);
    for plugin in dynamic_plugins {
        world.register_connector_type(plugin).await?;
    }

    Ok(())
}
