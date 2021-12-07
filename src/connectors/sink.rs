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

#![allow(clippy::module_name_repetitions)]

/// Providing a `Sink` implementation for connectors handling multiple Streams
pub mod channel_sink;

/// Providing a `Sink` implementation for connectors handling only a single Stream
pub mod single_stream_sink;

/// Utility for limiting concurrency (by sending CB::Close messages when a maximum concurrency value is reached)
pub(crate) mod concurrency_cap;

use crate::codec::{self, Codec};
use crate::config::{
    Codec as CodecConfig, Connector as ConnectorConfig, Postprocessor as PostprocessorConfig,
};
use crate::permge::PriorityMerge;
use crate::pipeline;
use crate::postprocessor::{make_postprocessors, postprocess, Postprocessors};
use async_std::channel::{bounded, unbounded, Receiver, Sender};
use async_std::stream::StreamExt; // for .next() on PriorityMerge
use async_std::task;
use beef::Cow;
pub use channel_sink::{ChannelSink, ChannelSinkRuntime};
pub use single_stream_sink::{SingleStreamSink, SingleStreamSinkRuntime};
use std::borrow::Borrow;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashSet};
use std::fmt::Display;
use tremor_common::time::nanotime;
use tremor_common::url::{ports::IN, TremorUrl};
use tremor_pipeline::{CbAction, Event, EventId, OpMeta, SignalKind, DEFAULT_STREAM_ID};
use tremor_script::EventPayload;

use tremor_value::Value;

use crate::connectors::utils::reconnect::Attempt;
use crate::connectors::utils::{
    quiescence::BoxedQuiescenceBeacon, reconnect::BoxedConnectionLostNotifier,
};
use crate::connectors::{ConnectorType, Context, Msg, StreamDone};
use crate::errors::{Error, Result};
use crate::pdk::{RError, RResult};
use abi_stable::{
    rvec,
    std_types::{RBox, RResult::ROk, RStr, RVec, SendRBoxError},
    type_level::downcasting::TD_Opaque,
    RMut, StableAbi,
};
use async_ffi::{BorrowingFfiFuture, FutureExt};
use std::future;
use tremor_pipeline::{pdk::PdkEvent, pdk::PdkOpMeta};
use tremor_script::pdk::PdkEventPayload;
use tremor_value::pdk::PdkValue;

pub use self::channel_sink::SinkMeta;

use super::utils::metrics::SinkReporter;

/// Result for a sink function that may provide insights or response.
///
///
/// An insight is a contraflowevent containing control information for the runtime like
/// circuit breaker events, guaranteed delivery events, etc.
///
/// A response is an event generated from the sink delivery.
#[repr(C)]
#[derive(Clone, Debug, Default, Copy, StableAbi)]
pub struct SinkReply {
    /// guaranteed delivery response - did we sent the event successfully `SinkAck::Ack` or did it fail `SinkAck::Fail`
    pub ack: SinkAck,
    /// circuit breaker action
    pub cb: CbAction,
}

impl SinkReply {
    /// Acknowledges
    pub const ACK: SinkReply = SinkReply {
        ack: SinkAck::Ack,
        cb: CbAction::None,
    };
    /// Fails
    pub const FAIL: SinkReply = SinkReply {
        ack: SinkAck::Fail,
        cb: CbAction::None,
    };
    /// None
    pub const NONE: SinkReply = SinkReply {
        ack: SinkAck::None,
        cb: CbAction::None,
    };

    /// Acknowledges
    pub fn ack() -> Self {
        SinkReply {
            ack: SinkAck::Ack,
            ..SinkReply::default()
        }
    }
    /// Fails
    pub fn fail() -> Self {
        SinkReply {
            ack: SinkAck::Fail,
            ..SinkReply::default()
        }
    }
}
impl From<bool> for SinkReply {
    fn from(ok: bool) -> Self {
        SinkReply {
            ack: SinkAck::from(ok),
            ..SinkReply::default()
        }
    }
}

/// stuff a sink replies back upon an event or a signal
/// to the calling sink/connector manager
#[repr(C)]
#[derive(Clone, Debug, Copy, StableAbi)]
pub enum SinkAck {
    /// no reply - maybe no reply yet, maybe replies come asynchronously...
    None,
    /// everything went smoothly, chill
    Ack,
    /// shit hit the fan, but only for this event, nothing big
    Fail,
}

impl Default for SinkAck {
    fn default() -> Self {
        Self::None
    }
}

impl From<bool> for SinkAck {
    fn from(ok: bool) -> Self {
        if ok {
            Self::Ack
        } else {
            Self::Fail
        }
    }
}

/// Possible replies from asynchronous sinks via `reply_channel` from event or signal handling
#[repr(C)]
#[derive(StableAbi)]
pub enum AsyncSinkReply {
    /// success
    Ack(ContraflowData, u64),
    /// failure
    Fail(ContraflowData),
    /// circuitbreaker shit
    CB(ContraflowData, CbAction),
}

/// Alias for the FFI-safe dynamic sink type
pub type BoxedRawSink = RawSink_TO<'static, RBox<()>>;

/// connector sink - receiving events
#[abi_stable::sabi_trait]
pub trait RawSink: Send {
    /// called when receiving an event
    fn on_event<'a>(
        &'a mut self,
        input: RStr<'a>,
        event: PdkEvent,
        ctx: &'a SinkContext,
        serializer: &'a mut MutEventSerializer,
        start: u64,
    ) -> BorrowingFfiFuture<'a, RResult<SinkReply>>;
    /// called when receiving a signal
    fn on_signal<'a>(
        &'a mut self,
        _signal: PdkEvent,
        _ctx: &'a SinkContext,
        _serializer: &'a mut MutEventSerializer,
    ) -> BorrowingFfiFuture<'a, RResult<SinkReply>> {
        future::ready(ROk(SinkReply::default())).into_ffi()
    }

    /// Pull metrics from the sink
    fn metrics(&mut self, _timestamp: u64) -> RVec<PdkEventPayload> {
        rvec![]
    }

    // lifecycle stuff
    /// called when started
    fn on_start(&mut self, _ctx: &SinkContext) -> BorrowingFfiFuture<'_, RResult<()>> {
        future::ready(ROk(())).into_ffi()
    }

    /// Connect to the external thingy.
    /// This function is called definitely after `on_start` has been called.
    ///
    /// This function might be called multiple times, check the `attempt` where you are at.
    /// The intended result of this function is to re-establish a connection. It might reuse a working connection.
    ///
    /// Return `Ok(true)` if the connection could be successfully established.
    fn connect(
        &mut self,
        _ctx: &SinkContext,
        _attempt: &Attempt,
    ) -> BorrowingFfiFuture<'_, RResult<bool>> {
        future::ready(ROk(true)).into_ffi()
    }

    /// called when paused
    fn on_pause(&mut self, _ctx: &SinkContext) -> BorrowingFfiFuture<'_, RResult<()>> {
        future::ready(ROk(())).into_ffi()
    }
    /// called when resumed
    fn on_resume(&mut self, _ctx: &SinkContext) -> BorrowingFfiFuture<'_, RResult<()>> {
        future::ready(ROk(())).into_ffi()
    }
    /// called when stopped
    fn on_stop(&mut self, _ctx: &SinkContext) -> BorrowingFfiFuture<'_, RResult<()>> {
        future::ready(ROk(())).into_ffi()
    }

    // connectivity stuff
    /// called when sink lost connectivity
    fn on_connection_lost(&mut self, _ctx: &SinkContext) -> BorrowingFfiFuture<'_, RResult<()>> {
        future::ready(ROk(())).into_ffi()
    }
    /// called when sink re-established connectivity
    fn on_connection_established(
        &mut self,
        _ctx: &SinkContext,
    ) -> BorrowingFfiFuture<'_, RResult<()>> {
        future::ready(ROk(())).into_ffi()
    }

    /// if `true` events are acknowledged/failed automatically by the sink manager.
    /// Such sinks should return SinkReply::None from on_event or SinkReply::Fail if they fail immediately.
    ///
    /// if `false` events need to be acked/failed manually by the sink impl
    fn auto_ack(&self) -> bool;

    /// if true events are sent asynchronously, not necessarily when `on_event` returns.
    /// if false events can be considered delivered once `on_event` returns.
    fn asynchronous(&self) -> bool {
        false
    }
}

/// Sink part of a connector.
///
/// Just like `Connector`, this wraps the FFI dynamic sink with `abi_stable`
/// types so that it's easier to use with `std`.
pub struct Sink(pub BoxedRawSink);
impl Sink {
    /// Wrapper for [`BoxedRawSink::on_event`]
    #[inline]
    pub async fn on_event(
        &mut self,
        input: &str,
        event: Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
        start: u64,
    ) -> Result<SinkReply> {
        let mut serializer = MutEventSerializer::from_ptr(serializer, TD_Opaque);
        self.0
            .on_event(input.into(), event.into(), ctx, &mut serializer, start)
            .await
            .map_err(Into::into) // RBoxError -> Box<dyn Error>
            .into() // RResult -> Result
    }
    /// Wrapper for [`BoxedRawSink::on_signal`]
    #[inline]
    pub async fn on_signal(
        &mut self,
        signal: Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
    ) -> Result<SinkReply> {
        let mut serializer = MutEventSerializer::from_ptr(serializer, TD_Opaque);
        self.0
            .on_signal(signal.into(), ctx, &mut serializer)
            .await
            .map_err(Into::into) // RBoxError -> Box<dyn Error>
            .into() // RResult -> Result
    }

    /// Wrapper for [`BoxedRawSink::metrics`]
    #[inline]
    pub fn metrics(&mut self, timestamp: u64) -> Vec<EventPayload> {
        self.0
            .metrics(timestamp)
            .into_iter()
            .map(Into::into)
            .collect()
    }

    /// Wrapper for [`BoxedRawSink::on_start`]
    #[inline]
    pub async fn on_start(&mut self, ctx: &SinkContext) -> Result<()> {
        self.0.on_start(ctx).await.map_err(Into::into).into()
    }

    /// Wrapper for [`BoxedRawSink::connect`]
    #[inline]
    pub async fn connect(&mut self, ctx: &SinkContext, attempt: &Attempt) -> Result<bool> {
        self.0
            .connect(ctx, attempt)
            .await
            .map_err(Into::into)
            .into()
    }

    /// Wrapper for [`BoxedRawSink::on_pause`]
    #[inline]
    pub async fn on_pause(&mut self, ctx: &SinkContext) -> Result<()> {
        self.0
            .on_pause(ctx)
            .await
            .map_err(Into::into) // RBoxError -> Box<dyn Error>
            .into() // RResult -> Result
    }
    /// Wrapper for [`BoxedRawSink::on_resume`]
    #[inline]
    pub async fn on_resume(&mut self, ctx: &SinkContext) -> Result<()> {
        self.0
            .on_resume(ctx)
            .await
            .map_err(Into::into) // RBoxError -> Box<dyn Error>
            .into() // RResult -> Result
    }
    /// Wrapper for [`BoxedRawSink::on_stop`]
    #[inline]
    pub async fn on_stop(&mut self, ctx: &SinkContext) -> Result<()> {
        self.0
            .on_stop(ctx)
            .await
            .map_err(Into::into) // RBoxError -> Box<dyn Error>
            .into() // RResult -> Result
    }

    /// Wrapper for [`BoxedRawSink::on_connection_lost`]
    #[inline]
    pub async fn on_connection_lost(&mut self, ctx: &SinkContext) -> Result<()> {
        self.0
            .on_connection_lost(ctx)
            .await
            .map_err(Into::into) // RBoxError -> Box<dyn Error>
            .into() // RResult -> Result
    }
    /// Wrapper for [`BoxedRawSink::on_connection_established`]
    #[inline]
    pub async fn on_connection_established(&mut self, ctx: &SinkContext) -> Result<()> {
        self.0
            .on_connection_established(ctx)
            .await
            .map_err(Into::into) // RBoxError -> Box<dyn Error>
            .into() // RResult -> Result
    }

    /// Wrapper for [`BoxedRawSink::auto_ack`]
    #[inline]
    pub fn auto_ack(&self) -> bool {
        self.0.auto_ack()
    }

    /// Wrapper for [`BoxedRawSink::asynchronous`]
    #[inline]
    pub fn asynchronous(&self) -> bool {
        self.0.asynchronous()
    }
}

/// handles writing to 1 stream (e.g. file or TCP connection)
#[async_trait::async_trait]
pub trait StreamWriter: Send + Sync {
    /// write the given data out to the stream
    async fn write(&mut self, data: Vec<Vec<u8>>, meta: Option<SinkMeta>) -> Result<()>;
    /// handle the stream being done, by error or
    async fn on_done(&mut self, _stream: u64) -> Result<StreamDone> {
        Ok(StreamDone::StreamClosed)
    }
}
/// context for the connector sink
#[repr(C)]
#[derive(Clone, StableAbi)]
pub struct SinkContext {
    /// the connector unique identifier
    pub uid: u64,
    /// the connector url
    pub(crate) url: TremorUrl,
    /// the connector type
    pub(crate) connector_type: ConnectorType,

    /// check if we are paused or should stop reading/writing
    pub(crate) quiescence_beacon: BoxedQuiescenceBeacon,

    /// notifier the connector runtime if we lost a connection
    pub(crate) notifier: BoxedConnectionLostNotifier,
}

impl Display for SinkContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[Sink::{}]", &self.url)
    }
}

impl Context for SinkContext {
    fn url(&self) -> &TremorUrl {
        &self.url
    }

    fn quiescence_beacon(&self) -> &BoxedQuiescenceBeacon {
        &self.quiescence_beacon
    }

    fn notifier(&self) -> &BoxedConnectionLostNotifier {
        &self.notifier
    }

    fn connector_type(&self) -> &ConnectorType {
        &self.connector_type
    }
}

/// messages a sink can receive
pub enum SinkMsg {
    /// receive an event to handle
    Event {
        /// the event
        event: Event,
        /// the port through which it came
        port: Cow<'static, str>,
    },
    /// receive a signal
    Signal {
        /// the signal event
        signal: Event,
    },
    /// link some pipelines to the give port
    Link {
        /// the port
        port: Cow<'static, str>,
        /// the pipelines
        pipelines: Vec<(TremorUrl, pipeline::Addr)>,
    },
    /// unlink a pipeline
    Unlink {
        /// url of the pipeline
        id: TremorUrl,
        /// the port
        port: Cow<'static, str>,
    },
    /// Connect to the outside world and send the result back
    Connect(Sender<Result<bool>>, Attempt),
    /// the connection to the outside world wasl ost
    ConnectionLost,
    /// connection established
    ConnectionEstablished,
    // TODO: fill those
    /// start the sink
    Start,
    /// pause the sink
    Pause,
    /// resume the sink
    Resume,
    /// stop the sink
    Stop(Sender<Result<()>>),
    /// drain this sink and notify the connector via the provided sender
    Drain(Sender<Msg>),
}

/// Wrapper around all possible sink messages
/// handled in the Sink task
#[allow(clippy::large_enum_variant)] // TODO: should we box SinkMsg here?
enum SinkMsgWrapper {
    FromSink(AsyncSinkReply),
    ToSink(SinkMsg),
}

/// address of a connector sink
#[derive(Clone, Debug)]
pub struct SinkAddr {
    /// the actual sender
    pub addr: Sender<SinkMsg>,
}
impl SinkAddr {
    /// send a message
    ///
    /// # Errors
    ///  * If sending failed
    pub async fn send(&self, msg: SinkMsg) -> Result<()> {
        Ok(self.addr.send(msg).await?)
    }
}

/// Builder for the sink manager
pub struct SinkManagerBuilder {
    qsize: usize,
    serializer: EventSerializer,
    reply_channel: (Sender<AsyncSinkReply>, Receiver<AsyncSinkReply>),
    metrics_reporter: SinkReporter,
}

impl SinkManagerBuilder {
    /// globally configured queue size
    pub fn qsize(&self) -> usize {
        self.qsize
    }

    /// Get yourself a sender to send replies back from your concrete sink.
    ///
    /// This is especially useful if your sink handles events asynchronously
    /// and you can't reply immediately.
    pub fn reply_tx(&self) -> Sender<AsyncSinkReply> {
        self.reply_channel.0.clone()
    }

    /// spawn your specific sink
    pub fn spawn(self, sink: Sink, ctx: SinkContext) -> Result<SinkAddr> {
        let qsize = self.qsize;
        let name = ctx.url.short_id("c-sink"); // connector sink
        let (sink_tx, sink_rx) = bounded(qsize);
        let manager = SinkManager::new(sink, ctx, self, sink_rx);
        // spawn manager task
        task::Builder::new().name(name).spawn(manager.run())?;

        Ok(SinkAddr { addr: sink_tx })
    }
}

#[abi_stable::sabi_trait]
pub trait ContraflowSenderOpaque: Send {
    /// Send a contraflow message to the runtime
    fn send(&self, reply: AsyncSinkReply) -> BorrowingFfiFuture<'_, RResult<()>>;
}
impl ContraflowSenderOpaque for Sender<AsyncSinkReply> {
    fn send(&self, reply: AsyncSinkReply) -> BorrowingFfiFuture<'_, RResult<()>> {
        async move {
            self.send(reply)
                .await
                .map_err(|e| RError::new(Error::from(e)))
                .into()
        }
        .into_ffi()
    }
}
/// Alias for the FFI-safe dynamic connector type
pub type BoxedContraflowSender = ContraflowSenderOpaque_TO<'static, RBox<()>>;

/// create a builder for a `SinkManager`.
/// with the generic information available in the connector
/// the builder then in a second step takes the source specific information to assemble and spawn the actual `SinkManager`.
pub fn builder(
    config: &ConnectorConfig,
    connector_default_codec: &str,
    qsize: usize,
    metrics_reporter: SinkReporter,
) -> Result<SinkManagerBuilder> {
    // resolve codec and processors
    let postprocessor_configs = config.postprocessors.clone().unwrap_or_default();
    let serializer = EventSerializer::build(
        config.codec.clone(),
        connector_default_codec,
        postprocessor_configs,
    )?;
    // the incoming channels for events are all bounded, so we can safely be unbounded here
    // TODO: actually we could have lots of CB events not bound to events here
    let reply_channel = unbounded();
    Ok(SinkManagerBuilder {
        qsize,
        serializer,
        reply_channel,
        metrics_reporter,
    })
}

/// Helper for serializing events within sinks
///
/// Keeps track of codec/postprocessors for seach stream
/// Attention: Take care to clear out data for streams that are not used
pub struct EventSerializer {
    // default stream handling
    codec: Box<dyn Codec>,
    postprocessors: Postprocessors,
    // creation templates for stream handling
    codec_config: CodecConfig,
    postprocessor_configs: Vec<PostprocessorConfig>,
    // stream data
    // TODO: clear out state from codec, postprocessors and enable reuse
    streams: BTreeMap<u64, (Box<dyn Codec>, Postprocessors)>,
}

impl EventSerializer {
    fn build(
        codec_config: Option<CodecConfig>,
        default_codec: &str,
        postprocessor_configs: Vec<PostprocessorConfig>,
    ) -> Result<Self> {
        let codec_config = codec_config.unwrap_or_else(|| CodecConfig::from(default_codec));
        let codec = codec::resolve(&codec_config)?;
        let postprocessors = make_postprocessors(postprocessor_configs.as_slice())?;
        Ok(Self {
            codec,
            postprocessors,
            codec_config,
            postprocessor_configs,
            streams: BTreeMap::new(),
        })
    }

    // This is kept separate so that conversions are done only once later on
    // FIXME: use a `try` block when they are stabilized
    fn serialize_for_stream_inner(
        &mut self,
        value: &PdkValue,
        ingest_ns: u64,
        stream_id: u64,
    ) -> Result<Vec<Vec<u8>>> {
        // FIXME: super ugly clone here
        let value: &Value = &value.clone().into();
        if stream_id == DEFAULT_STREAM_ID {
            postprocess(
                &mut self.postprocessors,
                ingest_ns,
                self.codec.encode(value)?,
            )
        } else {
            match self.streams.entry(stream_id) {
                Entry::Occupied(mut entry) => {
                    let (c, pps) = entry.get_mut();
                    postprocess(pps, ingest_ns, c.encode(value)?)
                }
                Entry::Vacant(entry) => {
                    let codec = codec::resolve(&self.codec_config)?;
                    let pps = make_postprocessors(self.postprocessor_configs.as_slice())?;
                    // insert data for a new stream
                    let (c, pps2) = entry.insert((codec, pps));
                    postprocess(pps2, ingest_ns, c.encode(value)?)
                }
            }
        }
    }
}

/// Note that since `EventSerializer` is used for the plugin system, it
/// must be `#[repr(C)]` in order to interact with it. However, since it uses
/// complex types and it's easier to just make it available as an opaque type
/// instead, with the help of `sabi_trait`.
#[abi_stable::sabi_trait]
pub trait EventSerializerOpaque: Send {
    /// drop a stream
    fn drop_stream(&mut self, stream_id: u64);

    /// clear out all streams - this can lead to data loss
    /// only use when you are sure, all the streams are gone
    fn clear(&mut self);

    /// serialize event for the default stream
    ///
    /// # Errors
    ///   * if serialization failed (codec or postprocessors)
    fn serialize(&mut self, value: &PdkValue, ingest_ns: u64) -> RResult<RVec<RVec<u8>>>;

    /// serialize event for a certain stream
    ///
    /// # Errors
    ///   * if serialization failed (codec or postprocessors)
    fn serialize_for_stream(
        &mut self,
        value: &PdkValue,
        ingest_ns: u64,
        stream_id: u64,
    ) -> RResult<RVec<RVec<u8>>>;
}
impl EventSerializerOpaque for EventSerializer {
    fn drop_stream(&mut self, stream_id: u64) {
        self.streams.remove(&stream_id);
    }

    fn clear(&mut self) {
        self.streams.clear();
    }

    fn serialize(&mut self, value: &PdkValue, ingest_ns: u64) -> RResult<RVec<RVec<u8>>> {
        self.serialize_for_stream(value.into(), ingest_ns, DEFAULT_STREAM_ID)
    }

    fn serialize_for_stream(
        &mut self,
        value: &PdkValue,
        ingest_ns: u64,
        stream_id: u64,
    ) -> RResult<RVec<RVec<u8>>> {
        self.serialize_for_stream_inner(value, ingest_ns, stream_id)
            .map(|v1| v1.into_iter().map(|v2| v2.into()).collect()) // RVec<RVec<T>> -> Vec<Vec<T>>
            .map_err(|e| SendRBoxError::new(e)) // RBoxError -> Error
            .into() // RResult -> Result
    }
}
/// Alias for the type used in FFI, as a box
pub type BoxedEventSerializer = EventSerializerOpaque_TO<'static, RBox<()>>;
/// Alias for the type used in FFI, as a mutable reference
pub type MutEventSerializer<'a> = EventSerializerOpaque_TO<'static, RMut<'a, ()>>;

#[derive(Debug, PartialEq)]
enum SinkState {
    Initialized,
    Running,
    Paused,
    Draining,
    Drained,
    Stopped,
}

pub(crate) struct SinkManager {
    sink: Sink,
    ctx: SinkContext,
    rx: Receiver<SinkMsg>,
    reply_rx: Receiver<AsyncSinkReply>,
    serializer: EventSerializer,
    metrics_reporter: SinkReporter,
    /// tracking which operators incoming events visited
    merged_operator_meta: OpMeta,
    // pipelines connected to IN port
    pipelines: Vec<(TremorUrl, pipeline::Addr)>,
    // set of connector ids we received start signals from
    starts_received: HashSet<u64>,
    // set of connector ids we received drain signals from
    drains_received: HashSet<u64>, // TODO: use a bitset for both?
    drain_channel: Option<Sender<Msg>>,
    state: SinkState,
}

impl SinkManager {
    fn new(
        sink: Sink,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
        rx: Receiver<SinkMsg>,
    ) -> Self {
        let SinkManagerBuilder {
            serializer,
            reply_channel,
            metrics_reporter,
            ..
        } = builder;
        Self {
            sink,
            ctx,
            rx,
            reply_rx: reply_channel.1,
            serializer,
            metrics_reporter,
            merged_operator_meta: OpMeta::default(),
            pipelines: Vec::with_capacity(1), // by default 1 connected to "in" port
            starts_received: HashSet::new(),
            drains_received: HashSet::new(),
            drain_channel: None,
            state: SinkState::Initialized,
        }
    }
    #[allow(clippy::too_many_lines)]
    async fn run(mut self) -> Result<()> {
        use SinkState::{Drained, Draining, Initialized, Paused, Running, Stopped};
        let from_sink = self.reply_rx.map(SinkMsgWrapper::FromSink);
        let to_sink = self.rx.map(SinkMsgWrapper::ToSink);
        let mut from_and_to_sink_channel = PriorityMerge::new(from_sink, to_sink);

        while let Some(msg_wrapper) = from_and_to_sink_channel.next().await {
            match msg_wrapper {
                SinkMsgWrapper::ToSink(sink_msg) => {
                    match sink_msg {
                        SinkMsg::Link {
                            port,
                            mut pipelines,
                        } => {
                            debug_assert!(
                                port == IN,
                                "[Sink::{}] connected to invalid connector sink port",
                                &self.ctx.url
                            );
                            self.pipelines.append(&mut pipelines);
                        }
                        SinkMsg::Unlink { id, port } => {
                            debug_assert!(
                                port == IN,
                                "[Sink::{}] disconnected from invalid connector sink port",
                                &self.ctx.url
                            );
                            self.pipelines.retain(|(url, _)| url != &id);
                        }
                        // FIXME: only handle those if in the right state (see source part)
                        SinkMsg::Start if self.state == Initialized => {
                            self.state = Running;
                            self.ctx.log_err(
                                self.sink.on_start(&self.ctx).await,
                                "Error during on_start",
                            );
                        }
                        SinkMsg::Start => {
                            info!(
                                "{} Ignoring Start message in {:?} state",
                                &self.ctx, &self.state
                            );
                        }
                        SinkMsg::Connect(sender, attempt) => {
                            info!("{} Connecting...", &self.ctx);
                            let connect_result = self.sink.connect(&self.ctx, &attempt).await;
                            if let Ok(true) = connect_result {
                                info!("{} Sink connected.", &self.ctx);
                            }
                            self.ctx.log_err(
                                sender.send(connect_result).await,
                                "Error sending sink connect result",
                            );
                        }
                        SinkMsg::Resume if self.state == Paused => {
                            self.state = Running;
                            self.ctx.log_err(
                                self.sink.on_resume(&self.ctx).await,
                                "Error during on_resume",
                            );
                        }
                        SinkMsg::Resume => {
                            info!(
                                "[Sink::{}] Ignoring Resume message in {:?} state",
                                &self.ctx.url, &self.state
                            );
                        }
                        SinkMsg::Pause if self.state == Running => {
                            self.state = Paused;
                            self.ctx.log_err(
                                self.sink.on_pause(&self.ctx).await,
                                "Error during on_pause",
                            );
                        }
                        SinkMsg::Pause => {
                            info!(
                                "[Sink::{}] Ignoring Pause message in {:?} state",
                                &self.ctx.url, &self.state
                            );
                        }
                        SinkMsg::Stop(sender) => {
                            info!("[Sink::{}] Stopping...", &self.ctx.url);
                            self.state = Stopped;
                            self.ctx.log_err(
                                sender.send(self.sink.on_stop(&self.ctx).await).await,
                                "Error sending Stop reply",
                            );
                            // exit control plane
                            break;
                        }
                        SinkMsg::Drain(_sender) if self.state == Draining => {
                            info!(
                                "[Sink::{}] Ignoring Drain message in {:?} state",
                                &self.ctx.url, &self.state
                            );
                        }
                        SinkMsg::Drain(sender) if self.state == Drained => {
                            debug!(
                                "[Sink::{}] Received Drain msg while already being drained.",
                                &self.ctx.url
                            );
                            if sender.send(Msg::SinkDrained).await.is_err() {
                                error!(
                                    "[Sink::{}] Error sending SinkDrained message.",
                                    &self.ctx.url
                                );
                            }
                        }
                        SinkMsg::Drain(sender) => {
                            // send message back if we already received Drain signal from all input pipelines
                            debug!("[Sink::{}] Draining...", &self.ctx.url);
                            self.state = Draining;
                            self.drain_channel = Some(sender);
                            if self.drains_received.is_superset(&self.starts_received) {
                                // we are all drained
                                debug!("[Sink::{}] Drained.", &self.ctx.url);
                                self.state = Drained;
                                if let Some(sender) = self.drain_channel.take() {
                                    if sender.send(Msg::SinkDrained).await.is_err() {
                                        error!(
                                            "[Sink::{}] Error sending SinkDrained message",
                                            &self.ctx.url
                                        );
                                    }
                                }
                            } else {
                                debug!("[Sink::{}] Not all drains received yet, waiting for drains from: {:?}", &self.ctx.url, self.starts_received.difference(&self.drains_received).collect::<Vec<_>>());
                            }
                        }
                        SinkMsg::ConnectionEstablished => {
                            self.ctx.log_err(
                                self.sink.on_connection_established(&self.ctx).await,
                                "Error during on_connection_established",
                            );
                            let cf = Event::cb_open(nanotime(), self.merged_operator_meta.clone());
                            // send CB restore to all pipes
                            send_contraflow(&self.pipelines, &self.ctx.url, cf).await;
                        }
                        SinkMsg::ConnectionLost => {
                            // clean out all pending stream data from EventSerializer - we assume all streams closed at this point
                            self.serializer.clear();
                            self.ctx.log_err(
                                self.sink.on_connection_lost(&self.ctx).await,
                                "Error during on_connection_lost",
                            );
                            // send CB trigger to all pipes
                            let cf = Event::cb_close(nanotime(), self.merged_operator_meta.clone());
                            send_contraflow(&self.pipelines, &self.ctx.url, cf).await;
                        }
                        SinkMsg::Event { event, port } => {
                            let cf_builder = ContraflowData::from(&event);

                            self.metrics_reporter.increment_in();
                            if let Some(t) = self.metrics_reporter.periodic_flush(event.ingest_ns) {
                                self.metrics_reporter
                                    .send_sink_metrics(self.sink.metrics(t));
                            }

                            // FIXME: fix additional clones here for merge
                            self.merged_operator_meta.merge(event.op_meta.clone());
                            let transactional = event.transactional;
                            let start = nanotime();
                            let res = self
                                .sink
                                .on_event(
                                    port.borrow(),
                                    event,
                                    &self.ctx,
                                    &mut self.serializer,
                                    start,
                                )
                                .await;
                            let duration = nanotime() - start;
                            match res {
                                Ok(replies) => {
                                    // TODO: send metric for duration
                                    handle_replies(
                                        replies,
                                        duration,
                                        cf_builder,
                                        &self.pipelines,
                                        &self.ctx.url,
                                        transactional && self.sink.auto_ack(),
                                    )
                                    .await;
                                }
                                Err(_e) => {
                                    // sink error that is not signalled via SinkReply::Fail (not handled)
                                    // TODO: error logging? This could fill the logs quickly. Rather emit a metrics event with the logging info?
                                    if transactional {
                                        let cf = cf_builder.into_fail();
                                        send_contraflow(&self.pipelines, &self.ctx.url, cf).await;
                                    }
                                }
                            };
                        }
                        SinkMsg::Signal { signal } => {
                            // special treatment
                            match signal.kind {
                                Some(SignalKind::Drain(source_uid)) => {
                                    debug!(
                                        "[Sink::{}] Drain signal received from {}",
                                        &self.ctx.url, source_uid
                                    );
                                    // account for all received drains per source
                                    self.drains_received.insert(source_uid);
                                    // check if all "reachable sources" did send a `Drain` signal
                                    if self.drains_received.is_superset(&self.starts_received) {
                                        debug!("[Sink::{}] Sink Drained.", &self.ctx.url);
                                        self.state = Drained;
                                        if let Some(sender) = self.drain_channel.take() {
                                            if sender.send(Msg::SinkDrained).await.is_err() {
                                                error!(
                                                    "[Sink::{}] Error sending SinkDrained message",
                                                    &self.ctx.url
                                                );
                                            }
                                        }
                                    }

                                    // send a cb Drained contraflow message back
                                    let cf = ContraflowData::from(&signal)
                                        .into_cb(CbAction::Drained(source_uid));
                                    send_contraflow(&self.pipelines, &self.ctx.url, cf).await;
                                }
                                Some(SignalKind::Start(source_uid)) => {
                                    debug!(
                                        "[Sink::{}] Received Start signal from {}",
                                        &self.ctx.url, source_uid
                                    );
                                    self.starts_received.insert(source_uid);
                                }
                                _ => {} // ignore
                            }
                            // hand it over to the sink impl
                            let cf_builder = ContraflowData::from(&signal);
                            let start = nanotime();
                            let res = self
                                .sink
                                .on_signal(signal, &self.ctx, &mut self.serializer)
                                .await;
                            let duration = nanotime() - start;
                            match res {
                                Ok(replies) => {
                                    handle_replies(
                                        replies,
                                        duration,
                                        cf_builder,
                                        &self.pipelines,
                                        &self.ctx.url,
                                        false,
                                    )
                                    .await;
                                }
                                Err(e) => {
                                    // logging here is ok, as this is mostly limited to ticks (every 100ms)
                                    error!(
                                        "[Connector::{}] Error handling signal: {}",
                                        &self.ctx.url, e
                                    );
                                }
                            }
                        }
                    }
                }
                SinkMsgWrapper::FromSink(reply) => {
                    // handle asynchronous sink replies
                    let cf = match reply {
                        AsyncSinkReply::Ack(data, duration) => Event::cb_ack_with_timing(
                            data.ingest_ns,
                            data.event_id,
                            data.op_meta.into(),
                            duration,
                        ),
                        AsyncSinkReply::Fail(data) => {
                            Event::cb_fail(data.ingest_ns, data.event_id, data.op_meta.into())
                        }
                        AsyncSinkReply::CB(data, cb) => {
                            Event::insight(cb, data.event_id, data.ingest_ns, data.op_meta.into())
                        }
                    };
                    send_contraflow(&self.pipelines, &self.ctx.url, cf).await;
                }
            }
        }
        // sink has been stopped
        info!("[Sink::{}] Terminating Sink Task.", &self.ctx.url);
        Ok(())
    }
}

#[repr(C)]
#[derive(Clone, Debug, StableAbi)]
/// basic data to build contraflow messages
pub struct ContraflowData {
    event_id: EventId,
    ingest_ns: u64,
    op_meta: PdkOpMeta,
}

impl ContraflowData {
    fn into_ack(self, duration: u64) -> Event {
        Event::cb_ack_with_timing(self.ingest_ns, self.event_id, self.op_meta.into(), duration)
    }
    fn into_fail(self) -> Event {
        Event::cb_fail(self.ingest_ns, self.event_id, self.op_meta.into())
    }
    fn cb(&self, cb: CbAction) -> Event {
        Event::insight(
            cb,
            self.event_id.clone(),
            self.ingest_ns,
            self.op_meta.clone().into(),
        )
    }
    fn into_cb(self, cb: CbAction) -> Event {
        Event::insight(cb, self.event_id, self.ingest_ns, self.op_meta.into())
    }
}

impl From<&Event> for ContraflowData {
    fn from(event: &Event) -> Self {
        ContraflowData {
            event_id: event.id.clone(),
            ingest_ns: event.ingest_ns,
            op_meta: event.op_meta.clone().into(), // TODO: mem::swap here?
        }
    }
}

impl From<Event> for ContraflowData {
    fn from(event: Event) -> Self {
        ContraflowData {
            event_id: event.id,
            ingest_ns: event.ingest_ns,
            op_meta: event.op_meta.into(),
        }
    }
}

impl From<&PdkEvent> for ContraflowData {
    fn from(event: &PdkEvent) -> Self {
        ContraflowData {
            event_id: event.id.clone(),
            ingest_ns: event.ingest_ns,
            op_meta: event.op_meta.clone(), // TODO: mem::swap here?
        }
    }
}

/// send contraflow back to pipelines
async fn send_contraflow(
    pipelines: &[(TremorUrl, pipeline::Addr)],
    connector_url: &TremorUrl,
    contraflow: Event,
) {
    if let Some(((last_url, last_addr), rest)) = pipelines.split_last() {
        for (url, addr) in rest {
            if let Err(e) = addr.send_insight(contraflow.clone()).await {
                error!(
                    "[Connector::{}] Error sending contraflow to {}: {}",
                    &connector_url, url, e
                );
            }
        }
        if let Err(e) = last_addr.send_insight(contraflow).await {
            error!(
                "[Connector::{}] Error sending contraflow to {}: {}",
                &connector_url, last_url, e
            );
        }
    }
}

async fn handle_replies(
    reply: SinkReply,
    duration: u64,
    cf_builder: ContraflowData,
    pipelines: &[(TremorUrl, pipeline::Addr)],
    connector_url: &TremorUrl,
    send_auto_ack: bool,
) {
    if reply.cb != CbAction::None {
        // we do not maintain a merged op_meta here, to avoid the cost
        // the downside is, only operators which this event passed get to know this CB event
        // but worst case is, 1 or 2 more events are lost - totally worth it
        send_contraflow(pipelines, connector_url, cf_builder.cb(reply.cb)).await;
    }
    match reply.ack {
        SinkAck::Ack => {
            send_contraflow(pipelines, connector_url, cf_builder.into_ack(duration)).await;
        }
        SinkAck::Fail => {
            send_contraflow(pipelines, connector_url, cf_builder.into_fail()).await;
        }
        SinkAck::None => {
            if send_auto_ack {
                send_contraflow(pipelines, connector_url, cf_builder.into_ack(duration)).await;
            }
        }
    }
}
