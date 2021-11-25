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

//! Sink implementation that keeps track of multiple streams and keeps channels to send to each stream

use crate::connectors::prelude::*;
use crate::connectors::{
    reconnect::ConnectionLostNotifierOpaque, sink::EventSerializerOpaque, ConnectorContext,
    QuiescenceBeaconOpaque, StreamDone,
};
use crate::errors::{ErrorKind, Result};
use crate::QSIZE;
use abi_stable::std_types::ROption::{RNone, RSome};
use async_std::channel::{bounded, Receiver, Sender};
use async_std::task;
use bimap::BiMap;
use either::Either;
use hashbrown::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::atomic::Ordering;
use tremor_common::time::nanotime;
use tremor_pipeline::{CbAction, Event, SignalKind};
use tremor_value::Value;
use value_trait::ValueAccess;

/// Behavioral trait for defining if a Channel Sink needs metadata or not
pub trait SinkMetaBehaviour: Send + Sync {
    /// Does this channel sink need metadata
    const NEEDS_META: bool;
}

/// Marker that this sink doesn't need any metadata
pub struct NoMeta {}
impl SinkMetaBehaviour for NoMeta {
    const NEEDS_META: bool = false;
}

/// Marker that this sink needs metadata - and thus requires a clone
pub struct WithMeta {}
impl SinkMetaBehaviour for WithMeta {
    const NEEDS_META: bool = true;
}

/// messages a channel sink can receive
pub enum ChannelSinkMsg<M>
where
    M: Hash + Eq + Send + 'static,
{
    /// add a new stream
    NewStream {
        /// the id of the stream
        stream_id: u64,
        /// stream metadata used for resolving a stream
        meta: Option<M>,
        /// sender to the actual stream handling data
        sender: Sender<SinkData>,
    },
    /// remove the stream
    RemoveStream(u64),
}

/// Metadata for a sink message
pub type SinkMeta = Value<'static>;

/// some data for a `ChannelSink` stream
#[derive(Clone, Debug)]
pub struct SinkData {
    /// data to send
    pub data: Vec<Vec<u8>>,
    /// async reply utils (if required)
    pub contraflow: Option<(ContraflowData, Sender<AsyncSinkReply>)>,
    /// Metadata for this request
    pub meta: Option<SinkMeta>,
    /// timestamp of processing start
    pub start: u64,
}

/// tracking 1 channel per stream
pub struct ChannelSink<M, F, B>
where
    M: Hash + Eq + Send + 'static,
    F: Fn(&Value<'_>) -> Option<M>,
    B: SinkMetaBehaviour,
{
    _b: PhantomData<B>,
    streams_meta: BiMap<M, u64>,
    streams: HashMap<u64, Sender<SinkData>>,
    resolver: F,
    tx: Sender<ChannelSinkMsg<M>>,
    rx: Receiver<ChannelSinkMsg<M>>,
    reply_tx: Sender<AsyncSinkReply>,
}

impl<T, F> ChannelSink<T, F, NoMeta>
where
    T: Hash + Eq + Send + 'static,
    F: Fn(&Value<'_>) -> Option<T>,
{
    /// constructor of a ChannelSink that is not sending the event metadata to the StreamWriter
    /// and by that saves an expensive clone()
    pub fn new_no_meta(qsize: usize, resolver: F, reply_tx: Sender<AsyncSinkReply>) -> Self {
        let (tx, rx) = bounded(qsize);
        ChannelSink::new(resolver, reply_tx, tx, rx)
    }

    /// Construct a new instance that redacts metadata with prepared `rx` and `tx`
    pub fn from_channel_no_meta(
        resolver: F,
        reply_tx: Sender<AsyncSinkReply>,
        tx: Sender<ChannelSinkMsg<T>>,
        rx: Receiver<ChannelSinkMsg<T>>,
    ) -> Self {
        ChannelSink::new(resolver, reply_tx, tx, rx)
    }
}

impl<T, F> ChannelSink<T, F, WithMeta>
where
    T: Hash + Eq + Send + 'static,
    F: Fn(&Value<'_>) -> Option<T>,
{
    /// Construct a new instance of a channel sink with metadata support
    pub fn new_with_meta(qsize: usize, resolver: F, reply_tx: Sender<AsyncSinkReply>) -> Self {
        let (tx, rx) = bounded(qsize);
        ChannelSink::new(resolver, reply_tx, tx, rx)
    }

    /// Construct a new instance with metadata support with prepared `rx` and `tx`
    pub fn from_channel_with_meta(
        resolver: F,
        reply_tx: Sender<AsyncSinkReply>,
        tx: Sender<ChannelSinkMsg<T>>,
        rx: Receiver<ChannelSinkMsg<T>>,
    ) -> Self {
        ChannelSink::new(resolver, reply_tx, tx, rx)
    }
}

// FIXME: implement PauseBehaviour correctly
impl<T, F, B> ChannelSink<T, F, B>
where
    T: Hash + Eq + Send + 'static,
    F: Fn(&Value<'_>) -> Option<T>,
    B: SinkMetaBehaviour,
{
    /// constructor of a ChannelSink that is sending the event metadata to the StreamWriter
    /// in case it needs it in the write.
    /// This costs a clone.
    pub fn new(
        resolver: F,
        reply_tx: Sender<AsyncSinkReply>,
        tx: Sender<ChannelSinkMsg<T>>,
        rx: Receiver<ChannelSinkMsg<T>>,
    ) -> Self {
        let streams = HashMap::with_capacity(8);
        let streams_meta = BiMap::with_capacity(8);
        Self {
            streams_meta,
            streams,
            resolver,
            tx,
            rx,
            reply_tx,
            _b: PhantomData::default(),
        }
    }

    /// hand out a `ChannelSinkRuntime` instance in order to register stream writers
    pub fn runtime(&self) -> ChannelSinkRuntime<T> {
        ChannelSinkRuntime {
            tx: self.tx.clone(),
        }
    }

    fn handle_channels_quickly(&mut self, serializer: &mut EventSerializer) -> bool {
        self.handle_channels(serializer, false)
    }
    /// returns true, if there are no more channels to send stuff to
    fn handle_channels(
        &mut self,
        serializer: &mut EventSerializer,
        clean_closed_streams: bool,
    ) -> bool {
        while let Ok(msg) = self.rx.try_recv() {
            match msg {
                ChannelSinkMsg::NewStream {
                    stream_id,
                    meta,
                    sender,
                } => {
                    trace!("[Sink::XXX] started new stream {}", stream_id);
                    self.streams.insert(stream_id, sender);
                    if let Some(meta) = meta {
                        self.streams_meta.insert(meta, stream_id);
                    }
                }
                ChannelSinkMsg::RemoveStream(stream_id) => {
                    self.remove_stream(stream_id);
                    serializer.drop_stream(stream_id);
                }
            }
        }
        // clean out closed streams
        if clean_closed_streams {
            for (stream_id, _) in self.streams.drain_filter(|_k, v| v.is_closed()) {
                self.streams_meta.remove_by_right(&stream_id);
                serializer.drop_stream(stream_id);
            }
        }
        self.streams.is_empty()
    }

    fn remove_stream(&mut self, stream_id: u64) {
        self.streams.remove(&stream_id);
        self.streams_meta.remove_by_right(&stream_id);
    }

    fn resolve_stream_from_meta<'lt, 'value>(
        &self,
        meta: &'lt Value<'value>,
        ctx: &SinkContext,
    ) -> Option<(&u64, &Sender<SinkData>)> {
        let sink_meta = get_sink_meta(meta, ctx);
        sink_meta
            .and_then(|sink_meta| (self.resolver)(sink_meta))
            .and_then(|stream_meta| self.streams_meta.get_by_left(&stream_meta))
            .and_then(|stream_id| {
                self.streams
                    .get(stream_id)
                    .map(|sender| (stream_id, sender))
            })
    }
}

/// The runtime driving the part of the sink that is receiving data and writing it out
#[derive(Clone)]
pub struct ChannelSinkRuntime<T>
where
    T: Hash + Eq + Send + 'static,
{
    tx: Sender<ChannelSinkMsg<T>>,
}

impl<T> ChannelSinkRuntime<T>
where
    T: Hash + Eq + Send + 'static,
{
    pub(crate) fn new(tx: Sender<ChannelSinkMsg<T>>) -> Self {
        Self { tx }
    }

    pub(crate) fn register_stream_writer<W, C>(
        &self,
        stream: u64,
        connection_meta: Option<T>,
        ctx: &C,
        mut writer: W,
    ) where
        W: StreamWriter + 'static,
        C: Context + Send + Sync + 'static,
    {
        let (stream_tx, stream_rx) = bounded::<SinkData>(QSIZE.load(Ordering::Relaxed));
        let stream_sink_tx = self.tx.clone();
        let ctx = ctx.clone();
        let tx = self.tx.clone();
        task::spawn(async move {
            tx.send(ChannelSinkMsg::NewStream {
                stream_id: stream,
                meta: connection_meta,
                sender: stream_tx,
            })
            .await?;
            // receive loop from channel sink
            while let (
                true,
                Ok(SinkData {
                    data,
                    meta,
                    contraflow,
                    start,
                }),
            ) = (
                ctx.quiescence_beacon.continue_writing().await,
                stream_rx.recv().await,
            ) {
                let failed = writer.write(data, meta).await.is_err();

                // send asyn contraflow insights if requested (only if event.transactional)
                if let Some((cf_data, sender)) = contraflow {
                    let reply = if failed {
                        AsyncSinkReply::Fail(cf_data)
                    } else {
                        AsyncSinkReply::Ack(cf_data, nanotime() - start)
                    };
                    if let Err(e) = sender.send(reply).await {
                        error!("{} Error sending async sink reply: {}", &ctx, e);
                    }
                }
                if failed {
                    break;
                }
            }
            let error = match writer.on_done(stream).await {
                Err(e) => RSome(e),
                Ok(StreamDone::ConnectorClosed) => ctx
                    .notifier
                    .notify()
                    .await
                    .err()
                    .map(|e| ErrorKind::PluginError(e).into()),
                Ok(_) => RNone,
            };
            if let RSome(e) = error {
                error!(
                    "{} Error shutting down write half of stream {}: {}",
                    &ctx, stream, e
                );
            }
            stream_sink_tx
                .send(ChannelSinkMsg::RemoveStream(stream))
                .await?;
            Result::Ok(())
        });
    }
}

/// Extract sink specific metadata from event metadata
///
/// The general path is `$<CONNECTOR_TYPE>`
/// Example: `$tcp_server`
fn get_sink_meta<'lt, 'value>(
    meta: &'lt Value<'value>,
    ctx: &SinkContext,
) -> Option<&'lt Value<'value>> {
    ctx.url
        .resource_type()
        .and_then(|rt| meta.get(&Cow::owned(rt.to_string())))
        .and_then(|rt_meta| {
            ctx.url
                .artefact()
                .and_then(|artefact| rt_meta.get(artefact))
        })
}
