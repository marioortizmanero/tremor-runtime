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

//! Simple Sink implementation for handling a single stream
//!
//! With some shenanigans removed, compared to `ChannelSink`.

use crate::connectors::{sink::SinkReply, ConnectorContext, StreamDone};
use crate::errors::Result;
use abi_stable::std_types::ROption::RSome;
use async_std::{
    channel::{bounded, Receiver, Sender},
    task,
};
use std::marker::PhantomData;
use tremor_common::time::nanotime;

use super::channel_sink::{NoMeta, SinkMeta, SinkMetaBehaviour, WithMeta};
use super::{AsyncSinkReply, ContraflowData, EventSerializer, Sink, SinkContext, StreamWriter};

/// simple Sink implementation that is handling only a single stream
pub struct SingleStreamSink<B>
where
    B: SinkMetaBehaviour + Send + Sync,
{
    _b: PhantomData<B>,
    tx: Sender<SinkData>,
    rx: Receiver<SinkData>,
    reply_tx: Sender<AsyncSinkReply>,
}

impl SingleStreamSink<NoMeta> {
    /// Constructs a new single stream sink with metadata support redacted
    pub fn new_no_meta(qsize: usize, reply_tx: Sender<AsyncSinkReply>) -> Self {
        SingleStreamSink::new(qsize, reply_tx)
    }
}

impl SingleStreamSink<WithMeta> {
    /// Constructs a new single stream sink with metadata support enabled
    pub fn new_with_meta(qsize: usize, reply_tx: Sender<AsyncSinkReply>) -> Self {
        SingleStreamSink::new(qsize, reply_tx)
    }
}

impl<B> SingleStreamSink<B>
where
    B: SinkMetaBehaviour + Send + Sync,
{
    /// constructs a sink that requires metadata
    pub fn new(qsize: usize, reply_tx: Sender<AsyncSinkReply>) -> Self {
        let (tx, rx) = bounded(qsize);
        Self {
            tx,
            rx,
            reply_tx,
            _b: PhantomData::default(),
        }
    }
    /// hand out a `ChannelSinkRuntime` instance in order to register stream writers
    pub fn runtime(&self) -> SingleStreamSinkRuntime {
        SingleStreamSinkRuntime {
            rx: self.rx.clone(),
            reply_tx: self.reply_tx.clone(),
        }
    }
}

pub(crate) struct SinkData {
    data: Vec<Vec<u8>>,
    meta: Option<SinkMeta>,
    contraflow: Option<ContraflowData>,
    start: u64,
}

/// The runtime receiving and writing data out
#[derive(Clone)]
pub struct SingleStreamSinkRuntime {
    rx: Receiver<SinkData>,
    reply_tx: Sender<AsyncSinkReply>,
}

impl SingleStreamSinkRuntime {
    pub(crate) fn register_stream_writer<W>(
        &self,
        stream: u64,
        ctx: &ConnectorContext,
        mut writer: W,
    ) where
        W: StreamWriter + 'static,
    {
        let ctx = ctx.clone();
        let rx = self.rx.clone();
        let reply_tx = self.reply_tx.clone();
        task::spawn(async move {
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
                rx.recv().await,
            ) {
                let failed = writer.write(data, meta).await.is_err();

                if let Some(cf_data) = contraflow {
                    let reply = if failed {
                        AsyncSinkReply::Fail(cf_data)
                    } else {
                        AsyncSinkReply::Ack(cf_data, nanotime() - start)
                    };
                    if let Err(e) = reply_tx.send(reply).await {
                        error!(
                            "[Connector::{}] Error sending async sink reply: {}",
                            ctx.alias, e
                        );
                    }
                };
            }
            let error = match writer.on_done(stream).await {
                Err(e) => Some(e),
                Ok(StreamDone::ConnectorClosed) => ctx
                    .notifier
                    .notify() /*.await*/
                    .err(),
                Ok(_) => None,
            };
            if let RSome(e) = error {
                error!(
                    "[Connector::{}] Error shutting down write half of stream {}: {}",
                    ctx.alias, stream, e
                );
            }
            Result::Ok(())
        });
    }
}
