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
use async_std::{
    channel::{bounded, Receiver, Sender},
    task,
};
use tremor_common::time::nanotime;

use super::{AsyncSinkReply, EventCfData, Sink, StreamWriter};

pub(crate) struct SinkData {
    data: Vec<Vec<u8>>,
    contraflow: Option<EventCfData>,
    start: u64,
}

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
                    contraflow,
                    start,
                }),
            ) = (
                ctx.quiescence_beacon.continue_writing().await,
                rx.recv().await,
            ) {
                let failed = writer.write(data).await.is_err();

                if let Some(cf_data) = contraflow {
                    let reply = if failed {
                        AsyncSinkReply::Fail(cf_data)
                    } else {
                        AsyncSinkReply::Ack(cf_data, nanotime() - start)
                    };
                    if let Err(e) = reply_tx.send(reply).await {
                        error!(
                            "[Connector::{}] Error sending async sink reply: {}",
                            ctx.url, e
                        );
                    }
                };
            }
            let error = match writer.on_done(stream).await {
                Err(e) => Some(e),
                Ok(StreamDone::ConnectorClosed) => ctx.notifier.notify().await.err(),
                Ok(_) => None,
            };
            if let Some(e) = error {
                error!(
                    "[Connector::{}] Error shutting down write half of stream {}: {}",
                    ctx.url, stream, e
                );
            }
            Result::Ok(())
        });
    }
}
