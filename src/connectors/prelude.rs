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

pub use crate::connectors::sink::{
    AsyncSinkReply, ChannelSink, ChannelSinkRuntime, ContraflowData, EventSerializer,
    SingleStreamSink, SingleStreamSinkRuntime, SinkAck, SinkAddr, SinkContext,
    SinkManagerBuilder, SinkMeta, SinkReply, SinkRuntime, StreamWriter,
};

pub use crate::connectors::source::{
    ChannelSource, ChannelSourceRuntime, SourceAddr, SourceContext, SourceManagerBuilder,
    SourceReply, StreamReader, DEFAULT_POLL_INTERVAL,
};
pub use crate::connectors::utils::{
    reconnect::Attempt,
    url::{Defaults, Url},
};
pub use crate::connectors::{
    spawn_task, CodecReq, ConnectorContext, ConnectorType, Context,
    StreamDone, StreamIdGen, ACCEPT_TIMEOUT,
};
pub use crate::errors::{Error, Kind as ErrorKind, Result};
pub use crate::utils::hostname;
pub use crate::{Event, QSIZE};
pub use std::sync::atomic::Ordering;
pub use tremor_common::ports::{ERR, IN, OUT};
pub use tremor_pipeline::{
    CbAction, ConfigImpl, EventIdGenerator, EventOriginUri, DEFAULT_STREAM_ID,
};

pub use tremor_script::prelude::*;
/// default buf size used for reading from files and streams (sockets etc)
///
/// equals default chunk size for `BufReader`
pub(crate) const DEFAULT_BUF_SIZE: usize = 8 * 1024;
/// default buf size used for reading from files and streams (sockets etc)
pub(crate) fn default_buf_size() -> usize {
    DEFAULT_BUF_SIZE
}

/// Encapsulates connector configuration
pub use crate::connectors::ConnectorConfig;

/// For the PDK
pub(crate) use crate::connectors::{sink::Sink, source::Source, Connector};
pub use crate::{
    connectors::{
        sink::{
            BoxedContraflowSender, BoxedEventSerializer, BoxedRawSink, ContraflowSenderOpaque,
            EventSerializerOpaque, MutEventSerializer, RawSink,
        },
        source::{BoxedRawSource, RawSource},
        utils::{
            quiescence::BoxedQuiescenceBeacon,
            reconnect::BoxedConnectionLostNotifier
        },
        BoxedRawConnector, RawConnector,
    },
    pdk::{
        connectors::{ConnectorMod, ConnectorMod_Ref},
        RResult,
    },
};
