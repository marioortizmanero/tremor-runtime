//! Implements the actual connector functionality.

use tremor_common::{time::nanotime, url::TremorUrl};
use tremor_pipeline::DEFAULT_STREAM_ID;
use tremor_runtime::{connectors::prelude::*, pdk::RResult, utils::hostname, ttry};
use tremor_script::{EventOriginUri, EventPayload};
use tremor_value::literal;

use std::{
    future,
    time::{Duration, Instant},
};

use abi_stable::{
    rstr, rvec, sabi_extern_fn,
    std_types::{
        ROption::{self, RNone, RSome},
        RResult::{RErr, ROk},
        RStr, RString,
    },
    type_level::downcasting::TD_Opaque,
};
use async_ffi::{BorrowingFfiFuture, FfiFuture, FutureExt};
use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Interval in milliseconds
    pub interval: u64,
}

impl ConfigImpl for Config {}

#[derive(Debug, Clone)]
struct Metronome {
    interval: Duration,
    next: Instant,
    /// Note that in the metronome we save the PDK version of EventOriginUri
    /// because it's needed to return a source reply.
    ///
    /// However, we could internally hold the regular feature-full
    /// EventOriginUri and convert to the PDK one whenever we want to return it.
    /// In this case we don't need any of its functionality so there's no need
    /// to.
    origin_uri: EventOriginUri,
}

impl RawConnector for Metronome {
    fn connect(
        &mut self,
        _ctx: &ConnectorContext,
        _attempt: &Attempt,
    ) -> BorrowingFfiFuture<'_, RResult<bool>> {
        // No connection is actually necessary, it's just local work
        future::ready(ROk(true)).into_ffi()
    }

    /// Exports the metronome as a source trait object
    fn create_source(
        &mut self,
        _ctx: SourceContext,
    ) -> BorrowingFfiFuture<'_, RResult<ROption<BoxedRawSource>>> {
        let metronome = self.clone();
        // We don't need to be able to downcast the connector back to the original
        // type, so we just pass it as an opaque type.
        let source = BoxedRawSource::from_value(metronome, TD_Opaque);
        future::ready(ROk(RSome(source))).into_ffi()
    }

    fn default_codec(&self) -> RStr {
        rstr!("json")
    }

    fn is_structured(&self) -> bool {
        true
    }
}

impl RawSource for Metronome {
    fn pull_data(
        &mut self,
        pull_id: u64,
        _ctx: &SourceContext,
    ) -> BorrowingFfiFuture<'_, RResult<SourceReply>> {
        // Even though this functionality may seem simple and panic-free,
        // it could occur in the addition operation, for example.
        let now = Instant::now();
        let reply = if self.next < now {
            self.next = now + self.interval;
            let data = literal!({
                "onramp": "metronome",
                "ingest_ns": nanotime(),
                "id": pull_id
            });
            // We need the pdk event payload, so we convert twice
            let data: EventPayload = data.into();
            SourceReply::Structured {
                origin_uri: self.origin_uri.clone(),
                payload: data.into(),
                stream: DEFAULT_STREAM_ID,
                port: RNone,
            }
        } else {
            let remaining = (self.next - now).as_millis() as u64;

            SourceReply::Empty(remaining)
        };

        future::ready(ROk(reply)).into_ffi()
    }

    fn is_transactional(&self) -> bool {
        false
    }
}

/// Exports the metronome as a connector trait object
#[sabi_extern_fn]
pub fn from_config(
    _id: TremorUrl,
    raw_config: ROption<RString>,
) -> FfiFuture<RResult<BoxedRawConnector>> {
    async move {
        if let RSome(raw_config) = raw_config {
            let config = ttry!(Config::from_str(&raw_config));

            let origin_uri = EventOriginUri {
                scheme: RString::from("tremor-metronome"),
                host: hostname().into(),
                port: RNone,
                path: rvec![config.interval.to_string().into()],
            };
            let metronome = Metronome {
                origin_uri,
                interval: Duration::from_millis(config.interval),
                next: Instant::now(),
            };

            ROk(BoxedRawConnector::from_value(metronome, TD_Opaque))
        } else {
            RErr(ErrorKind::MissingConfiguration(String::from("metronome")).into())
        }
    }
    .into_ffi()
}

#[sabi_extern_fn]
pub fn connector_type() -> ConnectorType {
    "metronome".into()
}
