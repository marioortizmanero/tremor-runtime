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

use std::sync::atomic::Ordering;

use crate::connectors::prelude::*;
use async_broadcast::{broadcast, Receiver, Sender};
use beef::Cow;
use halfbrown::HashMap;
use tremor_common::url::ports::{ERR, IN, OUT};
use tremor_pipeline::metrics::{metrics_value, metrics_value_count};
use tremor_pipeline::MetricsSender;
use tremor_script::EventPayload;

#[derive(Clone, Debug)]
pub(crate) struct MetricsChannel {
    tx: Sender<Msg>,
    rx: Receiver<Msg>,
}

impl MetricsChannel {
    pub(crate) fn new(qsize: usize) -> Self {
        let (mut tx, rx) = broadcast(qsize);
        // We user overflow so that non collected messages can be removed
        // FIXME: is this what we want? for Metrics it should be good enough
        // we consume them quickly and if not we got bigger problems
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

/// metrics reporter for connector sources
pub struct SourceReporter {
    alias: String,
    metrics_out: u64,
    metrics_err: u64,
    tx: MetricsSender,
    flush_interval_ns: Option<u64>,
    last_flush_ns: u64,
}

impl SourceReporter {
    pub(crate) fn new(alias: String, tx: MetricsSender, flush_interval_s: Option<u64>) -> Self {
        Self {
            alias,
            metrics_out: 0,
            metrics_err: 0,
            tx,
            flush_interval_ns: flush_interval_s.map(|s| s * 1_000_000_000),
            last_flush_ns: 0,
        }
    }

    pub(crate) fn increment_out(&mut self) {
        self.metrics_out += 1;
    }

    pub(crate) fn increment_err(&mut self) {
        self.metrics_err += 1;
    }

    /// Flush the metrics and send them out if the flush interval is set and the time has come
    /// returns `Some(timestamp)` if it did flush the system metrics
    pub(crate) fn periodic_flush(&mut self, timestamp: u64) -> Option<u64> {
        if let Some(interval) = self.flush_interval_ns {
            if timestamp >= self.last_flush_ns + interval {
                let payload_out = make_metrics_payload(
                    timestamp,
                    Cow::from(OUT),
                    self.metrics_out,
                    &self.artefact_url,
                );
                let payload_err = make_metrics_payload(
                    timestamp,
                    Cow::from(ERR),
                    self.metrics_err,
                    &self.artefact_url,
                );
                send(&self.tx, payload_out, &self.artefact_url);
                send(&self.tx, payload_err, &self.artefact_url);
                self.last_flush_ns = timestamp;
                return Some(timestamp);
            }
        }
        None
    }

    /// simply send source metrics
    pub(crate) fn send_source_metrics(&self, metrics: Vec<EventPayload>) {
        for metric in metrics {
            send(&self.tx, metric, &self.alias);
        }
    }
}

/// metrics reporter for connector sinks
pub(crate) struct SinkReporter {
    alias: String,
    metrics_in: u64,
    tx: MetricsSender,
    flush_interval_ns: Option<u64>,
    last_flush_ns: u64,
}

impl SinkReporter {
    pub(crate) fn new(alias: String, tx: MetricsSender, flush_interval_s: Option<u64>) -> Self {
        Self {
            alias,
            metrics_in: 0,
            tx,
            flush_interval_ns: flush_interval_s.map(|s| s * 1_000_000_000),
            last_flush_ns: 0,
        }
    }

    pub(crate) fn increment_in(&mut self) {
        self.metrics_in += 1;
    }

    pub(crate) fn periodic_flush(&mut self, timestamp: u64) -> Option<u64> {
        if let Some(interval) = self.flush_interval_ns {
            if timestamp >= self.last_flush_ns + interval {
                let payload = make_metrics_payload(
                    timestamp,
                    Cow::from(IN),
                    self.metrics_in,
                    &self.artefact_url,
                );
                send(&self.tx, payload, &self.artefact_url);
                self.last_flush_ns = timestamp;
                return Some(timestamp);
            }
        }
        None
    }

    /// simply send source metrics
    pub(crate) fn send_sink_metrics(&self, metrics: Vec<EventPayload>) {
        for metric in metrics {
            send(&self.tx, metric, &self.alias);
        }
    }
}

// this is simple forwarding
#[cfg(not(tarpaulin_include))]
pub(crate) fn send(tx: &MetricsSender, metric: EventPayload, alias: &str) {
    use tremor_pipeline::MetricsMsg;

    if let Err(_e) = tx.try_broadcast(MetricsMsg::new(metric, None)) {
        error!(
            "[Connector::{}] Error sending to system metrics connector.",
            &alias
        );
    }
}

#[must_use]
pub(crate) fn make_event_count_metrics_payload(
    timestamp: u64,
    port: Cow<'static, str>,
    count: u64,
    artefact_id: &str,
) -> EventPayload {
    let mut tags: HashMap<Cow<'static, str>, Value<'static>> = HashMap::with_capacity(2);
    tags.insert_nocheck(Cow::const_str("connector"), artefact_id.to_string().into());
    tags.insert_nocheck(Cow::const_str("port"), port.into());

    let value = metrics_value_count(Cow::from("connector_events"), tags, count, timestamp);
    // full metrics payload
    (value, Value::object()).into()
}

// TODO: add convenience functions for creating custom metrics payloads
#[must_use]
pub(crate) fn make_metrics_payload(
    name: &'static str,
    fields: HashMap<Cow<'static, str>, Value<'static>>,
    tags: HashMap<Cow<'static, str>, Value<'static>>,
    timestamp: u64,
) -> EventPayload {
    let value = metrics_value(Cow::const_str(name), tags, fields, timestamp);
    (value, Value::object()).into()
}
