//! FFI-safe types to communicate with the plugins. They're meant to be
//! converted to/from the original type and back so that it can be passed
//! through the plugin interface. Thus, no functionality is implemented other
//! than the conversion from and to the original type.

use crate::{CbAction, PrimStr, SignalKind, TrackedPullIds};

use tremor_script::pdk::EventPayload;
use tremor_value::pdk::Value;

use abi_stable::{
    std_types::{RHashMap, ROption, RString, RVec, Tuple2},
    StableAbi,
};

// FIXME: we can probably avoid this after `simd_json_derive` works for
// `abi_stable`.
// FIXME: this used to be a binary tree map, not a hash map. Not sure if that
// was because of performance or anything similar, but `abi_stable` only has
// hash maps so it's left that way for now.
#[repr(C)]
#[derive(Debug, Clone, StableAbi)]
pub struct OpMeta(RHashMap<PrimStr<u64>, Value<'static>>);

impl From<crate::OpMeta> for OpMeta {
    fn from(original: crate::OpMeta) -> Self {
        OpMeta(
            original
                .0
                .into_iter()
                .map(|(k, v)| {
                    let v: tremor_value::Value = v.into();
                    let v: Value = v.into();
                    (k, v)
                })
                .collect(),
        )
    }
}

impl From<OpMeta> for crate::OpMeta {
    fn from(original: OpMeta) -> Self {
        crate::OpMeta(
            original
                .0
                .into_iter()
                .map(|Tuple2(k, v)| {
                    let v: tremor_value::Value = v.into();
                    let v: simd_json::OwnedValue = v.into();
                    (k, v)
                })
                .collect(),
        )
    }
}

// FIXME: we can probably avoid this after `simd_json_derive` works for
// `abi_stable`.
#[repr(C)]
#[derive(Debug, Clone, StableAbi)]
pub struct EventId {
    source_id: u64,
    stream_id: u64,
    event_id: u64,
    pull_id: u64,
    tracked_pull_ids: RVec<TrackedPullIds>,
}

impl From<crate::EventId> for EventId {
    fn from(original: crate::EventId) -> Self {
        EventId {
            source_id: original.source_id,
            stream_id: original.stream_id,
            event_id: original.event_id,
            pull_id: original.pull_id,
            tracked_pull_ids: original.tracked_pull_ids.into(),
        }
    }
}

impl From<EventId> for crate::EventId {
    fn from(original: EventId) -> Self {
        crate::EventId {
            source_id: original.source_id,
            stream_id: original.stream_id,
            event_id: original.event_id,
            pull_id: original.pull_id,
            tracked_pull_ids: original.tracked_pull_ids.into(),
        }
    }
}

// FIXME: we can probably avoid this after `simd_json_derive` works for
// `abi_stable`.
#[repr(C)]
#[derive(Debug, Clone, StableAbi)]
pub struct EventOriginUri {
    /// schema part
    pub scheme: RString,
    /// host part
    pub host: RString,
    /// port part
    pub port: ROption<u16>,
    /// path part
    pub path: RVec<RString>,
    // implement query params if we find a good usecase for it
    //pub query: Hashmap<String, String>
}

impl From<crate::EventOriginUri> for EventOriginUri {
    fn from(original: crate::EventOriginUri) -> Self {
        EventOriginUri {
            scheme: original.scheme.into(),
            host: original.host.into(),
            port: original.port.into(),
            path: original.path.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<EventOriginUri> for crate::EventOriginUri {
    fn from(original: EventOriginUri) -> Self {
        crate::EventOriginUri {
            scheme: original.scheme.into(),
            host: original.host.into(),
            port: original.port.into(),
            path: original.path.into_iter().map(Into::into).collect(),
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone, StableAbi)]
pub struct Event {
    /// The event ID
    pub id: EventId,
    /// The event Data
    pub data: EventPayload,
    /// Nanoseconds at when the event was ingested
    pub ingest_ns: u64,
    /// URI to identify the origin of the event
    pub origin_uri: ROption<EventOriginUri>,
    /// The kind of the event
    pub kind: ROption<SignalKind>,
    /// If this event is batched (containing multiple events itself)
    pub is_batch: bool,

    /// Circuit breaker action
    pub cb: CbAction,
    /// Metadata for operators
    pub op_meta: OpMeta,
    /// this needs transactional data
    pub transactional: bool,
}

impl From<crate::Event> for Event {
    fn from(original: crate::Event) -> Self {
        Event {
            id: original.id.into(),
            data: original.data.into(),
            ingest_ns: original.ingest_ns,
            origin_uri: original.origin_uri.map(Into::into).into(),
            kind: original.kind.into(),
            is_batch: original.is_batch,
            cb: original.cb,
            op_meta: original.op_meta.into(),
            transactional: original.transactional,
        }
    }
}

impl From<Event> for crate::Event {
    fn from(original: Event) -> Self {
        crate::Event {
            id: original.id.into(),
            data: original.data.into(),
            ingest_ns: original.ingest_ns,
            origin_uri: original.origin_uri.map(Into::into).into(),
            kind: original.kind.into(),
            is_batch: original.is_batch,
            cb: original.cb,
            op_meta: original.op_meta.into(),
            transactional: original.transactional,
        }
    }
}
