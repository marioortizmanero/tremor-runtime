//! FFI-safe types to communicate with the plugins. They're meant to be
//! converted to/from the original type and back so that it can be passed
//! through the plugin interface. Thus, no functionality is implemented other
//! than the conversion from and to the original type.

use crate::{CbAction, Event, EventId, EventOriginUri, PrimStr, SignalKind, TrackedPullIds};

use tremor_script::pdk::PdkEventPayload;
use tremor_value::{pdk::PdkValue, Value};

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
pub struct OpMeta(RHashMap<PrimStr<u64>, PdkValue<'static>>);

impl From<crate::OpMeta> for OpMeta {
    fn from(original: crate::OpMeta) -> Self {
        OpMeta(
            original
                .0
                .into_iter()
                .map(|(k, v)| {
                    let v: Value = v.into();
                    let v: PdkValue = v.into();
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
                    let v: Value = v.into();
                    let v: simd_json::OwnedValue = v.into();
                    (k, v)
                })
                .collect(),
        )
    }
}

#[repr(C)]
#[derive(Debug, Clone, StableAbi)]
pub struct PdkEvent {
    /// The event ID
    pub id: EventId,
    /// The event Data
    pub data: PdkEventPayload,
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

impl From<Event> for PdkEvent {
    fn from(original: Event) -> Self {
        PdkEvent {
            id: original.id,
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

impl From<PdkEvent> for Event {
    fn from(original: PdkEvent) -> Self {
        Event {
            id: original.id.into(),
            data: original.data.into(),
            ingest_ns: original.ingest_ns,
            origin_uri: original.origin_uri.into(),
            kind: original.kind.into(),
            is_batch: original.is_batch,
            cb: original.cb,
            op_meta: original.op_meta.into(),
            transactional: original.transactional,
        }
    }
}
