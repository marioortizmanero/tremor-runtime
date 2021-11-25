//! Similarly to [`PdkValue`], the types defined in this module are only meant
//! to be used temporarily for the plugin interface. They can be converted to
//! their original types for full functionality, and back to the PDK version in
//! order to pass them to the runtime.
//!
//! [`PdkValue`]: [`tremor_value::pdk::PdkValue`]

use crate::{
    CbAction, Event, EventId, EventOriginUri, OpMeta, PrimStr, SignalKind, TrackedPullIds,
};

use tremor_script::pdk::PdkEventPayload;
use tremor_value::{pdk::PdkValue, Value};

use abi_stable::{
    std_types::{RHashMap, ROption, RString, RVec, Tuple2},
    StableAbi,
};

/// Temporary type to represent [`OpMeta`] in the PDK interface. Refer to
/// the [`crate::pdk`] top-level documentation for more information.
///
/// [`OpMeta`]: [`crate::OpMeta`]
#[repr(C)]
#[derive(Debug, Clone, StableAbi)]
// FIXME: this used to be a binary tree map, not a hash map. Not sure if that
// was because of performance or anything similar, but `abi_stable` only has
// hash maps so it's left that way for now.
pub struct PdkOpMeta(RHashMap<PrimStr<u64>, PdkValue<'static>>);

/// Easily converting the original type to the PDK one to pass it through the
/// FFI boundary.
impl From<OpMeta> for PdkOpMeta {
    fn from(original: OpMeta) -> Self {
        PdkOpMeta(
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

/// Easily converting the PDK type to the original one to access its full
/// functionality.
impl From<PdkOpMeta> for OpMeta {
    fn from(original: PdkOpMeta) -> Self {
        OpMeta(
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

/// Temporary type to represent [`Event`] in the PDK interface. Refer to
/// the [`crate::pdk`] top-level documentation for more information.
///
/// [`Event`]: [`crate::Event`]
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
    pub op_meta: PdkOpMeta,
    /// this needs transactional data
    pub transactional: bool,
}

/// Easily converting the original type to the PDK one to pass it through the
/// FFI boundary.
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

/// Easily converting the PDK type to the original one to access its full
/// functionality.
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
