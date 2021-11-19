//! FFI-safe types to communicate with the plugins. They're meant to be
//! converted to/from the original type and back so that it can be passed
//! through the plugin interface. Thus, no functionality is implemented other
//! than the conversion from and to the original type.

use crate::{EventId, EventOriginUri, SignalKind, CbAction, OpMeta};

use tremor_script::pdk::EventPayload;

use abi_stable::{StableAbi, std_types::ROption};

#[repr(C)]
#[derive(StableAbi)]
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
            id: original.id,
            data: original.data.into(),
            ingest_ns: original.ingest_ns,
            origin_uri: original.origin_uri.into(),
            kind: original.kind.into(),
            is_batch: original.is_batch,
            cb: original.cb,
            op_meta: original.op_meta,
            transactional: original.transactional,
        }
    }
}
