//! FFI-safe types to communicate with the plugins. They're meant to be
//! converted to/from the original type and back so that it can be passed
//! through the plugin interface. Thus, no functionality is implemented other
//! than the conversion from and to the original type.

use tremor_value::pdk::Value;

use std::pin::Pin;
use std::sync::Arc;

use abi_stable::{
    std_types::{RArc, RVec},
    StableAbi,
};

#[repr(C)]
#[derive(Debug, Clone, StableAbi)]
pub struct ValueAndMeta<'event> {
    v: Value<'event>,
    m: Value<'event>,
}

impl<'event> From<crate::ValueAndMeta<'event>> for ValueAndMeta<'event> {
    fn from(original: crate::ValueAndMeta<'event>) -> Self {
        ValueAndMeta {
            v: original.v.into(),
            m: original.m.into(),
        }
    }
}

impl<'event> From<ValueAndMeta<'event>> for crate::ValueAndMeta<'event> {
    fn from(original: ValueAndMeta<'event>) -> Self {
        crate::ValueAndMeta {
            v: original.v.into(),
            m: original.m.into(),
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone, StableAbi)]
pub struct EventPayload {
    /// The vector of raw input values
    raw: RVec<RArc<Pin<RVec<u8>>>>,
    data: ValueAndMeta<'static>,
}

impl From<crate::EventPayload> for EventPayload {
    fn from(original: crate::EventPayload) -> Self {
        let raw = original
            .raw
            .into_iter()
            .map(|x| {
                // FIXME: this conversion could probably be simpler
                let x: RArc<Pin<RVec<u8>>> = RArc::new(Pin::new((**x).into()));
                x
            })
            .collect();
        EventPayload {
            raw,
            data: original.data.into(),
        }
    }
}

impl From<EventPayload> for crate::EventPayload {
    fn from(original: EventPayload) -> Self {
        let raw = original
            .raw
            .into_iter()
            .map(|x| {
                // FIXME: this conversion could probably be simpler
                let x: Arc<Pin<Vec<u8>>> = Arc::new(Pin::new((**x).into()));
                x
            })
            .collect();
        crate::EventPayload {
            raw,
            data: original.data.into(),
        }
    }
}
