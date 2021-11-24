//! FFI-safe types to communicate with the plugins. They're meant to be
//! converted to/from the original type and back so that it can be passed
//! through the plugin interface. Thus, no functionality is implemented other
//! than the conversion from and to the original type.

use crate::{EventPayload, ValueAndMeta};

use std::{pin::Pin, sync::Arc};

use abi_stable::{
    std_types::{RArc, RVec},
    StableAbi,
};
use tremor_value::pdk::PdkValue;

#[repr(C)]
#[derive(Debug, Clone, StableAbi)]
pub struct PdkValueAndMeta<'event> {
    v: PdkValue<'event>,
    m: PdkValue<'event>,
}

impl<'event> From<ValueAndMeta<'event>> for PdkValueAndMeta<'event> {
    fn from(original: ValueAndMeta<'event>) -> Self {
        PdkValueAndMeta {
            v: original.v.into(),
            m: original.m.into(),
        }
    }
}

impl<'event> From<PdkValueAndMeta<'event>> for ValueAndMeta<'event> {
    fn from(original: PdkValueAndMeta<'event>) -> Self {
        ValueAndMeta {
            v: original.v.into(),
            m: original.m.into(),
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone, StableAbi)]
pub struct PdkEventPayload {
    /// The vector of raw input values
    raw: RVec<RArc<Pin<RVec<u8>>>>,
    data: PdkValueAndMeta<'static>,
}

impl From<EventPayload> for PdkEventPayload {
    fn from(original: EventPayload) -> Self {
        let raw = original
            .raw
            .into_iter()
            .map(|x| {
                // FIXME: this conversion could probably be simpler
                let x: RArc<Pin<RVec<u8>>> = RArc::new(Pin::new((**x).into()));
                x
            })
            .collect();
        PdkEventPayload {
            raw,
            data: original.data.into(),
        }
    }
}

impl From<PdkEventPayload> for EventPayload {
    fn from(original: PdkEventPayload) -> Self {
        let raw = original
            .raw
            .into_iter()
            .map(|x| {
                // FIXME: this conversion could probably be simpler
                let x: Arc<Pin<Vec<u8>>> = Arc::new(Pin::new((**x).into()));
                x
            })
            .collect();
        EventPayload {
            raw,
            data: original.data.into(),
        }
    }
}
