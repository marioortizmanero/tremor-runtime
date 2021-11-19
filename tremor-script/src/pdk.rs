//! FFI-safe types to communicate with the plugins. They're meant to be
//! converted to/from the original type and back so that it can be passed
//! through the plugin interface. Thus, no functionality is implemented other
//! than the conversion from and to the original type.

use abi_stable::{StableAbi, std_types::{RVec, RArc}};

#[repr(C)]
#[derive(StableAbi)]
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

#[repr(C)]
#[derive(Clone, Default, StableAbi)]
pub struct EventPayload {
    /// The vector of raw input values
    raw: RVec<RArc<Pin<RVec<u8>>>>,
    data: ValueAndMeta<'static>,
}

impl From<crate::EventPayload> for EventPayload {
    fn from(original: crate::EventPayload) -> Self {
        EventPayload {
            raw: original.raw.into(),
            data: original.data.into,
        }
    }
}
