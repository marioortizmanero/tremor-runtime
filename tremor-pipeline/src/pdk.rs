use std::pin::Pin;
use abi_stable::{StableAbi, std_types::{RVec, RArc}};
use tremor_value::pdk;

#[repr(C)]
#[derive(StableAbi)]
pub struct ValueAndMeta<'event> {
    v: pdk::Value<'event>,
    m: pdk::Value<'event>,
}

#[repr(C)]
#[derive(StableAbi)]
pub struct EventPayload {
    /// The vector of raw input values
    raw: RVec<RArc<Pin<RVec<u8>>>>,
    data: ValueAndMeta<'static>,
}

impl From<tremor_script::EventPayload> for EventPayload {
    fn from(original: tremor_script::EventPayload) -> Self {
        EventPayload {
            raw: original.raw.into_iter().map(|x| {
                x.into_iter().map(|y| {
                    y.into()
                })
            }).collect(),
            data: ValueAndMeta {
                v: original.data.value().into(),
                m: original.data.meta().into()
            }
        }
    }
}

