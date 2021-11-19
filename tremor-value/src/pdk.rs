use std::pin::Pin;
use abi_stable::{
    std_types::{RBox, RCow, RHashMap, RVec, RArc},
    StableAbi,
};
use value_trait::StaticNode;

/// Representation of a JSON object
pub type Object<'value> = RHashMap<RCow<'value, str>, Value<'value>>;
/// Bytes
pub type Bytes<'value> = RCow<'value, [u8]>;

/// FFI-safe `Value` type to communicate with the plugins. It's meant to be
/// converted to/from the original `crate::Value` type and back so that it can
/// can be passed through the plugin interface. Thus, no functionality is
/// implemented other than the conversion from and to the original type.
#[repr(C)]
#[derive(Debug, Clone, StableAbi)]
pub enum Value<'value> {
    /// Static values
    Static(StaticNode),
    /// string type
    String(RCow<'value, str>),
    /// array type
    Array(RVec<Value<'value>>),
    /// object type
    Object(RBox<Object<'value>>),
    /// A binary type
    Bytes(Bytes<'value>),
}

/// Easily converting the PDK value to the original one.
impl<'value> From<crate::Value<'value>> for Value<'value> {
    fn from(original: crate::Value<'value>) -> Self {
        match original {
            // No conversion needed; `StaticNode` implements `StableAbi`
            crate::Value::Static(s) => Value::Static(s),
            // This conversion is cheap
            crate::Value::String(s) => Value::String(conv_str(s)),
            // This unfortunately requires iterating the array
            crate::Value::Array(a) => {
                let a = a.into_iter().map(Into::into).collect();
                Value::Array(a)
            }
            // This unfortunately requires iterating the map and a new
            // allocation
            crate::Value::Object(m) => {
                let m: halfbrown::HashMap<_, _> = *m;
                let m = m
                    .into_iter()
                    .map(|(k, v)| (conv_str(k), v.into()))
                    .collect();
                Value::Object(RBox::new(m))
            }
            // This conversion is cheap
            crate::Value::Bytes(b) => Value::Bytes(conv_u8(b)),
        }
    }
}

/// There are no direct conversions between `beef::Cow` and `RCow`, so the type
/// has to be converted to std as the intermediate. These conversions are cheap
/// and they shouldn't be a performance issue.
fn conv_str(cow: beef::Cow<str>) -> RCow<str> {
    let cow: std::borrow::Cow<str> = cow.into();
    cow.into()
}
fn conv_u8(cow: beef::Cow<[u8]>) -> RCow<[u8]> {
    let cow: std::borrow::Cow<[u8]> = cow.into();
    cow.into()
}

pub struct ValueAndMeta<'event> {
    v: Value<'event>,
    m: Value<'event>,
}

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
