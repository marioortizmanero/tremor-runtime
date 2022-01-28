// Copyright 2020-2021, The Tremor Team
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

use crate::Value;
use std::fmt;
use value_trait::{Mutable, Value as ValueTrait, ValueAccess, ValueType};

use crate::value::from::cow_beef_to_sabi;
use abi_stable::std_types::{RCow, RHashMap};

/// Well known key that can be looked up in a `Value` faster.
/// It achives this by memorizing the hash.
#[derive(Debug, Clone, PartialEq)]
pub struct KnownKey<'key> {
    key: RCow<'key, str>,
    // FIXME: temporarily removed to enable PDK support
    // hash: u64,
}

/// Error for known keys
#[derive(Debug, PartialEq, Clone)]
pub enum Error {
    /// The target passed wasn't an object
    NotAnObject(ValueType),
}

#[cfg(not(tarpaulin_include))]
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::NotAnObject(t) => write!(f, "Expected object but got {:?}", t),
        }
    }
}
impl std::error::Error for Error {}

impl<'key> From<RCow<'key, str>> for KnownKey<'key> {
    fn from(key: RCow<'key, str>) -> Self {
        // FIXME: temporarily removed to enable PDK support
        // let hash_builder = halfbrown::DefaultHashBuilder::default();
        // let mut hasher = hash_builder.build_hasher();
        // key.hash(&mut hasher);
        Self {
            // hash: hasher.finish(),
            key,
        }
    }
}
impl<'key> From<beef::Cow<'key, str>> for KnownKey<'key> {
    fn from(key: beef::Cow<'key, str>) -> Self {
        // FIXME: temporarily removed to enable PDK support
        // let hash_builder = halfbrown::DefaultHashBuilder::default();
        // let mut hasher = hash_builder.build_hasher();
        // key.hash(&mut hasher);
        Self {
            // hash: hasher.finish(),
            key: cow_beef_to_sabi(key),
        }
    }
}

impl<'key> KnownKey<'key> {
    /// The known key
    #[inline]
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Looks up this key in a `Value`, returns None if the
    /// key wasn't present or `target` isn't an object
    ///
    /// ```rust
    /// use tremor_value::prelude::*;
    /// let object = literal!({
    ///   "answer": 42,
    ///   "key": 7
    /// });
    /// let known_key = KnownKey::from("answer");
    /// assert_eq!(known_key.lookup(&object).unwrap(), &42);
    /// ```
    #[inline]
    #[must_use]
    pub fn lookup<'target>(&self, target: &'target Value<'key>) -> Option<&'target Value<'key>>
    where
        'key: 'target,
    {
        target.as_object().and_then(|m| self.map_lookup(m))
    }
    /// Looks up this key in a `HashMap<<Cow<'key, str>, Value<'key>>` the inner representation of an object `Value`, returns None if the
    /// key wasn't present.
    ///
    /// ```rust
    /// use tremor_value::prelude::*;
    /// let object = literal!({
    ///   "answer": 42,
    ///   "key": 7
    /// });
    /// let known_key = KnownKey::from("answer");
    /// if let Some(inner) = object.as_object() {
    ///   assert_eq!(known_key.map_lookup(inner).unwrap(), &42);
    /// }
    /// ```

    #[inline]
    #[must_use]
    pub fn map_lookup<'target>(
        &self,
        map: &'target RHashMap<RCow<'key, str>, Value<'key>>,
    ) -> Option<&'target Value<'key>>
    where
        'key: 'target,
    {
        // FIXME: temporarily removed to enable PDK support
        // map.raw_entry()
        //     .from_key_hashed_nocheck(self.hash, self.key())
        //     .map(|kv| kv.1)
        map.get(self.key())
    }

    /// Looks up this key in a `Value`, returns None if the
    /// key wasn't present or `target` isn't an object
    ///
    /// ```rust
    /// use tremor_value::prelude::*;
    /// let mut object = literal!({
    ///   "answer": 23,
    ///   "key": 7
    /// });
    /// let known_key = KnownKey::from("answer");
    ///
    /// assert_eq!(object["answer"], 23);
    ///
    /// if let Some(answer) = known_key.lookup_mut(&mut object) {
    ///   *answer = Value::from(42);
    /// }
    ///
    /// assert_eq!(object["answer"], 42);
    /// ```
    #[inline]
    pub fn lookup_mut<'target>(
        &self,
        target: &'target mut Value<'key>,
    ) -> Option<&'target mut Value<'key>>
    where
        'key: 'target,
    {
        target.as_object_mut().and_then(|m| self.map_lookup_mut(m))
    }

    /// Looks up this key in a `HashMap<Cow<'key, str>, Value<'key>>`, the inner representation of an object value.
    /// returns None if the key wasn't present.
    ///
    /// ```rust
    /// use tremor_value::prelude::*;
    /// let mut object = literal!({
    ///   "answer": 23,
    ///   "key": 7
    /// });
    ///
    /// assert_eq!(object["answer"], 23);
    ///
    /// let known_key = KnownKey::from("answer");
    /// if let Some(inner) = object.as_object_mut() {
    ///   if let Some(answer) = known_key.map_lookup_mut(inner) {
    ///     *answer = Value::from(42);
    ///   }
    /// }
    /// assert_eq!(object["answer"], 42);
    ///
    /// ```
    #[inline]
    pub fn map_lookup_mut<'target>(
        &self,
        map: &'target mut RHashMap<RCow<'key, str>, Value<'key>>,
    ) -> Option<&'target mut Value<'key>>
    where
        'key: 'target,
    {
        // FIXME: temporarily removed to enable PDK support
        // match map
        //     .raw_entry_mut()
        //     .from_key_hashed_nocheck(self.hash, &self.key)
        // {
        //     RawEntryMut::Occupied(e) => Some(e.into_mut()),
        //     RawEntryMut::Vacant(_e) => None,
        // }
        map.get_mut(self.key())
    }

    /// Looks up this key in a `Value`, inserts `with` when the key
    ///  when wasn't present returns None if the `target` isn't an object
    ///
    /// # Errors
    /// - if the value isn't an object
    ///
    /// ```rust
    /// use tremor_value::prelude::*;
    /// let mut object = literal!({
    ///   "answer": 23,
    ///   "key": 7
    /// });
    /// let known_key = KnownKey::from("answer");
    ///
    /// assert_eq!(object["answer"], 23);
    ///
    /// if let Ok(answer) = known_key.lookup_or_insert_mut(&mut object, || 17.into()) {
    ///   assert_eq!(*answer, 23);
    ///   *answer = Value::from(42);
    /// }
    ///
    /// assert_eq!(object["answer"], 42);
    ///
    /// let known_key2 = KnownKey::from("also the answer");
    /// if let Ok(answer) = known_key2.lookup_or_insert_mut(&mut object, || 8.into()) {
    ///   assert_eq!(*answer, 8);
    ///   *answer = Value::from(42);
    /// }
    ///
    /// assert_eq!(object["also the answer"], 42);
    /// ```
    #[inline]
    pub fn lookup_or_insert_mut<'target, F>(
        &self,
        target: &'target mut Value<'key>,
        with: F,
    ) -> Result<&'target mut Value<'key>, Error>
    where
        'key: 'target,
        F: FnOnce() -> Value<'key>,
    {
        // we make use of internals here, but this is the fastest way, only requiring one match
        match target {
            Value::Object(m) => Ok(self.map_lookup_or_insert_mut(m, with)),
            other => Err(Error::NotAnObject(other.value_type())),
        }
    }

    /// Looks up this key in a `HashMap<Cow<'key, str>, Value<'key>>`, the inner representation of an object `Value`.
    /// Inserts `with` when the key when wasn't present.
    ///
    /// ```rust
    /// use tremor_value::prelude::*;
    /// let mut object = literal!({
    ///   "answer": 23,
    ///   "key": 7
    /// });
    /// let known_key = KnownKey::from("answer");
    ///
    /// assert_eq!(object["answer"], 23);
    ///
    /// if let Some(inner) = object.as_object_mut() {
    ///   let answer = known_key.map_lookup_or_insert_mut(inner, || 17.into());
    ///   assert_eq!(*answer, 23);
    ///   *answer = Value::from(42);
    /// }
    ///
    /// assert_eq!(object["answer"], 42);
    ///
    /// let known_key2 = KnownKey::from("also the answer");
    /// if let Some(inner) = object.as_object_mut() {
    ///   let answer = known_key2.map_lookup_or_insert_mut(inner, || 8.into());
    ///   assert_eq!(*answer, 8);
    ///   *answer = Value::from(42);
    /// }
    ///
    /// assert_eq!(object["also the answer"], 42);
    /// ```
    #[inline]
    pub fn map_lookup_or_insert_mut<'target, F>(
        &self,
        map: &'target mut RHashMap<RCow<'key, str>, Value<'key>>,
        with: F,
    ) -> &'target mut Value<'key>
    where
        'key: 'target,
        F: FnOnce() -> Value<'key>,
    {
        // FIXME: temporarily removed to enable PDK support
        // let key: &str = &self.key;
        // map.raw_entry_mut()
        //     .from_key_hashed_nocheck(self.hash, key)
        //     .or_insert_with(|| (self.key.clone(), with()))
        //     .1
        //
        //   match map.get_mut(self.key()) {
        //       Some(v) => v,
        //       None => {
        //           let mut v = with();
        //           map.insert(self.key, v);
        //           &mut v
        //       }
        //   }
        map.entry(self.key.clone()).or_insert_with(with)
    }

    /// Inserts a value key into  `Value`, returns None if the
    /// key wasn't present otherwise Some(`old value`).
    ///
    /// # Errors
    /// - if `target` isn't an object
    ///
    /// ```rust
    /// use tremor_value::prelude::*;
    /// let mut object = literal!({
    ///   "answer": 23,
    ///   "key": 7
    /// });
    /// let known_key = KnownKey::from("answer");
    ///
    /// assert_eq!(object["answer"], 23);
    ///
    /// assert!(known_key.insert(&mut object, Value::from(42)).is_ok());
    ///
    /// assert_eq!(object["answer"], 42);
    ///
    /// let known_key2 = KnownKey::from("also the answer");
    ///
    /// assert!(known_key2.insert(&mut object, Value::from(42)).is_ok());
    ///
    /// assert_eq!(object["also the answer"], 42);
    /// ```
    #[inline]
    pub fn insert<'target>(
        &self,
        target: &'target mut Value<'key>,
        value: Value<'key>,
    ) -> Result<Option<Value<'key>>, Error>
    where
        'key: 'target,
    {
        target
            .as_object_mut()
            .map(|m| self.map_insert(m, value))
            .ok_or_else(|| Error::NotAnObject(target.value_type()))
    }

    /// Inserts a value key into `map`, returns None if the
    /// key wasn't present otherwise Some(`old value`).
    ///
    /// ```rust
    /// use tremor_value::prelude::*;
    ///
    /// let mut object = literal!({
    ///   "answer": 23,
    ///   "key": 7
    /// });
    /// let known_key = KnownKey::from("answer");
    ///
    /// assert_eq!(object["answer"], 23);
    ///
    /// if let Some(inner) = object.as_object_mut() {
    ///   assert!(known_key.map_insert(inner, Value::from(42)).is_some());
    /// }
    ///
    /// assert_eq!(object["answer"], 42);
    ///
    /// let known_key2 = KnownKey::from("also the answer");
    ///
    /// if let Some(inner) = object.as_object_mut() {
    ///   assert!(known_key2.map_insert(inner, Value::from(42)).is_none());
    /// }
    ///
    /// assert_eq!(object["also the answer"], 42);
    /// ```
    #[inline]
    pub fn map_insert<'target>(
        &self,
        map: &'target mut RHashMap<RCow<'key, str>, Value<'key>>,
        value: Value<'key>,
    ) -> Option<Value<'key>>
    where
        'key: 'target,
    {
        // FIXME: temporarily removed to enable PDK support
        // match map
        //     .raw_entry_mut()
        //     .from_key_hashed_nocheck(self.hash, self.key())
        // {
        //     RawEntryMut::Occupied(mut e) => Some(e.insert(value)),
        //     RawEntryMut::Vacant(e) => {
        //         e.insert_hashed_nocheck(self.hash, self.key.clone(), value);
        //         None
        //     }
        // }
        map.insert(self.key.clone(), value).into()
    }
}

impl<'script> KnownKey<'script> {
    /// turns the key into one with static lifetime
    #[must_use]
    pub fn into_static(self) -> KnownKey<'static> {
        let KnownKey { key, /*, hash*/ } = self;
        KnownKey {
            key: RCow::Owned(key.to_string().into()),
            // hash,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unnecessary_operation, clippy::non_ascii_literal)]
    use super::*;
    use value_trait::Builder;

    use abi_stable::std_types::RCow;

    #[test]
    fn known_key() {
        let mut v = Value::object();
        v.try_insert("key", 1);
        let key1 = KnownKey::from(RCow::from("key"));
        let key2 = KnownKey::from(RCow::from("cake"));

        assert!(key1.lookup(&Value::null()).is_none());
        assert!(key2.lookup(&Value::null()).is_none());
        assert!(key1.lookup(&v).is_some());
        assert!(key2.lookup(&v).is_none());
        assert!(key1.lookup_mut(&mut v).is_some());
        assert!(key2.lookup_mut(&mut v).is_none());
    }

    #[test]
    fn known_key_insert() {
        let mut v = Value::object();
        v.try_insert("key", 1);
        let key1 = KnownKey::from(RCow::from("key"));
        let key2 = KnownKey::from(RCow::from("cake"));

        let mut v1 = Value::null();
        assert!(key1.insert(&mut v1, 2.into()).is_err());
        assert!(key2.insert(&mut v1, 2.into()).is_err());
        assert_eq!(key1.insert(&mut v, 2.into()).unwrap(), Some(1.into()));
        assert_eq!(key2.insert(&mut v, 3.into()).unwrap(), None);
        assert_eq!(v["key"], 2);
        assert_eq!(v["cake"], 3);
    }

    #[test]
    fn lookup_or_insert_mut() {
        let mut v = Value::object();
        v.try_insert("key", 1);
        let key1 = KnownKey::from(RCow::from("key"));
        let key2 = KnownKey::from(RCow::from("cake"));

        let mut v1 = Value::null();
        assert!(key1.lookup_or_insert_mut(&mut v1, || 2.into()).is_err());
        assert!(key2.lookup_or_insert_mut(&mut v1, || 2.into()).is_err());

        {
            let r1 = key1.lookup_or_insert_mut(&mut v, || 2.into()).unwrap();
            assert_eq!(r1.as_u8(), Some(1));
        }
        {
            let r2 = key2.lookup_or_insert_mut(&mut v, || 3.into()).unwrap();
            assert_eq!(r2.as_u8(), Some(3));
        }
    }
    #[test]
    fn known_key_map() {
        let mut v = Value::object_with_capacity(128);
        v.try_insert("key", 1);
        let key1 = KnownKey::from(RCow::from("key"));
        let key2 = KnownKey::from(RCow::from("cake"));

        assert!(key1.lookup(&Value::null()).is_none());
        assert!(key2.lookup(&Value::null()).is_none());
        assert!(key1.lookup(&v).is_some());
        assert!(key2.lookup(&v).is_none());
    }

    #[test]
    fn known_key_insert_map() {
        let mut v = Value::object_with_capacity(128);
        v.try_insert("key", 1);
        let key1 = KnownKey::from(RCow::from("key"));
        let key2 = KnownKey::from(RCow::from("cake"));

        let mut v1 = Value::null();

        assert!(key1.insert(&mut v1, 2.into()).is_err());
        assert!(key2.insert(&mut v1, 2.into()).is_err());
        assert_eq!(key1.insert(&mut v, 2.into()).unwrap(), Some(1.into()));
        assert_eq!(key2.insert(&mut v, 3.into()).unwrap(), None);
        assert_eq!(v["key"], 2);
        assert_eq!(v["cake"], 3);
    }

    #[test]
    fn known_key_get_key() {
        let key1 = KnownKey::from("snot").into_static();
        assert_eq!(key1.key(), "snot");
    }
}
