// Copyright 2021, The Tremor Team
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

use abi_stable::std_types::RBox;
use event_listener::Event;
use std::fmt;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

#[derive(Debug)]
struct Inner {
    state: AtomicU32,
    resume_event: Event,
}
impl Inner {
    // different states of the beacon
    const RUNNING: u32 = 0x0;
    const PAUSED: u32 = 0x1;
    const STOP_READING: u32 = 0x2;
    const STOP_ALL: u32 = 0x4;
}

impl Default for Inner {
    fn default() -> Self {
        Self {
            state: AtomicU32::new(Self::RUNNING),
            resume_event: Event::new(),
        }
    }
}
/// use this beacon to check if tasks reading or writing from external connections should stop
#[derive(Debug, Clone, Default)]
#[allow(clippy::module_name_repetitions)]
pub struct QuiescenceBeacon(Arc<Inner>);

/// `QuiescenceBeacon` is used for the plugin system, so it must be `#[repr(C)]`
/// in order to interact with it. However, since it uses complex types
/// internally, it's easier to just make it available as an opaque type instead,
/// with the help of `sabi_trait`.
#[abi_stable::sabi_trait]
pub trait QuiescenceBeaconOpaque: fmt::Debug + Clone + Send + Sync {
    // TODO: async
    /* async */
    fn continue_reading(&self) -> bool;
    /* async */
    fn continue_writing(&self) -> bool;
    fn stop_reading(&mut self);
    fn pause(&mut self);
    fn resume(&mut self);
    fn full_stop(&mut self);
}
impl QuiescenceBeacon {
    // we have max 2 listeners at a time, checking this beacon
    // the sink and the source of the connector
    const MAX_LISTENERS: usize = 2;
}
impl QuiescenceBeaconOpaque for QuiescenceBeacon {
    /// returns `true` if consumers should continue reading
    /// doesn't return until the beacon is unpaused
    fn continue_reading(&self) -> bool {
        loop {
            match self.0.state.load(Ordering::Acquire) {
                Inner::RUNNING => break true,
                Inner::PAUSED => {
                    // we wait to be notified
                    // if so, we re-enter the loop to check the new state
                    self.0.resume_event.listen();
                }
                _ => break false, // STOP_ALL | STOP_READING | _
            }
        }
    }

    /// returns `true` if consumers should continue writing.
    ///
    fn continue_writing(&self) -> bool {
        loop {
            match self.0.state.load(Ordering::Acquire) {
                Inner::RUNNING | Inner::STOP_READING => break true,
                Inner::PAUSED => {
                    self.0.resume_event.listen();
                }
                _ => break false, // STOP_ALL | _
            }
        }
    }

    /// notify consumers of this beacon that reading should be stopped
    fn stop_reading(&mut self) {
        self.0.state.store(Inner::STOP_READING, Ordering::Release);
        self.0.resume_event.notify(Self::MAX_LISTENERS); // we might have been paused, so notify here
    }

    /// pause both reading and writing
    fn pause(&mut self) {
        let _ = self.0.state.compare_exchange(
            Inner::RUNNING,
            Inner::PAUSED,
            Ordering::AcqRel,
            Ordering::Relaxed,
        );
    }

    /// Resume both reading and writing.
    ///
    /// Has no effect if not currently paused.
    fn resume(&mut self) {
        let _ = self.0.state.compare_exchange(
            Inner::PAUSED,
            Inner::RUNNING,
            Ordering::AcqRel,
            Ordering::Relaxed,
        );
        self.0.resume_event.notify(Self::MAX_LISTENERS); // we might have been paused, so notify here
    }

    /// notify consumers of this beacon that reading and writing should be stopped
    fn full_stop(&mut self) {
        self.0.state.store(Inner::STOP_ALL, Ordering::Release);
        self.0.resume_event.notify(Self::MAX_LISTENERS); // we might have been paused, so notify here
    }
}
/// Alias for the type used in FFI
pub type BoxedQuiescenceBeacon = QuiescenceBeaconOpaque_TO<'static, RBox<()>>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::Result;
    use async_std::prelude::*;
    use std::time::Duration;

    #[async_std::test]
    async fn quiescence_pause_resume() -> Result<()> {
        let mut beacon = QuiescenceBeacon::default();
        assert!(beacon.continue_reading().await);
        assert!(beacon.continue_writing().await);

        beacon.pause();

        let timeout_ms = Duration::from_millis(50);
        // no progress for reading while being paused
        assert!(beacon.continue_reading().timeout(timeout_ms).await.is_err());
        // no progress for writing while being paused
        assert!(beacon.continue_writing().timeout(timeout_ms).await.is_err());

        beacon.resume();
        assert!(beacon.continue_reading().timeout(timeout_ms).await?);
        assert!(beacon.continue_writing().timeout(timeout_ms).await?);

        beacon.stop_reading();
        // don't continue reading when stopped reading
        assert_eq!(false, beacon.continue_reading().timeout(timeout_ms).await?);
        // writing is fine
        assert!(beacon.continue_writing().timeout(timeout_ms).await?);

        // a resume after stopping reading has no effect
        beacon.resume();
        // don't continue reading when stopped reading
        assert!(!beacon.continue_reading().timeout(timeout_ms).await?);
        // writing is still fine
        assert!(beacon.continue_writing().timeout(timeout_ms).await?);

        beacon.full_stop();
        // no reading upon full stop
        assert!(!beacon.continue_reading().timeout(timeout_ms).await?);
        // no writing upon full stop
        assert!(!beacon.continue_writing().timeout(timeout_ms).await?);

        // a resume after a full stop has no effect
        beacon.resume();
        // no reading upon full stop
        assert!(!beacon.continue_reading().timeout(timeout_ms).await?);
        // no writing upon full stop
        assert!(!beacon.continue_writing().timeout(timeout_ms).await?);
        Ok(())
    }
}
