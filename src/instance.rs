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

//
// Artefact instance lifecycle support and specializations for the
// different artefact types
//

use std::fmt::Display;

/// Possible lifecycle states of an instance
#[repr(c)]
#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize, StableAbi)]
#[serde(rename_all = "lowercase")]
pub enum InstanceState {
    /// initialized - first state after coming to life
    Initialized,
    /// Running and consuming/producing/handling events
    Running,
    /// Paused, not consuming/producing/handling events
    Paused,
    /// Drained - flushing out all the pending events
    Draining,
    /// Stopped, final state
    Stopped,
    /// failed to start
    Failed,
}

impl InstanceState {
    /// checks if the state is stopped
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        *self == InstanceState::Stopped
    }
}

impl Display for InstanceState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Initialized => "initialized",
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Draining => "draining",
            Self::Stopped => "stopped",
            Self::Failed => "failed",
        })
    }
}
