pub mod connectors;

use abi_stable::std_types::SendRBoxError;

pub type RResult<T> = abi_stable::std_types::RResult<T, SendRBoxError>;

// Re-exports for ease of use
pub use tremor_value::pdk as value;
pub use tremor_script::pdk as script;
pub use tremor_pipeline::pdk as pipeline;

pub use connectors::{ConnectorMod, ConnectorMod_Ref};
