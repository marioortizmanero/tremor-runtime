pub mod connectors;

pub type RError = abi_stable::std_types::SendRBoxError;
pub type RResult<T> = abi_stable::std_types::RResult<T, RError>;

/// Wrappers for `tremor_pipeline`
pub use tremor_pipeline::pdk as pipeline;
/// Wrappers for `tremor_script`
pub use tremor_script::pdk as script;
/// Wrappers for `tremor_value`
pub use tremor_value::pdk as value;
