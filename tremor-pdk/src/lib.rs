/// Wrappers for `tremor_pipeline`
pub mod pipeline;
/// Wrappers for `tremor_script`
pub mod script;
/// Wrappers for `tremor_value`
pub mod value;
/// Plugin interfaces for each supported artefact
pub mod artefacts;

pub type RError = abi_stable::std_types::SendRBoxError;
pub type RResult<T> = abi_stable::std_types::RResult<T, RError>;

// FIXME: this should be reorganized after moving stuff to this crate
pub mod reexports {
    pub use tremor_runtime::utils::{metrics, quiescence, reconnect, hostname};
    pub use tremor_common::{time::nanotime, url::TremorUrl};
    pub use tremor_pipeline::DEFAULT_STREAM_ID;
    pub use tremor_script::EventPayload;
    pub use tremor_value::literal;
}
