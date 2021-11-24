pub mod connectors;

pub type RError = abi_stable::std_types::SendRBoxError;
pub type RResult<T> = abi_stable::std_types::RResult<T, RError>;
