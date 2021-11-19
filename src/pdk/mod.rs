pub mod connectors;

use abi_stable::std_types::SendRBoxError;

pub type RResult<T> = abi_stable::std_types::RResult<T, SendRBoxError>;
