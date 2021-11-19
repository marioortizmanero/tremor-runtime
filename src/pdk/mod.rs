pub mod connectors;

use abi_stable::std_types::RBoxError;

pub type RResult<T> = abi_stable::std_types::RResult<T, RBoxError>;
