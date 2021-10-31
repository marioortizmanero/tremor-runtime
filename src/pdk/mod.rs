pub mod connectors;
pub mod panic;

use crate::errors::Error;

use abi_stable::std_types::RBoxError;

pub type RResult<T> = abi_stable::std_types::RResult<T, RBoxError>;
