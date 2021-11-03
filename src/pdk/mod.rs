pub mod connectors;
pub mod panic;
pub mod value;

use crate::errors::Error;

use abi_stable::std_types::RBoxError;

pub use value::Value;
pub type RResult<T> = abi_stable::std_types::RResult<T, RBoxError>;
