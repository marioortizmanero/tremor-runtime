pub mod connectors;
mod value;

use abi_stable::std_types::RBoxError;

pub use value::Value;
pub type RResult<T> = abi_stable::std_types::RResult<T, RBoxError>;
