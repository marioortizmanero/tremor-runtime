pub mod connectors;
mod panic;
mod value;

use abi_stable::std_types::RBoxError;

pub use panic::MayPanic;
pub use value::Value;
pub type RResult<T> = abi_stable::std_types::RResult<T, RBoxError>;
