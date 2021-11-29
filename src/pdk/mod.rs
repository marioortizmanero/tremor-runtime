pub mod connectors;

pub type RError = abi_stable::std_types::SendRBoxError;
pub type RResult<T> = abi_stable::std_types::RResult<T, RError>;

/// This is a workaround until `?` can be used with functions that return
/// `RResult`: https://github.com/rust-lang/rust/issues/84277
///
/// NOTE: this might be less 'magic' by matching to `RResult` instead of
/// `Result`, but it also saves us a very common `.into()`.
#[macro_export]
macro_rules! ttry {
    ($e:expr) => {
        match $e {
            ::std::result::Result::Ok(val) => val,
            ::std::result::Result::Err(err) => return ::abi_stable::std_types::RResult::RErr(err.into()),
        }
    };
}

pub fn load_recursively(path: &str) {

}
