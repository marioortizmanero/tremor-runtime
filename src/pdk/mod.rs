pub mod connectors;

pub type RError = abi_stable::std_types::SendRBoxError;
pub type RResult<T> = abi_stable::std_types::RResult<T, RError>;

use self::connectors::ConnectorMod_Ref;

use std::env;

use abi_stable::library::RootModule;
use walkdir::WalkDir;

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
            ::std::result::Result::Err(err) => {
                return ::abi_stable::std_types::RResult::RErr(err.into())
            }
        }
    };
}

pub fn find_recursively(base_dir: &str) -> Vec<ConnectorMod_Ref> {
    WalkDir::new(base_dir)
        // No symlinks are followed for now
        .follow_links(false)
        // Adding some safe limits
        .max_depth(1000)
        .into_iter()
        // Ignoring permission errors
        .filter_map(Result::ok)
        // Only try to load those that look like plugins on the current platform
        .filter(|file| {
            file.path()
                .extension()
                .map(|ext| ext == env::consts::DLL_EXTENSION)
                .unwrap_or(false)
        })
        // Try to load the plugins and if successful, add them to the result
        .filter_map(|file| {
            match ConnectorMod_Ref::load_from_file(file.path()) {
                Ok(plugin) => Some(plugin),
                Err(e) => {
                    // TODO: log the error
                    None
                }
            }
        })
        .collect()
}
