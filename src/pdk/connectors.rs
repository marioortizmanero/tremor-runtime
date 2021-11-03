//! TODO: this should probably export a `ConnectorBuilder` instead.
//! TODO: move into a separate crate along  with the `RawConnector` trait and
//! similars.

use crate::connectors::RawConnector_TO;

use abi_stable::{
    declare_root_module_statics, package_version_strings, std_types::RBox,
    StableAbi,
    sabi_types::VersionStrings,
    library::RootModule,
};

#[repr(C)]
#[derive(StableAbi)]
#[sabi(kind(Prefix))]
pub struct ConnectorMod {
    pub new: extern "C" fn() -> RawConnector_TO<'static, RBox<()>>,
}

// Marking `MinMod` as the main module in this plugin. Note that `MinMod_Ref` is
// a pointer to the prefix of `MinMod`.
impl RootModule for ConnectorMod_Ref {
    // The name of the dynamic library
    const BASE_NAME: &'static str = "connector";
    // The name of the library for logging and similars
    const NAME: &'static str = "connector";
    // The version of this plugin's crate
    const VERSION_STRINGS: VersionStrings = package_version_strings!();

    // Implements the `RootModule::root_module_statics` function, which is the
    // only required implementation for the `RootModule` trait.
    declare_root_module_statics! {ConnectorMod_Ref}
}
