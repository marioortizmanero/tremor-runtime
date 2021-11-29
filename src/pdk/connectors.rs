use crate::{
    connectors::{BoxedRawConnector, ConnectorType},
    errors::Result,
    pdk::RResult,
};
use tremor_common::url::TremorUrl;
use tremor_value::pdk::PdkValue;

use std::path::Path;

use abi_stable::{
    declare_root_module_statics, library::RootModule, package_version_strings,
    sabi_types::VersionStrings, std_types::{ROption, RString}, StableAbi,
};
use async_ffi::FfiFuture;

/// The `new` function basically acts as the connector builder
#[repr(C)]
#[derive(StableAbi)]
#[sabi(kind(Prefix))]
pub struct ConnectorMod {
    pub connector_type: extern "C" fn() -> ConnectorType,

    #[sabi(last_prefix_field)]
    pub from_config: extern "C" fn(
        id: TremorUrl, config: ROption<RString>
    ) -> FfiFuture<RResult<BoxedRawConnector>>,
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
