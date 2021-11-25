//! Exports the plugin with `abi_stable`'s procedure.

mod connector;

use abi_stable::{export_root_module, prefix_type::PrefixTypeTrait};
use tremor_runtime::pdk::connectors::{ConnectorMod, ConnectorMod_Ref};

/// Exports the root module of this library.
///
/// This code isn't run until the layout of the type it returns is checked.
#[export_root_module]
fn instantiate_root_module() -> ConnectorMod_Ref {
    // Converts the `ConnectorMod` into `ConnectorMod_Ref` and leaks it
    ConnectorMod {
        connector_type: connector::connector_type,
        from_config: connector::from_config,
    }
    .leak_into_prefix()
}
