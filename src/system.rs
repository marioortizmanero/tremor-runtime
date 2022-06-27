// Copyright 2020-2021, The Tremor Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/// contains Flow definition, control plane task and lifecycle management
pub mod flow;
mod flow_supervisor;

use self::flow::Flow;
use crate::errors::{Error, Kind as ErrorKind, Result};
use crate::{connectors, QSIZE};
use async_std::channel::bounded;
use async_std::prelude::*;
use async_std::task::JoinHandle;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tremor_script::{ast, highlighter::Highlighter};

use crate::pdk::{self, ConnectorPluginRef, DEFAULT_PLUGIN_PATH};
use std::env;

/// Configuration for the runtime
pub struct WorldConfig {
    /// default size for queues
    pub qsize: usize,
    /// if debug connectors should be loaded
    pub debug_connectors: bool,
}
impl Default for WorldConfig {
    fn default() -> Self {
        Self {
            qsize: QSIZE.load(Ordering::Relaxed),
            debug_connectors: false,
        }
    }
}

/// default graceful shutdown timeout
pub const DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// default timeout for interrogating operations, like listing deployments

#[derive(Debug, PartialEq)]
/// shutdown mode - controls how we shutdown Tremor
pub enum ShutdownMode {
    /// shut down by stopping all binding instances and wait for quiescence
    Graceful,
    /// Just stop everything and not wait
    Forceful,
}

/// Tremor runtime
#[derive(Clone, Debug)]
pub struct World {
    pub(crate) system: flow_supervisor::Channel,
}

impl World {
    /// Instantiate a flow from
    /// # Errors
    /// If the flow can't be started
    pub async fn start_flow(&self, flow: &ast::DeployFlow<'static>) -> Result<()> {
        let (tx, rx) = bounded(1);
        self.system
            .send(flow_supervisor::Msg::StartDeploy {
                flow: Box::new(flow.clone()),
                sender: tx,
            })
            .await?;
        if let Err(e) = rx.recv().await? {
            let err_str = match e {
                Error(
                    ErrorKind::Script(e)
                    | ErrorKind::Pipeline(tremor_pipeline::errors::ErrorKind::Script(e)),
                    _,
                ) => {
                    let mut h = crate::ToStringHighlighter::new();
                    h.format_error(&tremor_script::errors::Error::from(e))?;
                    h.finalize()?;
                    h.to_string()
                }
                err => err.to_string(),
            };
            error!(
                "Error starting deployment of flow {}: {}",
                flow.instance_alias, &err_str
            );
            Err(ErrorKind::DeployFlowError(flow.instance_alias.clone(), err_str).into())
        } else {
            Ok(())
        }
    }

    /// Finds all the available plugins and loads them dynamically. For now,
    /// plugins are loaded from the path defined by `TREMOR_PLUGIN_PATH`.
    pub(crate) async fn register_plugin_types(&self) {
        let plugin_path =
            env::var("TREMOR_PLUGIN_PATH").unwrap_or_else(|_| String::from(DEFAULT_PLUGIN_PATH));
        // TODO: print warning when no paths are configured?
        for path in plugin_path.split(':') {
            log::info!("Dynamically loading plugins in directory '{}'", path);

            // TODO: this could be done in parallel
            for plugin in pdk::find_recursively(&path) {
                todo!()
                // match plugin {
                //     // TODO: create a `register_plugin_connector_type` function?
                //     // Or rename this into `register_connector_type`?
                //     ArtefactPlugin::Connector(p) => {
                //         self.register_builtin_connector_type(p).await?
                //     }
                // }
            }
        }
    }

    /// Registers the given connector type with `type_name` and the corresponding `builder`
    ///
    /// # Errors
    ///  * If the system is unavailable
    pub(crate) async fn register_builtin_connector_type(
        &self,
        builder: ConnectorPluginRef,
    ) -> Result<()> {
        self.system
            .send(flow_supervisor::Msg::RegisterConnectorType {
                connector_type: builder.connector_type()(),
                builder,
            })
            .await?;
        Ok(())
    }

    // METHODS EXPOSED BECAUSE API

    /// Get a flow instance address identified by `flow_id`
    ///
    /// # Errors
    ///  * if we fail to send the request or fail to receive it
    pub async fn get_flow(&self, flow_id: String) -> Result<Flow> {
        let (flow_tx, flow_rx) = bounded(1);
        let flow_id = flow::Id(flow_id);
        self.system
            .send(flow_supervisor::Msg::GetFlow(flow_id.clone(), flow_tx))
            .await?;
        flow_rx.recv().await?
    }

    /// list the currently deployed flows
    ///
    /// # Errors
    ///  * if we fail to send the request or fail to receive it
    pub async fn get_flows(&self) -> Result<Vec<Flow>> {
        let (reply_tx, reply_rx) = bounded(1);
        self.system
            .send(flow_supervisor::Msg::GetFlows(reply_tx))
            .await?;
        reply_rx.recv().await?
    }

    /// Starts the runtime system
    ///
    /// # Errors
    ///  * if the world manager can't be started
    pub async fn start(config: WorldConfig) -> Result<(Self, JoinHandle<Result<()>>)> {
        let (system_h, system) = flow_supervisor::FlowSupervisor::new(config.qsize).start();

        let world = Self { system };

        connectors::register_builtin_connector_types(&world, config.debug_connectors).await?;
        world.register_plugin_types().await;

        Ok((world, system_h))
    }

    /// Drain the runtime
    ///
    /// # Errors
    ///  * if the system failed to drain
    pub async fn drain(&self, timeout: Duration) -> Result<()> {
        let (tx, rx) = bounded(1);
        self.system.send(flow_supervisor::Msg::Drain(tx)).await?;
        if let Ok(res) = rx.recv().timeout(timeout).await {
            res??;
        } else {
            warn!("Timeout draining all Flows after {}s", timeout.as_secs());
        }
        Ok(())
    }

    /// Stop the runtime
    ///
    /// # Errors
    ///  * if the system failed to stop
    pub async fn stop(&self, mode: ShutdownMode) -> Result<()> {
        if mode == ShutdownMode::Graceful {
            if let Err(e) = self.drain(DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT).await {
                error!("Error draining all Flows: {}", e);
            }
        }
        let res = self.system.send(flow_supervisor::Msg::Stop).await;
        if let Err(e) = &res {
            error!("Error stopping all Flows: {e}");
        }
        Ok(res?)
    }
}
