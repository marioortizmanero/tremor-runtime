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

use crate::config::{BindingVec, Config, MappingMap};
use crate::connectors::utils::metrics::METRICS_CHANNEL;
use crate::errors::{Error, ErrorKind, Result};
use crate::pdk::connectors::ConnectorMod_Ref;
use crate::registry::Registries;
use crate::repository::{
    Artefact, BindingArtefact, ConnectorArtefact, PipelineArtefact, Repositories,
};

use crate::connectors::{self, ConnectorBuilder};
use crate::errors::{Error, Kind as ErrorKind, Result};
use crate::QSIZE;
use async_std::channel::{bounded, Sender};
use async_std::prelude::*;
use async_std::task::{self, JoinHandle};
use deployment::{Deployment, DeploymentId};
use hashbrown::{hash_map::Entry, HashMap};
use std::sync::atomic::Ordering;
use std::time::Duration;
use tremor_common::ids::{ConnectorIdGen, OperatorIdGen};
use tremor_script::{highlighter::Highlighter, srs::DeployFlow};

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

#[derive(Debug, PartialEq)]
/// shutdown mode - controls how we shutdown Tremor
pub enum ShutdownMode {
    /// shut down by stopping all binding instances and wait for quiescence
    Graceful,
    /// Just stop everything and not wait
    Forceful,
}

/// This is control plane
pub(crate) enum ManagerMsg {
    ///add a deployment
    StartDeploy {
        /// deploy source
        src: String,
        /// deploy flow
        flow: DeployFlow,
        /// result sender
        sender: Sender<Result<()>>,
    },
    RegisterConnectorType {
        /// the type of connector
        connector_type: ConnectorType,
        /// the builder
        builder: Box<dyn ConnectorBuilder>,
    },
    /// Initiate the Quiescence process
    Drain(Sender<Result<()>>),
    /// stop this manager
    Stop,
}
use async_std::channel::Sender as AsyncSender;

use self::connectors::{ConnectorType, KnownConnectors};
pub(crate) type ManagerSender = AsyncSender<ManagerMsg>;

#[derive(Debug)]
pub(crate) struct Manager {
    deployments: HashMap<DeploymentId, Deployment>,
    operator_id_gen: OperatorIdGen,
    connector_id_gen: ConnectorIdGen,
    known_connectors: KnownConnectors,
    pub qsize: usize,
}

impl Manager {
    pub fn start(mut self) -> (JoinHandle<Result<()>>, ManagerSender) {
        let (tx, rx) = bounded(self.qsize);
        let system_h = task::spawn(async move {
            while let Ok(msg) = rx.recv().await {
                match msg {
                    ManagerMsg::RegisterConnectorType {
                        connector_type,
                        builder,
                        ..
                    } => {
                        if let Some(old) = self
                            .known_connectors
                            .insert(connector_type.clone(), builder)
                        {
                            error!(
                                "FIXME: error on duplicate connectors: {}",
                                old.connector_type()
                            );
                        }
                    }
                    ManagerMsg::StartDeploy { src, flow, sender } => {
                        let id = DeploymentId::from(&flow);
                        let res = match self.deployments.entry(id.clone()) {
                            Entry::Occupied(_occupied) => {
                                Err(ErrorKind::DuplicateFlow(id.0.clone()).into())
                            }
                            Entry::Vacant(vacant) => {
                                let res = Deployment::start(
                                    src.clone(),
                                    flow,
                                    &mut self.operator_id_gen,
                                    &mut self.connector_id_gen,
                                    &self.known_connectors,
                                )
                                .await;
                                match res {
                                    Ok(deploy) => {
                                        vacant.insert(deploy);
                                        Ok(())
                                    }
                                    Err(e) => Err(e),
                                }
                            }
                        };
                        if sender.send(res).await.is_err() {
                            error!("Error sending StartDeploy Err Result");
                        }
                    }
                    ManagerMsg::Stop => {
                        info!("Stopping Manager ...");
                        let num_deployments = self.deployments.len();
                        if num_deployments > 0 {
                            // send stop to each deployment
                            let (tx, rx) = bounded(self.deployments.len());
                            let mut expected_stops = self.deployments.len();
                            for (_, deployment) in self.deployments {
                                deployment.stop(tx.clone()).await?;
                            }
                            let h = task::spawn::<_, Result<()>>(async move {
                                while expected_stops > 0 {
                                    let res = rx.recv().await?;
                                    if let Err(e) = res {
                                        error!("Error during Stopping: {}", e);
                                    }
                                    expected_stops = expected_stops.saturating_sub(1);
                                }
                                Ok(())
                            });
                            h.timeout(DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT).await??;
                        }
                        break;
                    }
                    ManagerMsg::Drain(sender) => {
                        let num_deployments = self.deployments.len();
                        if num_deployments == 0 {
                            sender.send(Ok(())).await?;
                        } else {
                            info!("Draining all Flows ...");
                            let (tx, rx) = bounded(num_deployments);
                            for (_, deployment) in &self.deployments {
                                deployment.drain(tx.clone()).await?;
                            }

                            task::spawn::<_, Result<()>>(async move {
                                let rx_futures =
                                    std::iter::repeat_with(|| rx.recv()).take(num_deployments);
                                for result in futures::future::join_all(rx_futures).await {
                                    match result {
                                        Err(_) => {
                                            error!("Error receiving from Draining process.");
                                        }
                                        Ok(Err(e)) => {
                                            error!("Error during Draining: {}", e);
                                        }
                                        Ok(Ok(())) => {}
                                    }
                                }
                                info!("Flows drained.");
                                sender.send(Ok(())).await?;
                                Ok(())
                            });
                        }
                    }
                }
            }
            info!("Manager stopped.");
            Ok(())
        });
        (system_h, tx)
    }
}

/// Tremor runtime
#[derive(Clone, Debug)]
pub struct World {
    pub(crate) system: ManagerSender,
}

impl World {
    /// Instantiate a flow from
    pub(crate) async fn start_deploy(&self, src: &str, flow: &DeployFlow) -> Result<()> {
        let (tx, rx) = bounded(1);
        self.system
            .send(ManagerMsg::StartDeploy {
                src: src.to_string(),
                flow: flow.clone(),
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
                    tremor_script::query::Query::format_error_from_script(
                        &src,
                        &mut h,
                        &tremor_script::errors::Error::from(e),
                    )?;
                    h.finalize()?;
                    h.to_string()
                }
                err => err.to_string(),
            };
            error!(
                "Error starting deployment of flow {}: {}",
                flow.instance_id.id(),
                &err_str
            );
            Err(ErrorKind::DeployFlowError(flow.instance_id.id().to_string(), err_str).into())
        } else {
            Ok(())
        }
    }
    /// Registers the given connector type with `type_name` and the corresponding `builder`
    ///
    /// # Errors
    ///  * If the system is unavailable
    pub(crate) async fn register_builtin_connector_type(
        &self,
        builder: ConnectorMod_Ref,
    ) -> Result<()> {
        self.system
            .send(ManagerMsg::Connector(connectors::ManagerMsg::Register {
                connector_type: builder.connector_type()(),
                builder,
            })
            .await?;
        Ok(())
    }
    /// Registers the given connector type with `type_name` and the corresponding `builder`
    ///
    /// # Errors
    ///  * If the system is unavailable
    pub async fn register_connector_type(&self, builder: ConnectorMod_Ref) -> Result<()> {
        self.system
            .send(ManagerMsg::Connector(connectors::ManagerMsg::Register {
                connector_type: builder.connector_type()(),
                builder,
                builtin: false,
            }))
            .await?;
        Ok(())
    }

    /// unregister a connector type
    ///
    /// # Errors
    ///  * If the system is unavailable
    pub async fn unregister_connector_type(&self, type_name: String) -> Result<()> {
        self.system
            .send(ManagerMsg::Connector(connectors::ManagerMsg::Unregister(
                type_name.into(),
            )))
            .await?;
        Ok(())
    }

    /// Ensures the existance of a pipeline instance, creating it if required.
    ///
    /// # Errors
    ///  * if we can't ensure the pipeline is bound
    pub async fn ensure_pipeline(&self, id: &TremorUrl) -> Result<()> {
        if self.reg.find_pipeline(id).await?.is_none() {
            info!(
                "Pipeline instance {} not found, create a new instance with that id.",
                &id
            );
            self.create_pipeline_instance(id).await?;
        } else {
            info!("Existing pipeline {} found", id);
        }
        Ok(())
    }

    /// Create a pipeline instance, identified by `id` for an existing artefact in the repository
    /// and start the instance
    ///
    /// # Errors
    ///  * if the id isn't a pipeline instance or can't be bound
    pub async fn create_pipeline_instance(&self, id: &TremorUrl) -> Result<pipeline::Addr> {
        info!("Creating pipeline instance {}", id);
        match (&self.repo.find_pipeline(id).await?, &id.instance()) {
            (Some(artefact), Some(_instance_id)) => {
                let artefact = artefact.artefact.clone();
                let instance = artefact.spawn(self, id.clone()).await?;
                self.repo.register_pipeline_instance(id).await?;
                self.reg
                    .publish_pipeline(id, artefact, instance.clone())
                    .await?;
                Ok(instance)
            }
            (None, _) => Err(ErrorKind::ArtefactNotFound(id.to_string()).into()),
            (_, None) => Err(ErrorKind::InvalidInstanceUrl(id.to_string()).into()),
        }
    }

    /// Remove a pipeline instance identified by `id` from registry and repo and stop the instance
    ///
    /// # Errors
    ///  * if the id isn't an pipeline instance or the pipeline can't be unregistered
    pub async fn destroy_pipeline_instance(&self, id: &TremorUrl) -> Result<()> {
        info!("Destroying pipeline instance {}", id);
        match (&self.reg.find_pipeline(id).await?, id.instance()) {
            (Some(_instance), Some(_instance_id)) => {
                // remove instance from registry
                let addr = self.reg.unpublish_pipeline(id).await?;
                // stop instance
                addr.stop().await?;
                // unregister instance from repo
                self.repo.unregister_pipeline_instance(id).await?;
                Ok(())
            }
            (None, _) => {
                Err(ErrorKind::InstanceNotFound("pipeline".to_string(), id.to_string()).into())
            }
            (_, None) => Err(ErrorKind::InvalidInstanceUrl(id.to_string()).into()),
        }
    }

    /// Connects a pipeline according to the given `mappings`.
    ///
    /// # Errors
    ///  * if the id isn't a pipeline or the pipeline can't be linked
    pub async fn link_pipeline(
        &self,
        id: &TremorUrl,
        mappings: HashMap<
            <PipelineArtefact as Artefact>::LinkLHS,
            <PipelineArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<PipelineArtefact as Artefact>::LinkResult> {
        info!(
            "Linking pipeline {} to {}",
            id,
            mappings
                .iter()
                .map(|(port, url)| format!("{} -> {}", port, url))
                .collect::<Vec<_>>()
                .join(", ")
        );
        if let Some(pipeline_a) = self.repo.find_pipeline(id).await? {
            if self.reg.find_pipeline(id).await?.is_none() {
                self.create_pipeline_instance(id).await?;
            };
            pipeline_a.artefact.link(self, id, mappings).await
        } else {
            Err(ErrorKind::ArtefactNotFound(id.to_string()).into())
        }
    }

    /// Unlink a pipeline
    ///
    /// # Errors
    ///  * if the id isn't a pipeline or the pipeline can't be unlinked
    pub async fn unlink_pipeline(
        &self,
        id: &TremorUrl,
        mappings: HashMap<
            <PipelineArtefact as Artefact>::LinkLHS,
            <PipelineArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<PipelineArtefact as Artefact>::LinkResult> {
        if let Some(pipeline_a) = self.repo.find_pipeline(id).await? {
            let r = pipeline_a.artefact.unlink(self, id, mappings).await?;
            if r {
                self.destroy_pipeline_instance(id).await?;
            };
            Ok(r)
        } else {
            Err(ErrorKind::ArtefactNotFound(id.to_string()).into())
        }
    }

    /// Create a connector instance - create an instance and stick it into the registry
    ///
    /// # Errors
    ///  * if the id isn't a connector instance or it can't be created
    pub async fn create_connector_instance(&self, id: &TremorUrl) -> Result<connectors::Addr> {
        info!("Creating connector instance {}", id);
        match (&self.repo.find_connector(id).await?, &id.instance()) {
            (Some(artefact), Some(_instance_id)) => {
                let artefact = artefact.artefact.clone();
                let instance = artefact.spawn(self, id.to_instance()).await?;
                self.repo.register_connector_instance(id).await?;
                self.reg
                    .publish_connector(id, artefact, instance.clone())
                    .await?;
                Ok(instance)
            }
            (None, _) => Err(ErrorKind::ArtefactNotFound(id.to_string()).into()),
            (_, None) => Err(ErrorKind::InvalidInstanceUrl(id.to_string()).into()),
        }
    }

    /// stop and remove a connector instance from the registry
    ///
    /// # Errors
    ///  * if the id isn't a connector instance or it can't be found in the registry or the process times out
    pub async fn destroy_connector_instance(&self, id: &TremorUrl) -> Result<()> {
        info!("Destroying connector instance {}", id);
        match (&self.reg.find_connector(id).await?, id.instance()) {
            (Some(_instance), Some(_instance_id)) => {
                // remove from registry
                let addr = self.reg.unpublish_connector(id).await?;
                // remove instance from repo
                self.repo.unregister_connector_instance(id).await?;
                // stop instance
                let (tx, rx) = bounded(1);
                addr.stop(tx).await?;
                // we timeout the stop process here, so we won't hang forever
                rx.recv()
                    .timeout(DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT)
                    .await??
                    .res?;
                Ok(())
            }
            (None, _) => Err(ErrorKind::InstanceNotFound(
                ResourceType::Connector.to_string(),
                id.to_string(),
            )
            .into()),
            (_, None) => Err(ErrorKind::InvalidInstanceUrl(id.to_string()).into()),
        }
    }

    /// Ensures the existance of a connector instance, bdingin it if required.
    ///
    /// # Errors
    ///  * if we can't ensure the connector is bound
    pub async fn ensure_connector(&self, id: &TremorUrl) -> Result<()> {
        if self.reg.find_connector(id).await?.is_none() {
            info!(
                "Connector instance {} not found in registry, creating a new instance.",
                &id
            );
            self.create_connector_instance(id).await?;
        } else {
            info!("Existing connector {} found", id);
        }
        Ok(())
    }

    /// Link a connector
    ///
    /// # Errors
    ///  * if the id isn't a connector or can't be linked
    pub async fn link_connector(
        &self,
        id: &TremorUrl,
        mappings: HashMap<
            <ConnectorArtefact as Artefact>::LinkLHS,
            <ConnectorArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<ConnectorArtefact as Artefact>::LinkResult> {
        if let Some(connector_a) = self.repo.find_connector(id).await? {
            if self.reg.find_connector(id).await?.is_none() {
                self.create_connector_instance(id).await?;
            }
            connector_a.artefact.link(self, id, mappings).await
        } else {
            Err(ErrorKind::ArtefactNotFound(id.to_string()).into())
        }
    }

    /// Disconnect connector from the connections given in `mappings`
    /// if fully disconnected, the connector is terminated
    ///
    /// # Errors
    ///  * invalid id, artefact or instance not found, error unlinking or unbinding
    pub async fn unlink_connector(
        &self,
        id: &TremorUrl,
        mappings: HashMap<
            <ConnectorArtefact as Artefact>::LinkLHS,
            <ConnectorArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<ConnectorArtefact as Artefact>::LinkResult> {
        if let Some(connector) = self.repo.find_connector(id).await? {
            let fully_disconnected = connector.artefact.unlink(self, id, mappings).await?;
            if fully_disconnected {
                self.destroy_connector_instance(id).await?;
            }
            Ok(fully_disconnected)
        } else {
            Err(ErrorKind::ArtefactNotFound(id.to_string()).into())
        }
    }

    pub(crate) async fn create_binding_instance(
        &self,
        id: &TremorUrl,
        artefact: &BindingArtefact,
    ) -> Result<binding::Addr> {
        info!("Creating Binding instance {}", id);
        match &id.instance() {
            Some(_instance_id) => {
                let artefact = artefact.clone();
                let instance = artefact.spawn(self, id.to_instance()).await?;
                self.repo.register_binding_instance(id).await?;
                self.reg
                    .publish_binding(id, artefact, instance.clone())
                    .await?;
                Ok(instance)
            }
            None => Err(ErrorKind::InvalidInstanceUrl(id.to_string()).into()),
        }
    }

    pub(crate) async fn destroy_binding_instance(&self, id: &TremorUrl) -> Result<()> {
        info!("Destroying Binding instance {}", id);
        match &id.instance() {
            Some(_instance_id) => {
                // remove from registry
                let (addr, _artefact) = self.reg.unpublish_binding(id).await?;
                // remove instance from repo
                self.repo.unregister_binding_instance(id).await?;
                // stop instance
                let (tx, rx) = bounded(1);
                addr.stop(tx).await?;
                rx.recv()
                    .timeout(DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT)
                    .await???;

                Ok(())
            }
            None => Err(ErrorKind::InvalidInstanceUrl(id.to_string()).into()),
        }
    }

    /// create and start an instance of a published binding artefact given a mapping in `mappings`
    ///
    /// # Errors
    ///  * If the id is not a valid binding instance Url
    ///  * If a binding instance with the same instance id is already running
    ///  * If for some reason it couldn't be linked or started
    pub async fn launch_binding(
        &self,
        id: &TremorUrl,
        mappings: HashMap<
            <BindingArtefact as Artefact>::LinkLHS,
            <BindingArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<BindingArtefact as Artefact>::LinkResult> {
        // ensure no instance is running yet
        if let Some(_instance) = self.reg.find_binding(id).await? {
            return Err(ErrorKind::InstanceAlreadyExists(id.to_string()).into());
        }
        // find the artefact
        if let Some(artefact) = self.repo.find_binding(id).await? {
            // link the config given the mappings
            let linked = artefact.artefact.link(self, id, mappings).await?;
            // spawn an instance
            let spawned = linked.spawn(self, id.clone()).await?;

            // register in repo, publish to registry
            self.repo.register_binding_instance(id).await?;
            self.reg
                .publish_binding(id, linked.clone(), spawned)
                .await?;

            // start the instance -> thus starting all contained instances
            self.reg.start_binding(id).await?;
            Ok(linked)
        } else {
            Err(ErrorKind::ArtefactNotFound(id.to_string()).into())
        }
    }

    /// Unpublish from registry, drain, unlin , unlink and unregister/unpublish the instance identified by `id`
    ///
    /// # Errors
    ///  * If the id is not a valid binding instance url
    ///  * If no binding with `id` is currently running, or no artefact could be found
    ///  * If for some reason stopping, unlinking unpublishing failed
    pub async fn destroy_binding(&self, id: &TremorUrl) -> Result<()> {
        let (instance, artefact) = self.reg.unpublish_binding(id).await?;
        // unregister instance from repo
        self.repo.unregister_binding_instance(id).await?;

        // Drain the binding
        let (drain_tx, drain_rx) = bounded(1);
        instance.send(binding::Msg::Drain(drain_tx)).await?;
        // swallow draining error, this is just best effort
        // this will include timeout errors
        let _ = drain_rx.recv().await?;

        // unlink all stopped instances
        let dummy_mappings = HashMap::new();
        artefact.unlink(self, id, dummy_mappings).await?;

        // stop this instance - and thus all contained instances
        // if we stop before we cannot actually unlink anymore
        let (tx, rx) = bounded(1);
        instance.stop(tx).await?;
        rx.recv()
            .timeout(DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT)
            .await???;

        Ok(())
    }

    /// Instantiates a binding given the mapping in `mappings`
    ///  * creates an instance if none is running yet
    ///
    /// # Errors
    ///  * If the id isn't a binding or the bindig can't be linked
    pub async fn link_binding(
        &self,
        id: &TremorUrl,
        mappings: HashMap<
            <BindingArtefact as Artefact>::LinkLHS,
            <BindingArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<BindingArtefact as Artefact>::LinkResult> {
        if let Some(binding_a) = self.repo.find_binding(id).await? {
            let r = binding_a.artefact.link(self, id, mappings).await?;
            if self.reg.find_binding(id).await?.is_none() {
                self.create_binding_instance(id, &r).await?;
            };
            Ok(r)
        } else {
            Err(ErrorKind::ArtefactNotFound(id.to_string()).into())
        }
    }

    /// Unlinks a binding
    ///
    /// # Errors
    ///  * if the id isn't an binding or the binding can't be unbound
    pub async fn unlink_binding(
        &self,
        id: &TremorUrl,
        mappings: HashMap<
            <BindingArtefact as Artefact>::LinkLHS,
            <BindingArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<BindingArtefact as Artefact>::LinkResult> {
        if let Some((_addr, artefact)) = self.reg.find_binding(id).await? {
            if artefact.unlink(self, id, mappings).await? {
                self.destroy_binding_instance(id).await?;
            }
            return Ok(artefact);
        }

        Err(ErrorKind::ArtefactNotFound(id.to_string()).into())
    }

    /// Turns the running system into a config
    ///
    /// # Errors
    ///  * If the systems configuration can't be stored
    pub async fn to_config(&self) -> Result<Config> {
        let binding: BindingVec = self
            .repo
            .serialize_bindings()
            .await?
            .into_iter()
            .map(|b| b.binding)
            .collect();
        let mapping: MappingMap = self.reg.serialize_mappings().await?;
        let config = crate::config::Config {
            connector: vec![],
            binding,
            mapping,
        };
        Ok(config)
    }

    /// Saves the current config
    ///
    /// # Errors
    ///  * if the config can't be saved
    pub async fn save_config(&self) -> Result<String> {
        if let Some(storage_directory) = &self.storage_directory {
            let config = self.to_config().await?;
            let path = Path::new(storage_directory);
            let file_name = format!("config_{}.yaml", nanotime());
            let mut file_path = path.to_path_buf();
            file_path.push(Path::new(&file_name));
            info!(
                "Serializing configuration to file {}",
                file_path.to_string_lossy()
            );
            let mut f = file::create(&file_path).await?;
            f.write_all(&serde_yaml::to_vec(&config)?).await?;
            // lets really sync this!
            f.sync_all().await?;
            f.sync_all().await?;
            f.sync_all().await?;
            Ok(file_path.to_string_lossy().to_string())
        } else {
            Ok("".to_string())
        }
    }

    /// Starts the runtime system
    ///
    /// # Errors
    ///  * if the world manager can't be started
    pub async fn start(config: WorldConfig) -> Result<(Self, JoinHandle<Result<()>>)> {
        let (system_h, system) = Manager {
            deployments: HashMap::new(),
            known_connectors: KnownConnectors::new(),
            operator_id_gen: OperatorIdGen::new(),
            connector_id_gen: ConnectorIdGen::new(),
            qsize: config.qsize,
        }
        .start();

        let world = Self { system };

        connectors::register_builtin_connector_types(&world, config.debug_connectors).await?;
        Ok((world, system_h))
    }

    /// Drain the runtime
    ///
    /// # Errors
    ///  * if the system failed to drain
    pub async fn drain(&self, timeout: Duration) -> Result<()> {
        let (tx, rx) = bounded(1);
        self.system.send(ManagerMsg::Drain(tx)).await?;
        match rx.recv().timeout(timeout).await {
            Err(_) => {
                warn!("Timeout draining all Flows after {}s", timeout.as_secs());
                Ok(())
            }
            Ok(res) => res?,
        }
    }

    /// Stop the runtime
    ///
    /// # Errors
    ///  * if the system failed to stop
    pub async fn stop(&self, mode: ShutdownMode) -> Result<()> {
        match mode {
            ShutdownMode::Graceful => {
                if let Err(e) = self.drain(DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT).await {
                    error!("Error draining all Flows: {}", e);
                }
            }
            ShutdownMode::Forceful => {}
        }
        if let Err(e) = self.system.send(ManagerMsg::Stop).await {
            error!("Error stopping all Flows: {}", e);
        }
        Ok(self.system.send(ManagerMsg::Stop).await?)
    }
}
