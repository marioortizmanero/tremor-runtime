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

use crate::connectors::ConnectorType;
use crate::Result;
use simd_json::ValueType;
use tremor_script::{
    ast::{self, Helper},
    FN_REGISTRY,
};
use tremor_value::prelude::*;

pub(crate) type Id = String;

/// Reconnect strategies for controlling if and how to reconnect
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase", deny_unknown_fields)]
pub enum Reconnect {
    /// No reconnection
    None,
    /// Configurable retries
    Retry {
        /// start interval to wait after a failing connect attempt
        interval_ms: u64,
        /// growth rate for consecutive connect attempts, will be added to interval_ms
        #[serde(default = "default_growth_rate")]
        growth_rate: f64,
        /// maximum number of retries to execute
        max_retries: Option<u64>,
        /// Randomize the growth rate
        #[serde(default = "default_randomized")]
        randomized: bool,
    },
}

fn default_growth_rate() -> f64 {
    1.5
}

fn default_randomized() -> bool {
    true
}

impl Default for Reconnect {
    fn default() -> Self {
        Self::None
    }
}

/* TODO: currently this is implemented differently in every connector

/// how a connector behaves upon Pause or CB trigger events
/// w.r.t maintaining its connection to the outside world (e.g. TCP connection, database connection)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PauseBehaviour {
    /// close the connection
    Close,
    /// does not support Pause and throws an error if it is attempted
    Error,
    /// keep the connection open, this will possibly fill OS buffers and lead to sneaky errors once they run full
    KeepOpen,
}

impl Default for PauseBehaviour {
    fn default() -> Self {
        Self::KeepOpen
    }
}
*/

/// Codec name and configuration
#[derive(Clone, Debug, Default)]
pub struct NameWithConfig {
    pub(crate) name: String,
    pub(crate) config: Option<Value<'static>>,
}

impl NameWithConfig {
    fn from_value(value: &Value) -> Result<Self> {
        if let Some(name) = value.as_str() {
            Ok(Self::from(name))
        } else if let Some(name) = value.get_str("name") {
            Ok(Self {
                name: name.to_string(),
                config: value.get("config").map(Value::clone_static),
            })
        } else {
            Err(format!("Invalid codec: {}", value).into())
        }
    }
}

impl From<&str> for NameWithConfig {
    fn from(name: &str) -> Self {
        Self {
            name: name.to_string(),
            config: None,
        }
    }
}
impl From<&String> for NameWithConfig {
    fn from(name: &String) -> Self {
        Self {
            name: name.clone(),
            config: None,
        }
    }
}

/// A Codec
pub type Codec = NameWithConfig;
/// A Preprocessor
pub(crate) type Preprocessor = NameWithConfig;
/// A Postprocessor
pub(crate) type Postprocessor = NameWithConfig;

/// Connector configuration - only the parts applicable to all connectors
/// Specific parts are catched in the `config` map.
#[derive(Clone, Debug, Default)]
pub struct Connector {
    /// Connector identifier
    pub id: Id,

    /// Connector type
    pub connector_type: ConnectorType,

    /// Codec in force for connector
    pub codec: Option<Codec>,

    /// Configuration map
    pub config: tremor_pipeline::ConfigMap,

    // TODO: interceptors or configurable processors
    /// Preprocessor chain configuration
    pub preprocessors: Option<Vec<Preprocessor>>,

    // TODO: interceptors or configurable processors
    /// Postprocessor chain configuration
    pub postprocessors: Option<Vec<Postprocessor>>,

    pub(crate) reconnect: Reconnect,

    //pub(crate) on_pause: PauseBehaviour,
    pub(crate) metrics_interval_s: Option<u64>,
}

impl Connector {
    /// Spawns a connector from a declaration
    pub fn from_defn(
        alias: &str,
        defn: &ast::ConnectorDefinition<'static>,
    ) -> crate::Result<Connector> {
        let aggr_reg = tremor_script::registry::aggr();
        let reg = &*FN_REGISTRY.read()?;

        let mut helper = Helper::new(reg, &aggr_reg);
        let params = defn.params.clone();

        let conf = params.generate_config(&mut helper)?;

        Connector::from_config(alias, defn.builtin_kind.clone().into(), conf)
    }
    /// Creates a connector from it's definition (aka config + settings)
    pub fn from_config<A: ToString>(
        alias: A,
        connector_type: ConnectorType,
        connector_config: Value<'static>,
    ) -> crate::Result<Connector> {
        let config = connector_config.get("config").cloned();

        fn validate_type(v: &Value, k: &str, t: ValueType) -> Result<()> {
            if v.get(k).is_some() && v.get(k).map(Value::value_type) != Some(t) {
                return Err(format!(
                    "Expect type {:?} for key {} but got {:?}",
                    t,
                    k,
                    v.get(k).map(Value::value_type).unwrap_or(ValueType::Null)
                )
                .into());
            }
            Ok(())
        }

        // TODO: can we get hygenic errors here?

        validate_type(&connector_config, "codec_map", ValueType::Object)?;
        validate_type(&connector_config, "preprocessors", ValueType::Array)?;
        validate_type(&connector_config, "postprocessors", ValueType::Array)?;
        validate_type(&connector_config, "metrics_interval_s", ValueType::U64)
            .or_else(|_| validate_type(&connector_config, "metrics_interval_s", ValueType::I64))?;

        Ok(Connector {
            id: alias.to_string(),
            connector_type,
            config: config,
            codec_map: connector_config
                .get_object("codec_map")
                .map(|o| {
                    o.iter()
                        .map(|(k, v)| Ok((k.to_string(), Codec::from_value(v)?)))
                        .collect::<Result<HashMap<_, _>>>()
                })
                .transpose()?,
            preprocessors: connector_config
                .get_array("preprocessors")
                .map(|o| {
                    o.iter()
                        .map(Preprocessor::from_value)
                        .collect::<Result<_>>()
                })
                .transpose()?,
            postprocessors: connector_config
                .get_array("postprocessors")
                .map(|o| {
                    o.iter()
                        .map(Preprocessor::from_value)
                        .collect::<Result<_>>()
                })
                .transpose()?,
            reconnect: connector_config
                .get("reconnect")
                .cloned()
                .map(tremor_value::structurize)
                .transpose()?
                .unwrap_or_default(),
            metrics_interval_s: connector_config.get_u64("metrics_interval_s"),
            codec: connector_config
                .get("codec")
                .map(Codec::from_value)
                .transpose()?,
        })
    }
}

/// Configuration for a Binding
#[derive(Clone, Debug)]
pub struct Binding {
    /// ID of the binding
    pub id: Id,
    /// Description
    pub description: String,
    /// Binding map
    pub links: Vec<ast::ConnectStmt>, // is this right? this should be url to url?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::Result;

    #[test]
    fn test_reconnect_serde() -> Result<()> {
        assert_eq!(
            "---\n\
            none\n",
            serde_yaml::to_string(&Reconnect::None)?
        );
        let none_strategy = r#"
        none
        "#;
        let reconnect = serde_yaml::from_str::<Reconnect>(none_strategy)?;
        assert!(matches!(reconnect, Reconnect::None));
        let retry = r#"
        retry:
          interval_ms: 123
          growth_rate: 1.234567
        "#;
        let reconnect = serde_yaml::from_str::<Reconnect>(retry)?;
        assert!(matches!(
            reconnect,
            Reconnect::Retry {
                interval_ms: 123,
                growth_rate: _,
                max_retries: None,
                randomized: true
            }
        ));
        Ok(())
    }

    #[test]
    fn test_config_builtin_preproc_with_config() -> Result<()> {
        let c = Connector::from_defn(
            "snot".to_string(),
            ConnectorType::from("otel_client".to_string()),
            literal!({
                "preprocessors": [ {"name": "snot", "config": { "separator": "\n" }}],
            }),
        )?;
        let pp = c.preprocessors;
        assert!(pp.is_some());

        if let Some(pp) = pp {
            assert_eq!("snot", pp[0].name);
            assert!(pp[0].config.is_some());
            if let Some(config) = &pp[0].config {
                assert_eq!("\n", config.get("separator").unwrap().to_string());
            }
        }
        Ok(())
    }
}
