pub mod status;
pub mod volume;
pub mod media;
pub mod system_control;
pub mod sysinfo;
pub mod nvidia;
#[cfg(feature = "obs")]
pub mod obs;

use std::collections::HashMap;
use async_trait::async_trait;
use crate::discovery::DiscoveryComponent;
use crate::mqtt::scope::MqttScope;

#[allow(unused_variables)]
#[async_trait]
pub trait Module: Send {
    fn name(&self) -> &'static str;

    /// Function returning the map of all discovery components published for Home Assistant.
    ///
    /// If an error is returned, the module initialization is canceled.
    fn discovery_components(&self) -> eyre::Result<HashMap<String, DiscoveryComponent>> { Ok(HashMap::new()) }
    /// Function returning all the subscribed topics of this module.
    ///
    /// If an error is returned, the module initialization is canceled.
    fn subscriptions(&self) -> eyre::Result<Vec<String>> { Ok(Vec::new()) }

    /// Function called when initializing the module. It may be used, e.g., to publish some starting values.
    ///
    /// If an error is returned, the module initialization is canceled.
    async fn init(&mut self, mqtt: &MqttScope) -> eyre::Result<()> { Ok(()) }
    /// Function called when a subscribed topic is updated.
    ///
    /// If an error is returned, it is logged as a warning, but the module keeps running.
    async fn handle_message(&mut self, mqtt: &MqttScope, topic: &str, payload: &[u8]) -> eyre::Result<()> { Ok(()) }
    /// Function called every few seconds.
    ///
    /// If an error is returned, it is logged as a warning, but the module keeps running.
    async fn update(&mut self, mqtt: &MqttScope) -> eyre::Result<()> { Ok(()) }
}
