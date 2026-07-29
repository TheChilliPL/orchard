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

    fn discovery_components(&self) -> HashMap<String, DiscoveryComponent> { HashMap::new() }
    fn subscriptions(&self) -> Vec<String> { Vec::new() }

    async fn init(&mut self, mqtt: &MqttScope) { }
    async fn handle_message(&mut self, mqtt: &MqttScope, topic: &str, payload: &[u8]) { }
    async fn update(&mut self, mqtt: &MqttScope) { }
}
