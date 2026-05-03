pub mod status;
pub mod volume;
pub mod media;

use std::collections::HashMap;
use async_trait::async_trait;
use crate::discovery::DiscoveryComponent;

#[allow(unused_variables)]
#[async_trait]
pub trait Module: Send {
    fn name(&self) -> &'static str;

    fn discovery_components(&self, hostname: &str) -> HashMap<String, DiscoveryComponent> { HashMap::new() }
    fn subscriptions(&self, hostname: &str) -> Vec<String> { Vec::new() }

    async fn init(&mut self, hostname: &str, mqttc: &rumqttc::AsyncClient);
    async fn handle_message(&mut self, hostname: &str, mqttc: &rumqttc::AsyncClient, topic: &str, payload: &[u8]) { }
    async fn update(&mut self, hostname: &str, mqttc: &rumqttc::AsyncClient) { }
}
