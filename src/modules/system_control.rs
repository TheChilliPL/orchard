use std::collections::HashMap;
use async_trait::async_trait;
use rumqttc::AsyncClient;
use tracing::error;
use crate::discovery::{ButtonSpec, DiscoveryComponent};
use crate::modules::Module;

pub struct SystemControlModule;

impl SystemControlModule {
    pub fn new() -> Self { Self }
}

#[async_trait]
impl Module for SystemControlModule {
    fn name(&self) -> &'static str {
        "System control module"
    }

    fn discovery_components(&self, hostname: &str) -> HashMap<String, DiscoveryComponent> {
        HashMap::from([
            ("shutdown".into(), DiscoveryComponent {
                unique_id: format!("orchard-{hostname}-shutdown"),
                name: "Shutdown".into(),
                spec: ButtonSpec {
                    command_topic: format!("orchard/{hostname}/shutdown"),
                    ..Default::default()
                }.into(),
                icon: Some("mdi:power".into()),
                ..Default::default()
            }),
            ("reboot".into(), DiscoveryComponent {
                unique_id: format!("orchard-{hostname}-reboot"),
                name: "Reboot".into(),
                spec: ButtonSpec {
                    command_topic: format!("orchard/{hostname}/reboot"),
                    ..Default::default()
                }.into(),
                icon: Some("mdi:refresh".into()),
                ..Default::default()
            }),
        ])
    }

    fn subscriptions(&self, hostname: &str) -> Vec<String> {
        vec![
            format!("orchard/{hostname}/shutdown"),
            format!("orchard/{hostname}/reboot"),
        ]
    }

    async fn handle_message(&mut self, hostname: &str, _mqttc: &AsyncClient, topic: &str, _payload: &[u8]) {
        if topic == format!("orchard/{hostname}/shutdown") {
            let res = system_shutdown::shutdown();
            if let Err(error) = res {
                error!(?error, "Failed to shutdown system.");
            }
        } else if topic == format!("orchard/{hostname}/reboot") {
            let res = system_shutdown::reboot();
            if let Err(error) = res {
                error!(?error, "Failed to reboot system.");
            }
        }
    }
}
