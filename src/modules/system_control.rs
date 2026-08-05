use std::collections::HashMap;
use async_trait::async_trait;
use tracing::error;
use crate::discovery::{ButtonSpec, DiscoveryComponent};
use crate::mqtt::scope::MqttScope;
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

    fn discovery_components(&self) -> eyre::Result<HashMap<String, DiscoveryComponent>> {
        Ok(HashMap::from([
            ("shutdown".into(), DiscoveryComponent {
                unique_id: "shutdown".into(),
                name: "Shutdown".into(),
                spec: ButtonSpec {
                    command_topic: "shutdown".into(),
                    ..Default::default()
                }.into(),
                icon: Some("mdi:power".into()),
                ..Default::default()
            }),
            ("reboot".into(), DiscoveryComponent {
                unique_id: "reboot".into(),
                name: "Reboot".into(),
                spec: ButtonSpec {
                    command_topic: "reboot".into(),
                    ..Default::default()
                }.into(),
                icon: Some("mdi:refresh".into()),
                ..Default::default()
            }),
        ]))
    }

    fn subscriptions(&self) -> eyre::Result<Vec<String>> {
        Ok(vec![
            "shutdown".into(),
            "reboot".into(),
        ])
    }

    async fn handle_message(&mut self, _mqtt: &MqttScope, topic: &str, _payload: &[u8]) -> eyre::Result<()> {
        if topic == "shutdown" {
            let res = system_shutdown::shutdown();
            if let Err(error) = res {
                error!(?error, "Failed to shutdown system.");
            }
        } else if topic == "reboot" {
            let res = system_shutdown::reboot();
            if let Err(error) = res {
                error!(?error, "Failed to reboot system.");
            }
        }
        Ok(())
    }
}
