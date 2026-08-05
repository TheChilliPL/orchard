use std::collections::HashMap;
use async_trait::async_trait;
use crate::discovery::{DiscoveryComponent, SensorDeviceClass, SensorSpec};
use crate::mqtt::scope::MqttScope;
use crate::modules::Module;

pub struct StatusModule;

impl StatusModule {
    pub fn new() -> Self { Self }
}

#[async_trait]
impl Module for StatusModule {
    fn name(&self) -> &'static str {
        "Status Module"
    }

    fn discovery_components(&self) -> eyre::Result<HashMap<String, DiscoveryComponent>> {
        Ok(HashMap::from([
            ("status".into(), DiscoveryComponent {
                unique_id: "status".into(),
                name: "Status".into(),
                spec: SensorSpec {
                    state_topic: "status".into(),
                    value_template: Some("{{value | capitalize}}".into()),
                    options: Some(vec!["Online".into()]),
                    device_class: Some(SensorDeviceClass::Enum),
                    ..Default::default()
                }.into(),
                icon: Some("mdi:desktop-classic".into()),
                ..Default::default()
            })
        ]))
    }

    async fn init(&mut self, _mqtt: &MqttScope) -> eyre::Result<()> {
        // Status is already published in main.rs for availability purposes,
        // so we don't have to do that again.
        Ok(())
    }
}
