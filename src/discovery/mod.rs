//! Home Assistant discovery structures

mod button;
mod component;
mod number;
mod sensor;
mod switch;

use std::collections::HashMap;
use orchard_macros::short_names;
use serde::Serialize;

pub use button::*;
pub use component::*;
pub use number::*;
pub use sensor::*;
pub use switch::*;
use crate::mqtt::scope::{MqttScope, Scopeable};

#[short_names]
#[derive(Debug, Clone, Serialize)]
pub struct DiscoveryPayload {
    #[short_name("dev")]
    pub device: DiscoveryDevice,
    #[short_name("o")]
    pub origin: DiscoveryOrigin,
    #[short_name("avty_t")]
    pub availability_topic: String,
    #[short_name("cmps")]
    pub components: HashMap<String, DiscoveryComponent>,
}

impl Scopeable for DiscoveryPayload {
    fn scope(mut self, scope: &MqttScope) -> Self {
        self.availability_topic = scope.scope_topic(&self.availability_topic);
        self.components = self.components.into_iter().map(|(k, v)| {
            let key = scope.scope_id(&k);
            let value = v.scope(scope);
            (key, value)
        }).collect();
        self
    }
}

#[short_names]
#[derive(Debug, Clone, Serialize)]
pub struct DiscoveryDevice {
    #[short_name("ids", "identifiers")]
    pub ids: Vec<String>,
    pub name: String,
}

#[short_names]
#[derive(Debug, Clone, Serialize)]
pub struct DiscoveryOrigin {
    pub name: String,
    #[short_name("sw", "sw_version")]
    pub version: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn serializes_component_spec_with_expected_keys() {
        let component = DiscoveryComponent {
            unique_id: "sensor-1".into(),
            name: "Sensor".into(),
            spec: ComponentSpec::Sensor(SensorSpec {
                state_topic: "orchard/sensor-1/state".into(),
                device_class: None,
                ..Default::default()
            }),
            ..Default::default()
        };

        let value = serde_json::to_value(component).expect("component should serialize");

        #[cfg(feature = "long_json")]
        assert_eq!(
            value,
            json!({
                "unique_id": "sensor-1",
                "name": "Sensor",
                "platform": "sensor",
                "state_topic": "orchard/sensor-1/state",
            })
        );

        #[cfg(not(feature = "long_json"))]
        assert_eq!(
            value,
            json!({
                "uniq_id": "sensor-1",
                "name": "Sensor",
                "p": "sensor",
                "stat_t": "orchard/sensor-1/state",
            })
        );
    }
}
