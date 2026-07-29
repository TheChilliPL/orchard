use derive_more::with_trait::From;
use orchard_macros::short_names;
use serde::Serialize;
use serde_with::{skip_serializing_none, DeserializeFromStr, SerializeDisplay};
use crate::mqtt::scope::{MqttScope, Scopeable};
use crate::prelude::*;

use super::{ButtonSpec, NumberSpec, SensorSpec, SwitchSpec};

#[skip_serializing_none]
#[short_names]
#[derive(Debug, Clone, Serialize, Default)]
pub struct DiscoveryComponent {
    #[short_name("uniq_id")]
    pub unique_id: String,
    #[short_name("def_ent_id")]
    pub default_entity_id: Option<String>,
    pub name: String,
    #[short_name("ic")]
    pub icon: Option<String>,
    #[short_name("ent_pic")]
    pub entity_picture: Option<String>,
    #[short_name("json_attr_t")]
    pub json_attributes_topic: Option<String>,
    #[short_name("json_attr_tpl")]
    pub json_attributes_template: Option<String>,
    #[serde(skip_serializing_if = "is_default")]
    pub qos: u8, // TODO
    #[short_name("ret")]
    #[serde(skip_serializing_if = "is_false")]
    pub retain: bool,
    #[serde(flatten)]
    pub spec: ComponentSpec,
    #[serde(skip_serializing_if = "is_default")]
    pub assumed_state: bool,
    #[short_name("avty")]
    #[serde(skip_serializing_if = "is_default")]
    pub availability: Vec<Availability>,
    #[short_name("avty_mode")]
    #[serde(skip_serializing_if = "is_default")]
    pub availability_mode: AvailabilityMode,
}

#[skip_serializing_none]
#[short_names]
#[derive(Debug, Clone, PartialEq, Serialize, Default)]
pub struct Availability {
    #[short_name("t")]
    pub topic: String,
    #[short_name("pl_avail")]
    pub payload_available: Option<String>,
    #[short_name("pl_not_avail")]
    pub payload_not_available: Option<String>,
    #[short_name("val_tpl")]
    pub value_template: Option<String>,
}

impl Scopeable for Availability {
    fn scope(mut self, scope: &MqttScope) -> Self {
        self.topic = scope.scope_topic(&self.topic);
        self
    }
}

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    Eq,
    SerializeDisplay,
    DeserializeFromStr,
    strum::Display,
    strum::EnumString,
)]
#[strum(serialize_all = "snake_case")]
pub enum AvailabilityMode {
    #[default]
    Latest,
    All,
    Any,
}

impl Scopeable for DiscoveryComponent {
    fn scope(mut self, scope: &MqttScope) -> Self {
        let entity_domain = match self.spec {
            ComponentSpec::Sensor(_) => "sensor",
            ComponentSpec::Number(_) => "number",
            ComponentSpec::Switch(_) => "switch",
            ComponentSpec::Button(_) => "button",
        };
        let raw_entity_id = self
            .default_entity_id
            .take()
            .unwrap_or_else(|| self.unique_id.clone());
        let entity_id_tail = raw_entity_id
            .split_once('.')
            .map(|(_, tail)| tail)
            .unwrap_or(&raw_entity_id);
        let scoped_entity_id_tail = scope.scope_id(entity_id_tail);
        self.default_entity_id = Some(format!("{entity_domain}.{scoped_entity_id_tail}"));
        self.unique_id = scope.scope_id(&self.unique_id);
        if let Some(json_attributes_topic) = self.json_attributes_topic.as_mut() {
            *json_attributes_topic = scope.scope_topic(json_attributes_topic);
        }
        self.availability = self.availability.into_iter().map(|a| a.scope(scope)).collect();
        self.spec = self.spec.scope(scope);
        self
    }
}

#[skip_serializing_none]
#[short_names("p", "platform")]
#[derive(Debug, Clone, Serialize, From)]
#[serde(rename_all = "lowercase")]
pub enum ComponentSpec {
    Sensor(SensorSpec),
    Number(NumberSpec),
    Switch(SwitchSpec),
    Button(ButtonSpec),
}

impl Default for ComponentSpec {
    fn default() -> Self { ComponentSpec::Sensor(SensorSpec::default()) }
}

impl Scopeable for ComponentSpec {
    fn scope(self, scope: &MqttScope) -> Self {
        use ComponentSpec::*;
        match self {
            Sensor(s) => Sensor(s.scope(scope)),
            Number(s) => Number(s.scope(scope)),
            Switch(s) => Switch(s.scope(scope)),
            Button(s) => Button(s.scope(scope)),
        }
    }
}
