use derive_more::with_trait::From;
use orchard_macros::short_names;
use serde::Serialize;
use serde_with::skip_serializing_none;

use crate::prelude::*;

use super::{ButtonSpec, NumberSpec, SensorSpec, SwitchSpec};

#[skip_serializing_none]
#[short_names]
#[derive(Debug, Clone, Serialize, Default)]
pub struct DiscoveryComponent {
    #[short_name("uniq_id")]
    pub unique_id: String,
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
