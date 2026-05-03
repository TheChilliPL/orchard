use orchard_macros::short_names;
use serde::Serialize;
use serde_with::skip_serializing_none;

#[skip_serializing_none]
#[short_names]
#[derive(Debug, Clone, Serialize)]
pub struct NumberSpec {
    #[short_name("stat_t")]
    pub state_topic: Option<String>,
    #[short_name("val_tpl")]
    pub value_template: Option<String>,
    #[short_name("cmd_t")]
    pub command_topic: String,
    #[short_name("cmd_tpl")]
    pub command_template: Option<String>,
    pub min: f32,
    pub max: f32,
    pub step: f32,
    #[short_name("unit_of_meas")]
    pub unit_of_measurement: Option<String>,
}

impl Default for NumberSpec {
    fn default() -> Self {
        NumberSpec {
            state_topic: None,
            value_template: None,
            command_topic: "".into(),
            command_template: None,
            min: 1.0,
            max: 100.0,
            step: 1.0,
            unit_of_measurement: None,
        }
    }
}
