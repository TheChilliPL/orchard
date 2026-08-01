use orchard_macros::short_names;
use serde::Serialize;
use serde_with::skip_serializing_none;
use crate::mqtt::scope::{MqttScope, Scopeable};

#[skip_serializing_none]
#[short_names]
#[derive(Debug, Clone, Serialize)]
pub struct SwitchSpec {
    #[short_name("stat_t")]
    pub state_topic: Option<String>,
    #[short_name("val_tpl")]
    pub value_template: Option<String>,
    #[short_name("cmd_t")]
    pub command_topic: String,
    #[short_name("cmd_tpl")]
    pub command_template: Option<String>,
    #[short_name("pl_off")]
    pub payload_off: String,
    #[short_name("pl_on")]
    pub payload_on: String,
}

impl Scopeable for SwitchSpec {
    fn scope(mut self, scope: &MqttScope) -> Self {
        if let Some(state_topic) = self.state_topic.as_mut() {
            *state_topic = scope.scope_topic(state_topic);
        }
        self.command_topic = scope.scope_topic(&self.command_topic);
        self
    }
}

impl Default for SwitchSpec {
    fn default() -> Self {
        SwitchSpec {
            state_topic: None,
            value_template: None,
            command_topic: "".into(),
            command_template: None,
            payload_off: "OFF".into(),
            payload_on: "ON".into(),
        }
    }
}
