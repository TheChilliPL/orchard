use orchard_macros::short_names;
use serde::Serialize;
use serde_with::skip_serializing_none;
use crate::mqtt::scope::{MqttScope, Scopeable};

#[skip_serializing_none]
#[short_names]
#[derive(Debug, Clone, Serialize)]
pub struct ButtonSpec {
    #[short_name("cmd_t")]
    pub command_topic: String,
    #[short_name("cmd_tpl")]
    pub command_template: Option<String>,
    #[short_name("pl_prs")]
    pub payload_press: String,
}

impl Scopeable for ButtonSpec {
    fn scope(mut self, scope: &MqttScope) -> Self {
        self.command_topic = scope.scope_topic(&self.command_topic);
        self
    }
}

impl Default for ButtonSpec {
    fn default() -> Self {
        ButtonSpec {
            command_topic: "".into(),
            command_template: None,
            payload_press: "PRESS".into(),
        }
    }
}
