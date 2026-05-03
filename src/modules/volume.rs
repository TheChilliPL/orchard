use std::collections::HashMap;
use async_trait::async_trait;
use rumqttc::QoS;
use tracing::{debug, warn};
use volumecontrol::AudioDevice;
use crate::discovery::{ButtonSpec, DiscoveryComponent, NumberSpec, SensorSpec, SwitchSpec};
use crate::modules::Module;

pub struct VolumeModule {
    last_state: Option<VolumeState>
}

impl VolumeModule {
    pub fn new() -> Self { Self { last_state: None } }

    fn get_current_state(&self) -> VolumeState {
        let device = AudioDevice::from_default().unwrap();
        let muted = device.is_mute().unwrap();
        let volume = device.get_vol().unwrap();

        VolumeState {
            volume,
            muted,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeState {
    pub volume: u8,
    pub muted: bool,
}

#[async_trait]
impl Module for VolumeModule {
    fn name(&self) -> &'static str {
        "Volume Module"
    }

    fn discovery_components(&self, hostname: &str) -> HashMap<String, DiscoveryComponent> {
        HashMap::from([
            ("mute".into(), DiscoveryComponent {
                unique_id: format!("orchard-{}-mute", hostname),
                name: "Mute audio".into(),
                spec: SwitchSpec {
                    state_topic: Some(format!("orchard/{}/mute", hostname)),
                    command_topic: format!("orchard/{}/mute/set", hostname),
                    ..Default::default()
                }.into(),
                icon: Some("mdi:volume-mute".into()),
                ..Default::default()
            }),
            ("volume".into(), DiscoveryComponent {
                unique_id: format!("orchard-{}-volume", hostname),
                name: "Volume".into(),
                spec: NumberSpec {
                    state_topic: Some(format!("orchard/{}/volume", hostname)),
                    command_topic: format!("orchard/{}/volume/set", hostname),
                    // step: 0.1,
                    unit_of_measurement: Some("%".into()),
                    ..Default::default()
                }.into(),
                icon: Some("mdi:volume-high".into()),
                ..Default::default()
            }),
            ("volume_inc".into(), DiscoveryComponent {
                unique_id: format!("orchard-{}-volume-inc", hostname),
                name: "Increase volume".into(),
                spec: ButtonSpec {
                    command_topic: format!("orchard/{}/volume/inc", hostname),
                    ..Default::default()
                }.into(),
                icon: Some("mdi:volume-plus".into()),
                ..Default::default()
            }),
            ("volume_dec".into(), DiscoveryComponent {
                unique_id: format!("orchard-{}-volume-dec", hostname),
                name: "Decrease volume".into(),
                spec: ButtonSpec {
                    command_topic: format!("orchard/{}/volume/dec", hostname),
                    ..Default::default()
                }.into(),
                icon: Some("mdi:volume-minus".into()),
                ..Default::default()
            }),
        ])
    }

    fn subscriptions(&self, hostname: &str) -> Vec<String> {
        vec![
            format!("orchard/{}/mute/set", hostname),
            format!("orchard/{}/volume/set", hostname),
            format!("orchard/{}/volume/inc", hostname),
            format!("orchard/{}/volume/dec", hostname),
        ]
    }

    async fn init(&mut self, hostname: &str, mqttc: &rumqttc::AsyncClient) {
        self.update(hostname, mqttc).await;
    }

    async fn handle_message(&mut self, hostname: &str, mqttc: &rumqttc::AsyncClient, topic: &str, payload: &[u8]) {
        let volume_topic = format!("orchard/{}/volume/set", hostname);
        let mute_topic = format!("orchard/{}/mute/set", hostname);
        let inc_topic = format!("orchard/{}/volume/inc", hostname);
        let dec_topic = format!("orchard/{}/volume/dec", hostname);

        let Ok(payload) = std::str::from_utf8(payload) else {
            warn!(topic, "Ignoring non-UTF8 payload.");
            return;
        };

        if topic == volume_topic {
            let Ok(volume) = payload.trim().parse::<u8>() else {
                warn!(topic, payload, "Ignoring invalid volume payload.");
                return;
            };

            AudioDevice::from_default().unwrap().set_vol(volume).unwrap();
            self.update(hostname, mqttc).await;
        } else if topic == mute_topic {
            let should_mute = match payload {
                "ON" => true,
                "OFF" => false,
                _ => {
                    warn!(topic, payload, "Ignoring invalid mute payload.");
                    return;
                }
            };

            AudioDevice::from_default().unwrap().set_mute(should_mute).unwrap();
            self.update(hostname, mqttc).await;
        } else if topic == inc_topic {
            {
                let device = AudioDevice::from_default().unwrap();
                let new_vol = (device.get_vol().unwrap() + 1).clamp(0, 100);
                device.set_vol(new_vol).unwrap();
            }
            self.update(hostname, mqttc).await;
        } else if topic == dec_topic {
            {
                let device = AudioDevice::from_default().unwrap();
                let new_vol = (device.get_vol().unwrap() - 1).clamp(0, 100);
                device.set_vol(new_vol).unwrap();
            }
            self.update(hostname, mqttc).await;
        }
    }

    async fn update(&mut self, hostname: &str, mqttc: &rumqttc::AsyncClient) {
        let state = self.get_current_state();
        if self.last_state.as_ref() == Some(&state) { return; }

        debug!(?state, "Updating volume state.");

        mqttc.publish(format!("orchard/{}/mute", hostname), QoS::AtMostOnce, true, if state.muted { "ON" } else { "OFF" }).await.unwrap();
        mqttc.publish(format!("orchard/{}/volume", hostname), QoS::AtMostOnce, true, state.volume.to_string()).await.unwrap();

        self.last_state = Some(state);

        debug!("Volume state updated.");
    }
}
