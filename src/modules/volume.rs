use std::collections::HashMap;
use async_trait::async_trait;
use rumqttc::QoS;
use tracing::{debug, warn};
use volumecontrol::AudioDevice;
use crate::discovery::{ButtonSpec, DiscoveryComponent, NumberSpec, SwitchSpec};
use crate::mqtt::scope::MqttScope;
use crate::modules::Module;

pub struct VolumeModule;

impl VolumeModule {
    pub fn new() -> Self { Self }

    fn get_current_state(&self) -> eyre::Result<VolumeState> {
        let device = AudioDevice::from_default()?;
        let muted = device.is_mute()?;
        let volume = device.get_vol()?;

        Ok(VolumeState {
            volume,
            muted,
        })
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

    fn discovery_components(&self) -> eyre::Result<HashMap<String, DiscoveryComponent>> {
        Ok(HashMap::from([
            ("mute".into(), DiscoveryComponent {
                unique_id: "mute".into(),
                name: "Mute audio".into(),
                spec: SwitchSpec {
                    state_topic: Some("mute".into()),
                    command_topic: "mute/set".into(),
                    ..Default::default()
                }.into(),
                icon: Some("mdi:volume-mute".into()),
                ..Default::default()
            }),
            ("volume".into(), DiscoveryComponent {
                unique_id: "volume".into(),
                name: "Volume".into(),
                spec: NumberSpec {
                    state_topic: Some("volume".into()),
                    command_topic: "volume/set".into(),
                    // step: 0.1,
                    unit_of_measurement: Some("%".into()),
                    ..Default::default()
                }.into(),
                icon: Some("mdi:volume-high".into()),
                ..Default::default()
            }),
            ("volume_inc".into(), DiscoveryComponent {
                unique_id: "volume-inc".into(),
                name: "Increase volume".into(),
                spec: ButtonSpec {
                    command_topic: "volume/inc".into(),
                    ..Default::default()
                }.into(),
                icon: Some("mdi:volume-plus".into()),
                ..Default::default()
            }),
            ("volume_dec".into(), DiscoveryComponent {
                unique_id: "volume-dec".into(),
                name: "Decrease volume".into(),
                spec: ButtonSpec {
                    command_topic: "volume/dec".into(),
                    ..Default::default()
                }.into(),
                icon: Some("mdi:volume-minus".into()),
                ..Default::default()
            }),
        ]))
    }

    fn subscriptions(&self) -> eyre::Result<Vec<String>> {
        Ok(vec![
            "mute/set".into(),
            "volume/set".into(),
            "volume/inc".into(),
            "volume/dec".into(),
        ])
    }

    async fn init(&mut self, mqtt: &MqttScope) -> eyre::Result<()> {
        self.update(mqtt).await?;
        Ok(())
    }

    async fn handle_message(&mut self, mqtt: &MqttScope, topic: &str, payload: &[u8]) -> eyre::Result<()> {
        let Ok(payload) = std::str::from_utf8(payload) else {
            warn!(topic, "Ignoring non-UTF8 payload.");
            return Ok(());
        };

        if topic == "volume/set" {
            let Ok(volume) = payload.trim().parse::<u8>() else {
                warn!(topic, payload, "Ignoring invalid volume payload.");
                return Ok(());
            };

            AudioDevice::from_default()?.set_vol(volume)?;
            self.update(mqtt).await?;
        } else if topic == "mute/set" {
            let should_mute = match payload {
                "ON" => true,
                "OFF" => false,
                _ => {
                    warn!(topic, payload, "Ignoring invalid mute payload.");
                    return Ok(());
                }
            };

            AudioDevice::from_default()?.set_mute(should_mute)?;
            self.update(mqtt).await?;
        } else if topic == "volume/inc" {
            {
                let device = AudioDevice::from_default()?;
                let new_vol = (device.get_vol()? + 1).clamp(0, 100);
                device.set_vol(new_vol)?;
            }
            self.update(mqtt).await?;
        } else if topic == "volume/dec" {
            {
                let device = AudioDevice::from_default()?;
                let new_vol = (device.get_vol()? - 1).clamp(0, 100);
                device.set_vol(new_vol)?;
            }
            self.update(mqtt).await?;
        }
        Ok(())
    }

    async fn update(&mut self, mqtt: &MqttScope) -> eyre::Result<()> {
        let state = self.get_current_state()?;

        mqtt.publish("mute", if state.muted { "ON" } else { "OFF" })
            .with_qos(QoS::AtMostOnce)
            .await
            ?;
        mqtt.publish("volume", state.volume.to_string())
            .with_qos(QoS::AtMostOnce)
            .await
            ?;
        Ok(())
    }
}
