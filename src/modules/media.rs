#![cfg(target_os = "linux")]

use std::collections::HashMap;
use std::time::Duration;
use async_trait::async_trait;
use rumqttc::QoS;
use tracing::{debug, trace, warn};
use crate::discovery::{ButtonSpec, DiscoveryComponent, SensorDeviceClass, SensorSpec};
use crate::mqtt::scope::MqttScope;
use crate::modules::Module;

pub struct MediaModule;

impl MediaModule {
    pub fn new() -> Self { Self }

    fn active_player(&self) -> Option<mpris::Player> {
        let player_finder = mpris::PlayerFinder::new().ok();
        player_finder.map(|f| f.find_active().ok()).flatten()
    }
}

#[async_trait]
impl Module for MediaModule {
    fn name(&self) -> &'static str {
        "Media control module"
    }

    fn discovery_components(&self) -> eyre::Result<HashMap<String, DiscoveryComponent>> {
        Ok(HashMap::from([
            ("media_toggle".into(), DiscoveryComponent {
                unique_id: "media_play_pause".into(),
                name: "Play/pause".into(),
                spec: ButtonSpec {
                    command_topic: "media/toggle".into(),
                    ..Default::default()
                }.into(),
                icon: Some("mdi:play-pause".to_string()),
                ..Default::default()
            }),
            ("media_prev".into(), DiscoveryComponent {
                unique_id: "media_prev".into(),
                name: "Previous".into(),
                spec: ButtonSpec {
                    command_topic: "media/prev".into(),
                    ..Default::default()
                }.into(),
                icon: Some("mdi:skip-previous".into()),
                ..Default::default()
            }),
            ("media_next".into(), DiscoveryComponent {
                unique_id: "media_next".into(),
                name: "Next".into(),
                spec: ButtonSpec {
                    command_topic: "media/next".into(),
                    ..Default::default()
                }.into(),
                icon: Some("mdi:skip-next".into()),
                ..Default::default()
            }),
            ("media_status".into(), DiscoveryComponent {
                unique_id: "media_status".into(),
                name: "Media status".into(),
                spec: SensorSpec {
                    state_topic: "media/status".into(),
                    device_class: Some(SensorDeviceClass::Enum),
                    options: Some(vec!["Playing".into(), "Paused".into(), "Stopped".into()]),
                    value_template: Some("{{value | capitalize}}".into()),
                    ..Default::default()
                }.into(),
                icon: Some("mdi:play-box".into()),
                ..Default::default()
            }),
            ("media_title".into(), DiscoveryComponent {
                unique_id: "media_title".into(),
                name: "Media title".into(),
                spec: SensorSpec {
                    state_topic: "media/title".into(),
                    ..Default::default()
                }.into(),
                icon: Some("mdi:music-note".into()),
                ..Default::default()
            }),
            ("media_artists".into(), DiscoveryComponent {
                unique_id: "media_artists".into(),
                name: "Media artists".into(),
                spec: SensorSpec {
                    state_topic: "media/artists".into(),
                    ..Default::default()
                }.into(),
                icon: Some("mdi:account".into()),
                ..Default::default()
            }),
            ("media_position".into(), DiscoveryComponent {
                unique_id: "media_position".into(),
                name: "Media position".into(),
                spec: SensorSpec {
                    state_topic: "media/position".into(),
                    device_class: Some(SensorDeviceClass::Duration),
                    unit_of_measurement: Some("s".into()),
                    ..Default::default()
                }.into(),
                ..Default::default()
            }),
            ("media_duration".into(), DiscoveryComponent {
                unique_id: "media_duration".into(),
                name: "Media duration".into(),
                spec: SensorSpec {
                    state_topic: "media/duration".into(),
                    device_class: Some(SensorDeviceClass::Duration),
                    unit_of_measurement: Some("s".into()),
                    ..Default::default()
                }.into(),
                ..Default::default()
            }),

        ]))
    }

    fn subscriptions(&self) -> eyre::Result<Vec<String>> {
        Ok(vec![
            "media/toggle".into(),
            "media/prev".into(),
            "media/next".into(),
            "media/position/set".into(), // TODO implement
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

        if topic == "media/toggle" {
            self.active_player().map(|p| p.play_pause());
        } else if topic == "media/prev" {
            self.active_player().map(|p| p.previous());
        } else if topic == "media/next" {
            self.active_player().map(|p| p.next());
        } else {
            warn!(topic, ?payload, "Ignoring unknown topic.");
        }

        self.update(mqtt).await?;
        Ok(())
    }

    async fn update(&mut self, mqtt: &MqttScope) -> eyre::Result<()> {
        let (status, metadata) = {
            let player = self.active_player();

            let status = player
                .as_ref()
                .map(|p| MediaStatus::try_from(p).ok())
                .flatten();

            let metadata: Option<MediaMetadata> = player
                .map(|p| p.get_metadata().ok())
                .flatten()
                .map(|m| MediaMetadata::from(&m));

            (status, metadata)
        };

        let status_text = match status {
            Some(MediaStatus { playing, .. }) => if playing { "Playing" } else { "Paused" },
            None => "Stopped",
        };

        let position_text = match status {
            Some(MediaStatus { position, .. }) => position.map(|p| p.as_secs_f32().to_string()).unwrap_or_else(|| "None".into()),
            None => "None".into(),
        };

        trace!("Publishing media status.");

        mqtt.publish("media/status", status_text)
            .with_qos(QoS::AtMostOnce)
            .await
            ?;
        mqtt.publish("media/position", position_text)
            .with_qos(QoS::AtMostOnce)
            .await
            ?;

        let title = metadata.as_ref().map_or("", |m| &m.title);
        let artists = metadata.as_ref().map_or("", |m| &m.artists);
        let duration_text = metadata.as_ref().map_or_else(|| "None".into(), |m| m.duration.map(|d| d.as_secs_f32().to_string()).unwrap_or_else(|| "None".into()));

        mqtt.publish("media/title", title)
            .with_qos(QoS::AtMostOnce)
            .await
            ?;
        mqtt.publish("media/artists", artists)
            .with_qos(QoS::AtMostOnce)
            .await
            ?;
        mqtt.publish("media/duration", duration_text)
            .with_qos(QoS::AtMostOnce)
            .await
            ?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaMetadata {
    title: String,
    artists: String,
    duration: Option<Duration>,
}


impl From<&mpris::Metadata> for MediaMetadata {
    fn from(value: &mpris::Metadata) -> Self {
        MediaMetadata {
            title: value.title().unwrap_or_else(|| "Unknown").into(),
            artists: value.artists().map(|a| a.join(", ")).unwrap_or_default(),
            duration: value.length(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaStatus {
    playing: bool,
    position: Option<Duration>,
    loop_mode: LoopMode,
}

impl TryFrom<&mpris::Player> for MediaStatus {
    type Error = ();

    fn try_from(value: &mpris::Player) -> Result<Self, Self::Error> {
        let Ok(status) = value.get_playback_status() else { return Err(()) };

        if status == mpris::PlaybackStatus::Stopped { return Err(()) }

        let position = value.get_position().ok();
        let loop_status = value.get_loop_status().ok();

        Ok(MediaStatus {
            playing: status == mpris::PlaybackStatus::Playing,
            position,
            loop_mode: loop_status.map(|s| s.into()).unwrap_or(LoopMode::None),
        })
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum LoopMode {
    None,
    LoopOne,
    LoopAll,
}

impl From<mpris::LoopStatus> for LoopMode {
    fn from(value: mpris::LoopStatus) -> Self {
        match value {
            mpris::LoopStatus::None => LoopMode::None,
            mpris::LoopStatus::Track => LoopMode::LoopOne,
            mpris::LoopStatus::Playlist => LoopMode::LoopAll,
        }
    }
}
