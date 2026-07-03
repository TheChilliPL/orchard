#![cfg(target_os = "linux")]

use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt::format;
use std::sync::Mutex;
use std::time::Duration;
use async_trait::async_trait;
use rumqttc::{AsyncClient, QoS};
use tracing::{debug, trace, warn};
use crate::discovery::{ButtonSpec, DiscoveryComponent, SensorDeviceClass, SensorSpec};
use crate::modules::Module;

#[derive(Default)]
pub struct MediaModule {
    last_metadata: Option<MediaMetadata>,
    last_status: Option<MediaStatus>,
    force_update: bool,
}

impl MediaModule {
    pub fn new() -> Self { Default::default() }

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

    fn discovery_components(&self, hostname: &str) -> HashMap<String, DiscoveryComponent> {
        HashMap::from([
            ("media_toggle".into(), DiscoveryComponent {
                unique_id: format!("orchard-{hostname}-media_play_pause"),
                name: "Play/pause".into(),
                spec: ButtonSpec {
                    command_topic: format!("orchard/{hostname}/media/toggle"),
                    ..Default::default()
                }.into(),
                icon: Some("mdi:play-pause".to_string()),
                ..Default::default()
            }),
            ("media_prev".into(), DiscoveryComponent {
                unique_id: format!("orchard-{hostname}-media_prev"),
                name: "Previous".into(),
                spec: ButtonSpec {
                    command_topic: format!("orchard/{hostname}/media/prev"),
                    ..Default::default()
                }.into(),
                icon: Some("mdi:skip-previous".into()),
                ..Default::default()
            }),
            ("media_next".into(), DiscoveryComponent {
                unique_id: format!("orchard-{hostname}-media_next"),
                name: "Next".into(),
                spec: ButtonSpec {
                    command_topic: format!("orchard/{hostname}/media/next"),
                    ..Default::default()
                }.into(),
                icon: Some("mdi:skip-next".into()),
                ..Default::default()
            }),
            ("media_status".into(), DiscoveryComponent {
                unique_id: format!("orchard-{hostname}-media_status"),
                name: "Media status".into(),
                spec: SensorSpec {
                    state_topic: format!("orchard/{hostname}/media/status"),
                    device_class: Some(SensorDeviceClass::Enum),
                    options: Some(vec!["Playing".into(), "Paused".into(), "Stopped".into()]),
                    value_template: Some("{{value | capitalize}}".into()),
                    ..Default::default()
                }.into(),
                icon: Some("mdi:play-box".into()),
                ..Default::default()
            }),
            ("media_title".into(), DiscoveryComponent {
                unique_id: format!("orchard-{hostname}-media_title"),
                name: "Media title".into(),
                spec: SensorSpec {
                    state_topic: format!("orchard/{hostname}/media/title"),
                    ..Default::default()
                }.into(),
                icon: Some("mdi:music-note".into()),
                ..Default::default()
            }),
            ("media_artists".into(), DiscoveryComponent {
                unique_id: format!("orchard-{hostname}-media_artists"),
                name: "Media artists".into(),
                spec: SensorSpec {
                    state_topic: format!("orchard/{hostname}/media/artists"),
                    ..Default::default()
                }.into(),
                icon: Some("mdi:account".into()),
                ..Default::default()
            }),
            ("media_position".into(), DiscoveryComponent {
                unique_id: format!("orchard-{hostname}-media_position"),
                name: "Media position".into(),
                spec: SensorSpec {
                    state_topic: format!("orchard/{hostname}/media/position"),
                    device_class: Some(SensorDeviceClass::Duration),
                    unit_of_measurement: Some("s".into()),
                    ..Default::default()
                }.into(),
                ..Default::default()
            }),
            ("media_duration".into(), DiscoveryComponent {
                unique_id: format!("orchard-{hostname}-media_duration"),
                name: "Media duration".into(),
                spec: SensorSpec {
                    state_topic: format!("orchard/{hostname}/media/duration"),
                    device_class: Some(SensorDeviceClass::Duration),
                    unit_of_measurement: Some("s".into()),
                    ..Default::default()
                }.into(),
                ..Default::default()
            }),

        ])
    }

    fn subscriptions(&self, hostname: &str) -> Vec<String> {
        vec![
            format!("orchard/{hostname}/media/toggle"),
            format!("orchard/{hostname}/media/prev"),
            format!("orchard/{hostname}/media/next"),
            format!("orchard/{hostname}/media/position/set"), // TODO implement
        ]
    }

    async fn init(&mut self, hostname: &str, mqttc: &AsyncClient) {
        self.force_update = true;
        self.update(hostname, mqttc).await;
    }

    async fn handle_message(&mut self, hostname: &str, mqttc: &AsyncClient, topic: &str, payload: &[u8]) {
        let Ok(payload) = std::str::from_utf8(payload) else {
            warn!(topic, "Ignoring non-UTF8 payload.");
            return;
        };

        if topic == format!("orchard/{hostname}/media/toggle") {
            self.active_player().map(|p| p.play_pause());
        } else if topic == format!("orchard/{hostname}/media/prev") {
            self.active_player().map(|p| p.previous());
        } else if topic == format!("orchard/{hostname}/media/next") {
            self.active_player().map(|p| p.next());
        } else {
            warn!(topic, ?payload, "Ignoring unknown topic.");
        }

        self.update(hostname, mqttc).await;
    }

    async fn update(&mut self, hostname: &str, mqttc: &AsyncClient) {
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

        if status != self.last_status || self.force_update {
            let status_text = match status {
                Some(MediaStatus { playing, .. }) => if playing { "Playing" } else { "Paused" },
                None => "Stopped",
            };

            let position_text = match status {
                Some(MediaStatus { position, .. }) => position.map(|p| p.as_secs_f32().to_string()).unwrap_or_else(|| "None".into()),
                None => "None".into(),
            };

            trace!("Publishing media status.");

            mqttc.publish(format!("orchard/{hostname}/media/status"), QoS::AtMostOnce, true, status_text).await.unwrap();
            mqttc.publish(format!("orchard/{hostname}/media/position"), QoS::AtMostOnce, true, position_text).await.unwrap();

            self.last_status = status;
        }

        if metadata != self.last_metadata || self.force_update {
            let title = metadata.as_ref().map_or("", |m| &m.title);
            let artists = metadata.as_ref().map_or("", |m| &m.artists);
            let duration_text = metadata.as_ref().map_or_else(|| "None".into(), |m| m.duration.map(|d| d.as_secs_f32().to_string()).unwrap_or_else(|| "None".into()));

            debug!("Publishing media metadata.");

            mqttc.publish(format!("orchard/{hostname}/media/title"), QoS::AtMostOnce, true, title).await.unwrap();
            mqttc.publish(format!("orchard/{hostname}/media/artists"), QoS::AtMostOnce, true, artists).await.unwrap();
            mqttc.publish(format!("orchard/{hostname}/media/duration"), QoS::AtMostOnce, true, duration_text).await.unwrap();

            self.last_metadata = metadata;
        }

        self.force_update = false;
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
