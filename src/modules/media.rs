#![cfg(target_os = "linux")]

use std::collections::HashMap;
use std::fmt::format;
use async_trait::async_trait;
use mpris::{PlaybackStatus, PlayerFinder};
use rumqttc::{AsyncClient, QoS};
use tracing::{debug, warn};
use crate::discovery::{ButtonSpec, DiscoveryComponent, SensorDeviceClass, SensorSpec};
use crate::modules::Module;

pub struct MediaModule;

impl MediaModule {
    pub fn new() -> Self { Self }
}

#[async_trait]
impl Module for MediaModule {
    fn name(&self) -> &'static str {
        "Media control module"
    }

    fn discovery_components(&self, hostname: &str) -> HashMap<String, DiscoveryComponent> {
        HashMap::from([
            ("media_toggle".into(), DiscoveryComponent {
                unique_id: format!("orchard-{}-media_play_pause", hostname),
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
        ])
    }

    fn subscriptions(&self, hostname: &str) -> Vec<String> {
        vec![
            format!("orchard/{hostname}/media/toggle"),
            format!("orchard/{hostname}/media/prev"),
            format!("orchard/{hostname}/media/next"),
        ]
    }

    async fn init(&mut self, hostname: &str, mqttc: &AsyncClient) {
        self.update(hostname, mqttc).await;
    }

    async fn handle_message(&mut self, _hostname: &str, _mqttc: &AsyncClient, _topic: &str, _payload: &[u8]) {
        // TODO
    }

    async fn update(&mut self, hostname: &str, mqttc: &AsyncClient) {
        let mut status = "stopped";
        let mut title = "".into();
        let mut artists = "".into();

        debug!("Trying to get metadata from MPRIS.");
        'get_metadata: {
            let player_finder = PlayerFinder::new().unwrap();
            let Ok(player) = player_finder.find_active() else { debug!("No player found."); break 'get_metadata };
            debug!("Found active player, trying to get metadata.");
            let metadata = player.get_metadata();
            status = match player.get_playback_status() {
                Ok(PlaybackStatus::Playing) => "playing",
                Ok(PlaybackStatus::Paused) => "paused",
                Ok(PlaybackStatus::Stopped) => "stopped",
                Err(_) => "unknown",
            };

            if let Ok(m) = metadata {
                title = m.title().unwrap_or_else(|| "Unknown").to_owned();
                artists = m.artists().map(|a| a.join(", ")).unwrap_or_else(|| "Unknown".into());
            } else {
                warn!(?metadata, "Failed to get metadata");
                title = "Unknown".into();
                artists = "Unknown".into();
            }
        }

        debug!("Publishing media status: {}.", status);

        mqttc.publish(format!("orchard/{hostname}/media/status"), QoS::AtMostOnce, true, status).await.unwrap();
        mqttc.publish(format!("orchard/{hostname}/media/title"), QoS::AtMostOnce, true, title).await.unwrap();
        mqttc.publish(format!("orchard/{hostname}/media/artists"), QoS::AtMostOnce, true, artists).await.unwrap();
    }
}
