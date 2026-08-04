#![cfg(feature = "obs")]
/// # OBS Module
///
/// ## MQTT
///
/// ### Published topics
///
/// - `obs/scene/uuid` — UUID of the current scene.
/// - `obs/scene/name` — Name of the current scene. Note that scenes may be renamed.
/// - `obs/recording` — Recording state, one of: Stopped, Paused, Recording.
/// - `obs/recording/duration` — Recording duration in seconds. If recording is stopped, set to `Unknown`.
/// - `obs/streaming` — Streaming state, one of: Stopped, Streaming.
/// - `obs/streaming/duration` — Streaming duration in seconds. If streaming is stopped, set to `Unknown`.
/// - `obs/virtual_cam` — Virtual camera state, one of: Stopped, Active.
///
/// ### Commands
///
/// - `obs/recording/command` — Topic for recording controls.
///     - `start` — Starts recording.
///     - `pause` — Pauses recording.
///     - `resume` — Resumes recording.
///     - `toggle_pause` — Pauses or resumes recording.
///     - `stop` — Stops recording.
///     - `toggle` — Toggles recording.
/// - `obs/streaming/command` — Topic for streaming controls.
///     - `start` — Starts streaming.
///     - `stop` — Stops streaming.
///     - `toggle` — Toggles streaming.
/// - `obs/virtual_cam/command` — Topic for virtual camera controls.
///     - `start` — Starts virtual camera.
///     - `stop` — Stops virtual camera.
///     - `toggle` — Toggles virtual camera.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use async_trait::async_trait;
use obws::responses::recording::RecordStatus;
use obws::responses::streaming::StreamStatus;
use rumqttc::QoS;
use serde::Deserialize;
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tracing::{debug, error, trace, warn};
use crate::discovery::{Availability, AvailabilityMode, ButtonSpec, DiscoveryComponent, SensorDeviceClass, SensorSpec};
use crate::mqtt::scope::MqttScope;
use crate::modules::Module;
use crate::utils::exponential_backoff::{ExponentialBackoff, ExponentialBackoffConfig};

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ObsModuleConfig {
    #[serde(default)]
    host: Option<String>,
    #[serde(default)]
    port: Option<u16>,
    #[serde(default)]
    password: Option<String>,
    #[serde(default)]
    tls: bool,
    #[serde(default)]
    exponential_backoff: ExponentialBackoffConfig,
}

pub struct ObsModule {
    _reconnect_task: JoinHandle<()>,
    reconnect_needed: Arc<Notify>,
    client: Arc<Mutex<Option<obws::Client>>>,
}

impl ObsModule {
    pub fn new(config: &ObsModuleConfig) -> Self {
        let host = config.host.clone().unwrap_or_else(|| "127.0.0.1".to_string());
        let port = config.port.unwrap_or(4455);
        let password = config.password.clone();
        let tls = config.tls;
        let backoff_config = config.exponential_backoff.clone();

        let client: Arc<Mutex<Option<obws::Client>>> = Arc::new(Mutex::new(None));
        let notify = Arc::new(Notify::new());

        let reconnect_task = {
            let client = client.clone();
            let notify = notify.clone();

            tokio::spawn(async move {
                let mut backoff = ExponentialBackoff::new_or_default(backoff_config);

                loop {
                    let mut client = client.lock().await;

                    if client.is_some() {
                        drop(client);
                        debug!("Client available. Reconnection task sleeping.");
                        notify.notified().await;
                        debug!("Reconnection task woke up.");
                    } else {
                        let new_client = obws::Client::connect_with_config(obws::client::ConnectConfig {
                            host: &host,
                            port,
                            dangerous: None,
                            password: password.as_ref(),
                            event_subscriptions: None,
                            tls,
                            broadcast_capacity: obws::client::DEFAULT_BROADCAST_CAPACITY,
                            connect_timeout: obws::client::DEFAULT_CONNECT_TIMEOUT,
                        }).await;

                        match new_client {
                            Ok(c) => {
                                debug!("Client connected.");
                                *client = Some(c);
                                backoff.reset();
                            }
                            Err(e) => {
                                warn!("Error connecting to client: {e:?}");
                                drop(client);
                                sleep(backoff.next()).await;
                            }
                        }
                    }
                }
            })
        };

        ObsModule {
            _reconnect_task: reconnect_task,
            reconnect_needed: notify,
            client,
        }
    }

    async fn try_update(&mut self, mqtt: &MqttScope<'_>, client: &obws::Client) -> Result<(), obws::error::Error> {
        let ver = client.general().version().await?;
        trace!("Connected to OBS {} with server {} on {}.", ver.obs_studio_version, ver.obs_web_socket_version, ver.platform_description);

        let scenes = client.scenes().list().await?;
        let scene = scenes.current_program_scene;

        if let Some(scene) = scene {
            mqtt.publish("obs/scene/uuid", scene.uuid.to_string()).await.unwrap();
            mqtt.publish("obs/scene/name", scene.name.to_string()).await.unwrap();
        } else {
            mqtt.publish("obs/scene/uuid", "").await.unwrap();
            mqtt.publish("obs/scene/name", "").await.unwrap();
        }

        let recording = client.recording().status().await?;
        if recording.active {
            let duration = recording.duration.as_seconds_f32();
            mqtt.publish("obs/recording", if recording.paused { "Paused" } else { "Recording" }).retained().await.unwrap();
            mqtt.publish("obs/recording/duration", duration.to_string()).await.unwrap();
        } else {
            mqtt.publish("obs/recording", "Stopped").retained().await.unwrap();
            mqtt.publish("obs/recording/duration", "None").await.unwrap();
        }

        let streaming = client.streaming().status().await?;
        if streaming.active {
            let duration = streaming.duration.as_seconds_f32();
            mqtt.publish("obs/streaming", "Streaming").retained().await.unwrap();
            mqtt.publish("obs/streaming/duration", duration.to_string()).await.unwrap();
        } else {
            mqtt.publish("obs/streaming", "Stopped").retained().await.unwrap();
            mqtt.publish("obs/streaming/duration", "None").await.unwrap();
        }

        let virtual_cam_on = client.virtual_cam().status().await?;
        mqtt.publish("obs/virtual_cam", if virtual_cam_on { "Active" } else { "Stopped" }).retained().await.unwrap();

        Ok(())
    }
}

#[async_trait]
impl Module for ObsModule {
    fn name(&self) -> &'static str {
        "OBS module"
    }

    fn discovery_components(&self) -> HashMap<String, DiscoveryComponent> {
        let availability = vec![
            Availability {
                topic: "obs/available".into(),
                ..Default::default()
            },
            Availability {
                topic: "status".into(),
                ..Default::default()
            },
        ];
        HashMap::from([
            ("scene-uuid".into(), DiscoveryComponent {
                unique_id: "obs-scene-uuid".into(),
                name: "OBS current scene UUID".into(),
                spec: SensorSpec {
                    state_topic: "obs/scene/uuid".into(),
                    ..Default::default()
                }.into(),
                availability: availability.clone(),
                availability_mode: AvailabilityMode::All,
                icon: Some("mdi:projector-screen".into()),
                ..Default::default()
            }),
            ("scene-name".into(), DiscoveryComponent {
                unique_id: "obs-scene-name".into(),
                name: "OBS current scene name".into(),
                spec: SensorSpec {
                    state_topic: "obs/scene/name".into(),
                    ..Default::default()
                }.into(),
                availability: availability.clone(),
                availability_mode: AvailabilityMode::All,
                icon: Some("mdi:projector-screen".into()),
                ..Default::default()
            }),
            ("recording".into(), DiscoveryComponent {
                unique_id: "obs-recording".into(),
                name: "OBS recording state".into(),
                spec: SensorSpec {
                    state_topic: "obs/recording".into(),
                    device_class: Some(SensorDeviceClass::Enum),
                    options: Some(vec![
                        "Recording".to_string(),
                        "Paused".to_string(),
                        "Stopped".to_string(),
                    ]),
                    ..Default::default()
                }.into(),
                availability: availability.clone(),
                availability_mode: AvailabilityMode::All,
                icon: Some("mdi:record-rec".into()),
                ..Default::default()
            }),
            ("recording-duration".into(), DiscoveryComponent {
                unique_id: "obs-recording-duration".into(),
                name: "OBS recording duration".into(),
                spec: SensorSpec {
                    state_topic: "obs/recording/duration".into(),
                    device_class: Some(SensorDeviceClass::Duration),
                    unit_of_measurement: Some("s".into()),
                    suggested_display_precision: Some(0),
                    ..Default::default()
                }.into(),
                availability: availability.clone(),
                availability_mode: AvailabilityMode::All,
                icon: Some("mdi:record-rec".into()),
                ..Default::default()
            }),
            ("streaming".into(), DiscoveryComponent {
                unique_id: "obs-streaming".into(),
                name: "OBS streaming state".into(),
                spec: SensorSpec {
                    state_topic: "obs/streaming".into(),
                    device_class: Some(SensorDeviceClass::Enum),
                    options: Some(vec![
                        "Streaming".to_string(),
                        "Stopped".to_string(),
                    ]),
                    ..Default::default()
                }.into(),
                availability: availability.clone(),
                availability_mode: AvailabilityMode::All,
                icon: Some("mdi:broadcast".into()),
                ..Default::default()
            }),
            ("streaming-duration".into(), DiscoveryComponent {
                unique_id: "obs-streaming-duration".into(),
                name: "OBS streaming duration".into(),
                spec: SensorSpec {
                    state_topic: "obs/streaming/duration".into(),
                    device_class: Some(SensorDeviceClass::Duration),
                    unit_of_measurement: Some("s".into()),
                    suggested_display_precision: Some(0),
                    ..Default::default()
                }.into(),
                availability: availability.clone(),
                availability_mode: AvailabilityMode::All,
                icon: Some("mdi:broadcast".into()),
                ..Default::default()
            }),
            ("virtual_cam".into(), DiscoveryComponent {
                unique_id: "obs-virtual_cam".into(),
                name: "OBS Virtual cam state".into(),
                spec: SensorSpec {
                    state_topic: "obs/virtual_cam".into(),
                    device_class: Some(SensorDeviceClass::Enum),
                    options: Some(vec![
                        "Active".to_string(),
                        "Stopped".to_string(),
                    ]),
                    ..Default::default()
                }.into(),
                availability: availability.clone(),
                availability_mode: AvailabilityMode::All,
                icon: Some("mdi:fit-to-screen".into()),
                ..Default::default()
            }),
            ("recording-start".into(), DiscoveryComponent {
                unique_id: "obs-recording-start".into(),
                name: "Start OBS recording".into(),
                spec: ButtonSpec {
                    command_topic: "obs/recording/command".into(),
                    payload_press: "start".into(),
                    ..Default::default()
                }.into(),
                availability: availability.clone(),
                availability_mode: AvailabilityMode::All,
                icon: Some("mdi:record-rec".into()),
                ..Default::default()
            }),
            ("recording-pause".into(), DiscoveryComponent {
                unique_id: "obs-recording-pause".into(),
                name: "Pause OBS recording".into(),
                spec: ButtonSpec {
                    command_topic: "obs/recording/command".into(),
                    payload_press: "pause".into(),
                    ..Default::default()
                }.into(),
                availability: availability.clone(),
                availability_mode: AvailabilityMode::All,
                icon: Some("mdi:pause".into()),
                ..Default::default()
            }),
            ("recording-resume".into(), DiscoveryComponent {
                unique_id: "obs-recording-resume".into(),
                name: "Resume OBS recording".into(),
                spec: ButtonSpec {
                    command_topic: "obs/recording/command".into(),
                    payload_press: "resume".into(),
                    ..Default::default()
                }.into(),
                availability: availability.clone(),
                availability_mode: AvailabilityMode::All,
                icon: Some("mdi:play".into()),
                ..Default::default()
            }),
            ("recording-toggle_pause".into(), DiscoveryComponent {
                unique_id: "obs-recording-toggle_pause".into(),
                name: "Pause/resume OBS recording".into(),
                spec: ButtonSpec {
                    command_topic: "obs/recording/command".into(),
                    payload_press: "toggle_pause".into(),
                    ..Default::default()
                }.into(),
                availability: availability.clone(),
                availability_mode: AvailabilityMode::All,
                icon: Some("mdi:play-pause".into()),
                ..Default::default()
            }),
            ("recording-stop".into(), DiscoveryComponent {
                unique_id: "obs-recording-stop".into(),
                name: "Stop OBS recording".into(),
                spec: ButtonSpec {
                    command_topic: "obs/recording/command".into(),
                    payload_press: "stop".into(),
                    ..Default::default()
                }.into(),
                availability: availability.clone(),
                availability_mode: AvailabilityMode::All,
                icon: Some("mdi:stop".into()),
                ..Default::default()
            }),
            ("recording-toggle".into(), DiscoveryComponent {
                unique_id: "obs-recording-toggle".into(),
                name: "Toggle OBS recording".into(),
                spec: ButtonSpec {
                    command_topic: "obs/recording/command".into(),
                    payload_press: "toggle".into(),
                    ..Default::default()
                }.into(),
                availability: availability.clone(),
                availability_mode: AvailabilityMode::All,
                icon: Some("mdi:record-rec".into()),
                ..Default::default()
            }),
            ("streaming-start".into(), DiscoveryComponent {
                unique_id: "obs-streaming-start".into(),
                name: "Start OBS streaming".into(),
                spec: ButtonSpec {
                    command_topic: "obs/streaming/command".into(),
                    payload_press: "start".into(),
                    ..Default::default()
                }.into(),
                availability: availability.clone(),
                availability_mode: AvailabilityMode::All,
                icon: Some("mdi:broadcast".into()),
                ..Default::default()
            }),
            ("streaming-stop".into(), DiscoveryComponent {
                unique_id: "obs-streaming-stop".into(),
                name: "Stop OBS streaming".into(),
                spec: ButtonSpec {
                    command_topic: "obs/streaming/command".into(),
                    payload_press: "stop".into(),
                    ..Default::default()
                }.into(),
                availability: availability.clone(),
                availability_mode: AvailabilityMode::All,
                icon: Some("mdi:stop".into()),
                ..Default::default()
            }),
            ("streaming-toggle".into(), DiscoveryComponent {
                unique_id: "obs-streaming-toggle".into(),
                name: "Toggle OBS streaming".into(),
                spec: ButtonSpec {
                    command_topic: "obs/streaming/command".into(),
                    payload_press: "toggle".into(),
                    ..Default::default()
                }.into(),
                availability: availability.clone(),
                availability_mode: AvailabilityMode::All,
                icon: Some("mdi:broadcast".into()),
                ..Default::default()
            }),
            ("virtual_cam-start".into(), DiscoveryComponent {
                unique_id: "obs-virtual_cam-start".into(),
                name: "Start OBS virtual cam".into(),
                spec: ButtonSpec {
                    command_topic: "obs/virtual_cam/command".into(),
                    payload_press: "start".into(),
                    ..Default::default()
                }.into(),
                availability: availability.clone(),
                availability_mode: AvailabilityMode::All,
                icon: Some("mdi:fit-to-screen".into()),
                ..Default::default()
            }),
            ("virtual_cam-stop".into(), DiscoveryComponent {
                unique_id: "obs-virtual_cam-stop".into(),
                name: "Stop OBS virtual cam".into(),
                spec: ButtonSpec {
                    command_topic: "obs/virtual_cam/command".into(),
                    payload_press: "stop".into(),
                    ..Default::default()
                }.into(),
                availability: availability.clone(),
                availability_mode: AvailabilityMode::All,
                icon: Some("mdi:stop".into()),
                ..Default::default()
            }),
            ("virtual_cam-toggle".into(), DiscoveryComponent {
                unique_id: "obs-virtual_cam-toggle".into(),
                name: "Toggle OBS virtual cam".into(),
                spec: ButtonSpec {
                    command_topic: "obs/virtual_cam/command".into(),
                    payload_press: "toggle".into(),
                    ..Default::default()
                }.into(),
                availability: availability.clone(),
                availability_mode: AvailabilityMode::All,
                icon: Some("mdi:fit-to-screen".into()),
                ..Default::default()
            }),
        ])
    }

    fn subscriptions(&self) -> Vec<String> {
        vec![
            "obs/recording/command".into(),
            "obs/streaming/command".into(),
            "obs/virtual_cam/command".into()
        ]
    }

    async fn init(&mut self, mqtt: &MqttScope) {
        // Prevent status from showing up as online at start even when OBS is not available
        // May be overridden on first update
        mqtt.publish("obs/available", "offline")
            .retained()
            .with_qos(QoS::AtLeastOnce)
            .await
            .unwrap();
    }

    async fn handle_message(&mut self, _mqtt: &MqttScope, topic: &str, payload: &[u8]) {
        let client = self.client.lock().await;
        let Some(client) = client.as_ref() else {
            error!("Can't handle command! Client is not available.");
            return;
        };

        let payload = str::from_utf8(payload).unwrap();

        match topic {
            "obs/recording/command" => match payload {
                "start" => _ = client.recording().start().await,
                "pause" => _ = client.recording().pause().await,
                "resume" => _ = client.recording().resume().await,
                "toggle_pause" => _ = client.recording().toggle_pause().await,
                "stop" => _ = client.recording().stop().await,
                "toggle" => _ = client.recording().toggle().await,
                _other => warn!("Unrecognized recording command: {_other}."),
            },
            "obs/streaming/command" => match payload {
                "start" => _ = client.streaming().start().await,
                "stop" => _ = client.streaming().stop().await,
                "toggle" => _ = client.streaming().toggle().await,
                _other => warn!("Unrecognized streaming command: {_other}."),
            },
            "obs/virtual_cam/command" => match payload {
                "start" => _ = client.virtual_cam().start().await,
                "stop" => _ = client.virtual_cam().stop().await,
                "toggle" => _ = client.virtual_cam().toggle().await,
                _other => warn!("Unrecognized virtual camera command: {_other}."),
            },
            _other => warn!("Unrecognized command topic: {_other}."),
        }
    }

    async fn update(&mut self, mqtt: &MqttScope) {
        match timeout(Duration::from_millis(100), self.client.clone().lock()).await {
            Ok(mut guard) => {
                match guard.as_ref() {
                    None => {
                        trace!("Client not available");
                        mqtt.publish("obs/available", "offline")
                            .await
                            .unwrap();
                    }
                    Some(client) => match self.try_update(mqtt, client).await {
                        Ok(()) => {
                            mqtt.publish("obs/available", "online")
                                .await
                                .unwrap();
                        }
                        Err(e) => {
                            warn!("Couldn't connect to OBS server! {e}");
                            *guard = None;
                            self.reconnect_needed.notify_one();
                            mqtt.publish("obs/available", "offline")
                                .await
                                .unwrap();
                        }
                    }
                }
            }
            Err(_) => {
                warn!("Couldn't lock client");
            }
        }
    }
}
