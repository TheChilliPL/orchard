#![cfg(feature = "obs")]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use async_trait::async_trait;
use obws::error::Error;
use obws::responses::general::Version;
use obws::responses::recording::RecordStatus;
use obws::responses::streaming::StreamStatus;
use rumqttc::{AsyncClient, QoS};
use serde::Deserialize;
use tokio::sync::{Mutex, MutexGuard, Notify, TryLockError};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tracing::{debug, info, trace, warn};
use crate::discovery::{Availability, AvailabilityMode, DiscoveryComponent, SensorDeviceClass, SensorSpec};
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

    async fn try_update(&mut self, hostname: &str, mqttc: &AsyncClient, client: &obws::Client) -> Result<(), obws::error::Error> {
        let ver = client.general().version().await?;
        debug!("Connected to OBS {} with server {} on {}.", ver.obs_studio_version, ver.obs_web_socket_version, ver.platform_description);

        let recording = {
            let status = client.recording().status().await?;
            match status {
                RecordStatus { active: true, paused: false, .. } => "Recording",
                RecordStatus { active: true, paused: true, .. } => "Paused",
                RecordStatus { active: false, .. } => "Stopped",
            }
        };

        let streaming = {
            let status = client.streaming().status().await?;
            match status {
                StreamStatus { active: true, .. } => "Streaming",
                StreamStatus { active: false, .. } => "Stopped",
            }
        };

        mqttc.publish(
            format!("orchard/{hostname}/obs/recording"),
            QoS::AtMostOnce,
            true,
            recording,
        ).await.unwrap();

        mqttc.publish(
            format!("orchard/{hostname}/obs/streaming"),
            QoS::AtMostOnce,
            true,
            streaming,
        ).await.unwrap();

        Ok(())
    }
}

#[async_trait]
impl Module for ObsModule {
    fn name(&self) -> &'static str {
        "OBS module"
    }

    async fn init(&mut self, hostname: &str, mqttc: &AsyncClient) {
        mqttc.publish(
            format!("orchard/{hostname}/obs/available"),
            QoS::AtLeastOnce,
            true,
            "offline"
        ).await.unwrap();
    }

    fn discovery_components(&self, hostname: &str) -> HashMap<String, DiscoveryComponent> {
        let availability = vec![
            Availability {
                topic: format!("orchard/{hostname}/obs/available"),
                ..Default::default()
            },
            Availability {
                topic: format!("orchard/{hostname}/status"),
                ..Default::default()
            },
        ];
        HashMap::from([
            ("recording".into(), DiscoveryComponent {
                unique_id: format!("orchard-{hostname}-obs-recording"),
                name: "OBS recording state".into(),
                spec: SensorSpec {
                    state_topic: format!("orchard/{hostname}/obs/recording"),
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
            ("streaming".into(), DiscoveryComponent {
                unique_id: format!("orchard-{hostname}-obs-streaming"),
                name: "OBS streaming state".into(),
                spec: SensorSpec {
                    state_topic: format!("orchard/{hostname}/obs/streaming"),
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
        ])
    }

    async fn update(&mut self, hostname: &str, mqttc: &AsyncClient) {

        match timeout(Duration::from_millis(100), self.client.clone().lock()).await {
            Ok(mut guard) => {
                match guard.as_ref() {
                    None => {
                        trace!("Client not available");
                        mqttc.publish(
                            format!("orchard/{hostname}/obs/available"),
                            QoS::AtMostOnce,
                            false,
                            "offline".to_string(),
                        ).await.unwrap();
                    }
                    Some(client) => match self.try_update(hostname, mqttc, client).await {
                        Ok(()) => {
                            mqttc.publish(
                                format!("orchard/{hostname}/obs/available"),
                                QoS::AtMostOnce,
                                false,
                                "online".to_string(),
                            ).await.unwrap();
                        }
                        Err(e) => {
                            warn!("Couldn't connect to OBS server! {e}");
                            *guard = None;
                            self.reconnect_needed.notify_one();
                            mqttc.publish(
                                format!("orchard/{hostname}/obs/available"),
                                QoS::AtMostOnce,
                                false,
                                "offline".to_string(),
                            ).await.unwrap();
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


