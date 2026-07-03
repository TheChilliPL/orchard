mod prelude;
mod mqtt;
mod discovery;
mod modules;
mod utils;

use std::collections::HashMap;
use std::time::Duration;
use clap::Parser;
use mqtt::{MqttOptions, connect_mqtt, topic_matches};
use prelude::*;
use rumqttc::{Event, Packet, QoS};
use tokio::sync::mpsc;
use tokio::time;
use tracing::metadata::LevelFilter;
use tracing_subscriber::util::SubscriberInitExt;
use crate::discovery::{DiscoveryPayload, DiscoveryDevice, DiscoveryOrigin};
use crate::modules::media::MediaModule;
use crate::modules::Module;
use crate::modules::status::StatusModule;
use crate::modules::sysinfo::SysInfoModule;
use crate::modules::system_control::SystemControlModule;
use crate::modules::volume::VolumeModule;

#[derive(clap::Parser)]
struct Cli {
    /// Host of the MQTT broker.
    #[arg(value_name = "MQTT_HOST", env = "MQTT_HOST")]
    host: String,
    /// Port of the MQTT broker.
    #[arg(
        value_name = "MQTT_PORT",
        short,
        long,
        env = "MQTT_PORT",
        default_value_t = 1883
    )]
    port: u16,
    /// Enables using SSL/TLS encrypted transport with the MQTT broker.
    #[arg(short = 'S', long, env = "MQTT_USE_SSL")]
    ssl: bool,
    #[arg(short, long, env = "MQTT_USERNAME", requires = "password")]
    username: Option<String>,
    #[arg(short = 'P', long, env = "MQTT_PASSWORD", requires = "username")]
    password: Option<String>,

    /// Hostname of the client. Used for hierarchical topics in MQTT. Defaults to device hostname.
    #[arg(long, env = "ORCHARD_HOSTNAME")]
    hostname: Option<String>,
    /// Client ID passed to MQTT. Defaults to `orchard-<hostname>`.
    #[arg(long, env = "MQTT_CLIENT_ID")]
    client_id: Option<String>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    init_tracing();

    _ = dotenv::dotenv().ok();

    let cli = Cli::parse();
    let opts = MqttOptions::from(&cli);

    let ref hostname = opts.hostname.clone();

    info!(?opts, "Orchard daemon starting!");
    let (mqttc, mut event_loop) = connect_mqtt(opts).await.expect("MQTT connection failed");
    info!("MQTT connection established!");

    let (publish_tx, mut publish_rx) = mpsc::channel::<(String, Vec<u8>)>(32);
    tokio::spawn(async move {
        loop {
            match event_loop.poll().await {
                Ok(Event::Incoming(packet)) => {
                    trace!(?packet, "Incoming packet.");

                    if let Packet::Publish(publish) = packet {
                        if publish_tx
                            .send((publish.topic, publish.payload.to_vec()))
                            .await
                            .is_err()
                        {
                            warn!("MQTT publish channel closed, stopping event loop task.");
                            break;
                        }
                    }
                }
                Ok(Event::Outgoing(packet)) => {
                    trace!(?packet, "Outgoing packet.");
                }
                Err(err) => {
                    error!(error = ?err, "MQTT event loop failed.");
                    break;
                }
            }
        }
    });

    let mut components = HashMap::with_capacity(32);
    let mut subscriptions: Vec<(String, usize)> = Vec::with_capacity(32);

    let modules: &mut [Box<dyn Module>] = &mut [
        Box::new(StatusModule::new()),
        Box::new(VolumeModule::new()),
        #[cfg(target_os = "linux")]
        Box::new(MediaModule::new()),
        Box::new(SystemControlModule::new()),
        Box::new(SysInfoModule::new()),
    ];

    for (module_idx, module) in modules.iter().enumerate() {
        info!("Preparing module: {}.", module.name());

        let discovery_components = module.discovery_components(hostname);
        if !discovery_components.is_empty() {
            debug!(?discovery_components, "Module {} added discovery components.", module.name());
            components.extend(discovery_components);
        }

        let module_subscriptions = module.subscriptions(hostname);
        if !module_subscriptions.is_empty() {
            debug!(subscriptions = ?module_subscriptions, "Module {} added subscriptions.", module.name());
            subscriptions.extend(
                module_subscriptions
                    .into_iter()
                    .map(|topic| (topic, module_idx))
            );
        }
    }

    for topic in subscriptions.iter().map(|(topic, _)| topic).collect::<std::collections::HashSet<_>>() {
        mqttc.subscribe(topic, QoS::AtLeastOnce).await.unwrap();
    }

    let discovery = DiscoveryPayload {
        device: DiscoveryDevice {
            name: hostname.clone(),
            ids: vec![hostname.clone()],
        },
        origin: DiscoveryOrigin {
            name: env!("CARGO_PKG_NAME").into(),
            version: env!("CARGO_PKG_VERSION").into(),
        },
        availability_topic: format!("orchard/{}/status", hostname),
        components,
    };

    let discovery_topic = format!("homeassistant/device/orchard/{}/config", hostname);

    debug!(topic = discovery_topic, payload = ?discovery, "Sending discovery payload.");

    mqttc.publish(
        discovery_topic,
        QoS::AtLeastOnce,
        true,
        serde_json::to_string(&discovery).unwrap(),
    ).await.unwrap();

    for module in modules.iter_mut() {
        debug!("Initializing module: {}", module.name());
        module.init(hostname, &mqttc).await;
    }

    let mut update_interval = time::interval(Duration::from_secs(1));

    info!("Orchard started successfully!");

    loop {
        tokio::select! {
            publish = publish_rx.recv() => {
                let Some((topic, payload)) = publish else {
                    error!("MQTT event loop task stopped.");
                    break;
                };

                let mut module_idxs = subscriptions
                    .iter()
                    .filter_map(|(topic_filter, module_idx)| {
                        topic_matches(topic_filter, &topic).then_some(*module_idx)
                    })
                    .collect::<Vec<_>>();
                module_idxs.sort_unstable();
                module_idxs.dedup();

                for module_idx in module_idxs {
                    modules[module_idx]
                        .handle_message(hostname, &mqttc, &topic, &payload)
                        .await;
                }
            }
            _ = update_interval.tick() => {
                for module in modules.iter_mut() {
                    module.update(hostname, &mqttc).await;
                }
            }
        }
    }
}

fn init_tracing() {
    use tracing_subscriber::layer::SubscriberExt;

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::filter::EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
}
