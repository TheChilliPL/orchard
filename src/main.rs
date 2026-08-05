#![feature(impl_trait_in_assoc_type)]
#![feature(debug_closure_helpers)]

mod prelude;
mod mqtt;
mod discovery;
mod modules;
mod utils;
pub mod config;

use std::collections::HashMap;
use std::path::PathBuf;
use std::thread::scope;
use std::time::Duration;
use clap::Parser;
use eyre::{eyre, Context};
use futures::future::join_all;
use mqtt::{MqttOptions, connect_mqtt, topic_matches};
use prelude::*;
use rumqttc::{Event, Packet, QoS};
use tokio::sync::mpsc;
use tokio::time;
use tracing::metadata::LevelFilter;
use tracing_subscriber::util::SubscriberInitExt;
use crate::config::Config;
use crate::discovery::{DiscoveryPayload, DiscoveryDevice, DiscoveryOrigin, Availability, DiscoveryComponent};
use crate::modules::media::MediaModule;
use crate::modules::Module;
use crate::modules::status::StatusModule;
use crate::modules::sysinfo::SysInfoModule;
use crate::modules::system_control::SystemControlModule;
use crate::modules::volume::VolumeModule;
use crate::mqtt::scope::{ClientExt, MqttScope, Scopeable};

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

    /// Specifies the config path to load.
    ///
    /// By default, chooses one depending on the system.
    /// Linux: `~/.config/orchard/config.toml`,
    /// Windows: `%APPDATA%/Orchard/config.toml`,
    /// MacOS: `~/Library/Application Support/dev.thechilli.orchard/config.toml`.
    #[arg(short, long = "config", env = "ORCHARD_CONFIG")]
    config_path: Option<PathBuf>,
}

async fn init_module(
    module: &mut dyn Module,
    mqtt: &MqttScope<'_>,
) -> eyre::Result<(HashMap<String, DiscoveryComponent>, Vec<String>)> {
    let module_name = module.name();
    let discovery_components = module.discovery_components().wrap_err_with(|| format!("couldn't get discovery components for module {module_name}"))?;
    let subscriptions = module.subscriptions().wrap_err_with(|| format!("couldn't get subscriptions for module {module_name}"))?;

    module.init(mqtt).await?;
    Ok((discovery_components, subscriptions))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> eyre::Result<()> {
    init_tracing();

    _ = dotenv::dotenv().ok();

    let cli = Cli::parse();
    let opts = MqttOptions::from(&cli);

    let ref hostname = opts.hostname.clone();

    info!(?opts, "Orchard daemon starting!");
    let (mqtt_client, mut event_loop) = connect_mqtt(opts).await.expect("MQTT connection failed");
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
                    // trace!(?packet, "Outgoing packet.");
                }
                Err(err) => {
                    error!(error = ?err, "MQTT event loop failed.");
                    break;
                }
            }
        }
    });

    let mqtt_scope = mqtt_client.scope(format!("orchard/{hostname}"));

    let mut components = HashMap::with_capacity(32);
    let mut subscriptions: Vec<(String, usize)> = Vec::with_capacity(32);

    let config = match Config::load(cli.config_path.as_deref()) {
        Ok(c) => c,
        Err(e) => {
            panic!("Failed to load config: {}", e);
        }
    };

    let mut modules = config.load_modules();

    for (module_idx, module) in modules.iter_mut().enumerate() {
        let module_name = module.name();
        let (discovery_components, module_subscriptions) = match init_module(module.as_mut(), &mqtt_scope).await {
            Ok(it) => it,
            Err(e) => {
                error!("Couldn't initialize module {module_name}: {e:?}");
                continue;
            }
        };

        if !discovery_components.is_empty() {
            trace!(?discovery_components, "Module {module_name} added discovery components.");
            components.extend(discovery_components);
        }

        if !module_subscriptions.is_empty() {
            trace!(subscriptions = ?module_subscriptions, "Module {module_name} added subscriptions.");
            subscriptions.extend(
                module_subscriptions
                    .into_iter()
                    .map(|topic| (topic, module_idx))
            );
        }
    }

    for topic in subscriptions.iter().map(|(topic, _)| topic) {
        mqtt_client.subscribe(mqtt_scope.scope_topic(topic), QoS::AtLeastOnce).await
            .wrap_err("couldn't subscribe to MQTT topics")?;
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
        availability: vec![
            Availability {
                topic: "status".into(),
                ..Default::default()
            },
        ],
        components,
    };

    let discovery = discovery.scope(&mqtt_scope);

    let discovery_topic = format!("homeassistant/device/orchard/{hostname}/config");

    mqtt_client.publish(
        discovery_topic,
        QoS::AtLeastOnce,
        true,
        serde_json::to_string(&discovery).unwrap(),
    ).await
        .wrap_err("couldn't publish discovery data")?;

    let mut update_interval = time::interval(Duration::from_secs(1));

    info!("Orchard started successfully!");

    loop {
        tokio::select! {
            publish = publish_rx.recv() => {
                let Some((topic, payload)) = publish else {
                    error!("MQTT event loop task stopped.");
                    break;
                };

                let Some(topic) = mqtt_scope.unscope_topic(&topic) else {
                    warn!("Couldn't unscope received event topic: {}", topic);
                    continue;
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
                    let module = &mut modules[module_idx];
                    let result = module
                        .handle_message(&mqtt_scope, &topic, &payload)
                        .await;
                    if let Err(e) = result {
                        warn!("Error in module {} when trying to handle a message: {e:?}", module.name());
                    }
                }
            }
            _ = update_interval.tick() => {
                join_all(
                    modules.iter_mut().map(async |m| {
                        trace!("Updating {}", m.name());
                        let result = m.update(&mqtt_scope).await;
                        if let Err(e) = result {
                            warn!("Error in module {} when trying to update: {e:?}", m.name());
                        }
                    })
                ).await;
            }
        }
    }

    Ok(())
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
