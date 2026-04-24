mod prelude;

use std::env;
use std::time::Duration;
use clap::Parser;
use gethostname::gethostname;
use rumqttc::{AsyncClient, LastWill, MqttOptions, QoS, TlsConfiguration, Transport};
use rumqttc::ConnectionError::MqttState;
use tokio::time::sleep;
use tracing::metadata::LevelFilter;
use tracing_subscriber::filter::Directive;
use tracing_subscriber::util::SubscriberInitExt;
use prelude::*;

#[derive(clap::Parser)]
struct Cli {
    /// Host of the MQTT broker.
    #[arg(value_name = "MQTT_HOST", env = "MQTT_HOST")]
    host: String,
    /// Port of the MQTT broker.
    #[arg(value_name = "MQTT_PORT", short, long, env = "MQTT_PORT", default_value_t = 1883)]
    port: u16,
    /// Enables using SSL/TLS encrypted transport with the MQTT broker.
    #[arg(short = 'S', long, env = "MQTT_USE_SSL")]
    ssl: bool,
    #[arg(short, long, env = "MQTT_USERNAME", requires = "password")]
    username: Option<String>,
    #[arg(short = 'P', long, env = "MQTT_PASSWORD", requires = "username")]
    password: Option<String>,

    /// Hostname of the client. Used for hierarchical topics in MQTT. Defaults to device hostname.
    #[arg(long)]
    hostname: Option<String>,
    /// Client ID passed to MQTT. Defaults to `orchard-<hostname>`.
    #[arg(long)]
    client_id: Option<String>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    init_tracing();

    dotenv::dotenv().unwrap();

    let cli = Cli::parse();
    let hostname = cli.hostname.unwrap_or(gethostname().to_string_lossy().to_string());
    let client_id = cli.client_id.unwrap_or_else(|| format!("orchard-{}", hostname));

    info!(hostname, "Orchard daemon starting!");
    let mut mqtt_opts = MqttOptions::new(client_id, cli.host, cli.port);
    if let (Some(username), Some(password)) = (cli.username, cli.password) {
        mqtt_opts.set_credentials(username, password);
    }
    if cli.ssl {
        mqtt_opts.set_transport(Transport::tls_with_default_config());
    }
    mqtt_opts.set_last_will(LastWill::new(format!("orchard/{}/status", hostname), "offline", QoS::AtLeastOnce, false));
    let (mqtt_client, mut event_loop) = AsyncClient::new(mqtt_opts, 10);
    info!("MQTT client started");

    mqtt_client.try_publish(format!("orchard/{}/status", hostname), QoS::AtLeastOnce, false, "online").unwrap();

    loop {
        let notification = event_loop.poll().await.unwrap();
        debug!(?notification);
    }
}

fn init_tracing() {
    use tracing_subscriber::layer::SubscriberExt;

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::filter::EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy()
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
}
