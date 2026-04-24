mod prelude;

use clap::Parser;
use gethostname::gethostname;
use prelude::*;
use rumqttc::{AsyncClient, ClientError, ConnectionError, Event, EventLoop, LastWill, QoS, Transport};
use std::fmt::{Debug, Formatter};
use thiserror::Error;
use tracing::metadata::LevelFilter;
use tracing_subscriber::util::SubscriberInitExt;

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
    #[arg(long)]
    hostname: Option<String>,
    /// Client ID passed to MQTT. Defaults to `orchard-<hostname>`.
    #[arg(long)]
    client_id: Option<String>,
}

#[derive(Debug, Clone)]
struct MqttOptions {
    hostname: String,
    client_id: String,
    broker: String,
    port: u16,
    use_ssl: bool,
    credentials: Option<MqttCredentials>,
}

impl From<&Cli> for MqttOptions {
    fn from(cli: &Cli) -> Self {
        let hostname = cli
            .hostname
            .clone()
            .unwrap_or(gethostname().to_string_lossy().to_string());
        let client_id = cli
            .client_id
            .clone()
            .unwrap_or_else(|| format!("orchard-{}", hostname));

        MqttOptions {
            hostname,
            client_id,
            broker: cli.host.clone(),
            port: cli.port,
            use_ssl: cli.ssl,
            credentials: cli.into(),
        }
    }
}

#[derive(Clone)]
struct MqttCredentials {
    username: String,
    password: String,
}

impl From<&Cli> for Option<MqttCredentials> {
    fn from(cli: &Cli) -> Option<MqttCredentials> {
        if let Some(username) = cli.username.clone()
            && let Some(password) = cli.password.clone()
        {
            Some(MqttCredentials { username, password })
        } else {
            None
        }
    }
}

impl Debug for MqttCredentials {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqttCredentials")
            .field("username", &self.username)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Error)]
enum MqttConnectionError {
    #[error("unexpected event: {0:?}")]
    UnexpectedEvent(Event),
    #[error("connection error: {0}")]
    ConnectionError(#[from] ConnectionError),
    #[error("client error: {0}")]
    ClientError(#[from] ClientError),
}

impl From<Event> for MqttConnectionError {
    fn from(event: Event) -> Self {
        MqttConnectionError::UnexpectedEvent(event)
    }
}

/// Connects to an MQTT broker.
///
/// Returns [Ok] if connection is successful, otherwise returns an [Err].
///
/// The connection is considered successful if the first event received from the broker is a `ConnAck` with `ConnectReturnCode::Success`.
/// All other events should be handled by the application.
async fn connect_mqtt(opts: MqttOptions) -> Result<(AsyncClient, EventLoop), MqttConnectionError> {
    let mut rumqttc_opts = rumqttc::MqttOptions::new(opts.client_id, opts.broker, opts.port);

    if let Some(credentials) = opts.credentials {
        rumqttc_opts.set_credentials(credentials.username, credentials.password);
    }

    if opts.use_ssl {
        rumqttc_opts.set_transport(Transport::tls_with_default_config());
    }

    rumqttc_opts.set_last_will(LastWill::new(
        format!("orchard/{}/status", opts.hostname),
        "offline",
        QoS::AtLeastOnce,
        true,
    ));

    let (mqttc, mut event_loop) = AsyncClient::new(rumqttc_opts, 10);

    let event = event_loop.poll().await?;

    match event {
        Event::Incoming(rumqttc::Packet::ConnAck(rumqttc::mqttbytes::v4::ConnAck {
            code: rumqttc::mqttbytes::v4::ConnectReturnCode::Success,
            ..
        })) => {
            // Connection successful
            debug!(?event, "Connection established successfully.");
            mqttc.publish(
                format!("orchard/{}/status", opts.hostname),
                QoS::AtLeastOnce,
                true,
                "online",
            ).await?;
            Ok((mqttc, event_loop))
        }
        _ => {
            // Something is wrong
            error!(?event, "Unexpected event when trying to connect.");
            Err(MqttConnectionError::UnexpectedEvent(event))
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    init_tracing();

    dotenv::dotenv().unwrap();

    let cli = Cli::parse();
    let opts = MqttOptions::from(&cli);

    info!(?opts, "Orchard daemon starting!");
    let (_mqttc, mut event_loop) = connect_mqtt(opts).await.expect("MQTT connection failed");
    info!("MQTT connection established!");

    loop {
        let notification = event_loop.poll().await.unwrap();
        match notification {
            Event::Incoming(packet) => {
                debug!(?packet, "Incoming packet.")
            }
            Event::Outgoing(packet) => {
                debug!(?packet, "Outgoing packet.")
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
