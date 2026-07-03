use crate::Cli;
use crate::prelude::*;
use gethostname::gethostname;
use rumqttc::{AsyncClient, ClientError, ConnectionError, Event, EventLoop, LastWill, QoS, Transport};
use std::fmt::{Debug, Formatter};
use thiserror::Error;

#[derive(Debug, Clone)]
pub(crate) struct MqttOptions {
    pub(crate) hostname: String,
    pub(crate) client_id: String,
    pub(crate) broker: String,
    pub(crate) port: u16,
    pub(crate) use_ssl: bool,
    pub(crate) credentials: Option<MqttCredentials>,
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
            .unwrap_or_else(|| format!("orchard-{hostname}"));

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
pub(crate) struct MqttCredentials {
    pub(crate) username: String,
    pub(crate) password: String,
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
pub(crate) enum MqttConnectionError {
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
pub(crate) async fn connect_mqtt(
    opts: MqttOptions,
) -> Result<(AsyncClient, EventLoop), MqttConnectionError> {
    let status_topic = format!("orchard/{}/status", opts.hostname);
    let mut rumqttc_opts = rumqttc::MqttOptions::new(opts.client_id, opts.broker, opts.port);

    if let Some(credentials) = opts.credentials {
        rumqttc_opts.set_credentials(credentials.username, credentials.password);
    }

    if opts.use_ssl {
        rumqttc_opts.set_transport(Transport::tls_with_default_config());
    }

    rumqttc_opts.set_last_will(LastWill::new(
        status_topic.clone(),
        "offline",
        QoS::AtLeastOnce,
        true,
    ));

    // Startup queues a burst of subscribes and retained publishes before the main loop starts
    // polling the MQTT event loop again, so keep the request channel comfortably above that.
    let (mqttc, mut event_loop) = AsyncClient::new(rumqttc_opts, 32);

    let event = event_loop.poll().await?;

    match event {
        Event::Incoming(rumqttc::Packet::ConnAck(rumqttc::mqttbytes::v4::ConnAck {
            code: rumqttc::mqttbytes::v4::ConnectReturnCode::Success,
            ..
        })) => {
            debug!(?event, "Connection established successfully.");
            mqttc.publish(status_topic, QoS::AtLeastOnce, true, "online")
                .await?;
            Ok((mqttc, event_loop))
        }
        _ => {
            error!(?event, "Unexpected event when trying to connect.");
            Err(MqttConnectionError::UnexpectedEvent(event))
        }
    }
}

pub(crate) fn topic_matches(filter: &str, topic: &str) -> bool {
    let mut filter_levels = filter.split('/');
    let mut topic_levels = topic.split('/');

    loop {
        match (filter_levels.next(), topic_levels.next()) {
            (Some("#"), _) => return filter_levels.next().is_none(),
            (Some("+"), Some(_)) => continue,
            (Some(filter_level), Some(topic_level)) if filter_level == topic_level => continue,
            (None, None) => return true,
            _ => return false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::topic_matches;

    #[test]
    fn hash_wildcard_matches_remaining_levels() {
        assert!(topic_matches("A/#", "A/C/D/B"));
    }

    #[test]
    fn plus_wildcard_matches_exactly_one_level() {
        assert!(topic_matches("A/+/B", "A/C/B"));
        assert!(!topic_matches("A/+/B", "A/C/D/B"));
    }
}
