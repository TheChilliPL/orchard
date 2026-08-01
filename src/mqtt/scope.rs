use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant};
use rumqttc::{ClientError, QoS};
use tokio::sync::RwLock;
use tracing::{trace, warn};

#[derive(Debug, Clone)]
pub struct MqttScope<'c> {
    client: &'c rumqttc::AsyncClient,
    scope_topic: Cow<'c, str>,
    cache: Arc<RwLock<HashMap<String, (Instant, u64)>>>
}

impl <'c> MqttScope<'c> {
    pub fn new(client: &'c rumqttc::AsyncClient, scope_topic: Cow<'c, str>) -> Self {
        MqttScope {
            client,
            scope_topic,
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn sub_scope(&self, scope: &str) -> Self {
        let topic = self.scope_topic(scope);
        Self::new(self.client, topic.into())
    }

    pub fn scope_topic(&self, topic: &str) -> String {
        if cfg!(debug_assertions) && topic.starts_with(&*self.scope_topic) {
            warn!("Topic {} already starts with {}! Are you sure you're not scoping it twice?", topic, self.scope_topic);
        }
        format!("{}/{}", self.scope_topic, topic)
    }

    pub fn scope_id(&self, id: &str) -> String {
        let scope_as_id = self.scope_topic.replace('/', "-");
        if cfg!(debug_assertions) && id.starts_with(&scope_as_id) {
            warn!("ID {} already starts with {}! Are you sure you're not scoping it twice?", id, scope_as_id);
        }
        format!("{}-{}", scope_as_id, id)
    }

    pub fn unscope_topic<'s>(&self, topic: &'s str) -> Option<&'s str> {
        topic.strip_prefix(&*self.scope_topic)?.strip_prefix('/')
    }

    pub fn publish<'a>(&'a self, topic: &'a str, payload: impl Into<Vec<u8>>) -> PublishFuture<'a> {
        PublishFuture::new(&self, topic, payload)
    }
}

pub trait ClientExt<'c> {
    fn scope(self, scope: impl Into<Cow<'c, str>>) -> MqttScope<'c>;
}

impl <'c> ClientExt<'c> for &'c rumqttc::AsyncClient {
    fn scope(self, scope: impl Into<Cow<'c, str>>) -> MqttScope<'c> {
        MqttScope::new(self, scope.into())
    }
}

pub struct PublishFuture<'a> {
    scope_ref: &'a MqttScope<'a>,
    topic: &'a str,
    payload: Vec<u8>,
    qos: QoS,
    retain: bool,
    skip_cache: bool,
}

impl Debug for PublishFuture<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PublishFuture")
            .field("topic", &self.topic)
            .field_with("payload", |f| {
                if let Ok(payload_str) = str::from_utf8(&self.payload) {
                    (&payload_str as &dyn Debug).fmt(f)
                } else {
                    (&self.payload as &dyn Debug).fmt(f)
                }
            })
            .field("qos", &self.qos)
            .field("retain", &self.retain)
            .field("skip_cache", &self.skip_cache)
            .finish_non_exhaustive()
    }
}

impl <'a> PublishFuture<'a> {
    pub fn new(scope_ref: &'a MqttScope<'a>, topic: &'a str, payload: impl Into<Vec<u8>>) -> Self {
        PublishFuture {
            scope_ref,
            topic,
            payload: payload.into(),
            qos: QoS::AtMostOnce,
            retain: false,
            skip_cache: false,
        }
    }

    pub fn with_qos(mut self, qos: QoS) -> Self {
        self.qos = qos;
        self
    }

    pub fn retained(mut self) -> Self {
        self.retain = true;
        self
    }

    pub fn skip_cache(mut self) -> Self {
        self.skip_cache = true;
        self
    }
}

impl <'a> IntoFuture for PublishFuture<'a> {
    type Output = Result<(), ClientError>;
    type IntoFuture = impl Future<Output = Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        async move {
            let topic = self.scope_ref.scope_topic(self.topic);
            let cache = self.scope_ref.cache.clone();

            let mut hasher = DefaultHasher::new();
            self.payload.hash(&mut hasher);
            let payload_hash = hasher.finish();

            if !self.skip_cache {
                let cache = cache.read().await;
                let cache_entry = cache.get(&topic);
                if let Some((timestamp, cached_hash)) = cache_entry {
                    let max_duration = if self.retain { Duration::from_secs(30) } else { Duration::from_secs(10) };
                    if *cached_hash == payload_hash && Instant::now().duration_since(*timestamp) <= max_duration {
                        trace!("Skipping MQTT publish due to cache: {:?}", self);
                        return Ok(())
                    }
                }
            }

            trace!("Publishing MQTT: {:?}", self);

            self.scope_ref.client.publish(
                &topic,
                self.qos,
                self.retain,
                self.payload,
            ).await?;

            let mut cache = cache.write().await;
            cache.insert(topic, (Instant::now(), payload_hash));

            Ok(())
        }
    }
}

pub trait Scopeable {
    fn scope(self, scope: &MqttScope) -> Self;
}
