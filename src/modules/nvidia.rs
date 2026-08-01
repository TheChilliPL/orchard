use std::default::Default;
use std::collections::HashMap;
use async_trait::async_trait;
use crate::modules::Module;
use nvml_wrapper::error::NvmlError;
use nvml_wrapper::{Device, Nvml};
use nvml_wrapper::enum_wrappers::device::TemperatureSensor;
use rumqttc::QoS;
use tracing::warn;
use crate::discovery::{DiscoveryComponent, SensorDeviceClass, SensorSpec};
use crate::mqtt::scope::MqttScope;

pub struct NvidiaModule {
    nvml: Nvml,
    device_index: u32,
}

impl NvidiaModule {
    pub fn new(device_index: u32) -> Result<Self, NvmlError> {
        let nvml = Nvml::init()?;

        Ok(NvidiaModule {
            nvml,
            device_index,
        })
    }

    fn device(&self) -> Result<Device, NvmlError> {
        self.nvml.device_by_index(self.device_index)
    }
}

#[async_trait]
impl Module for NvidiaModule {
    fn name(&self) -> &'static str {
        "Nvidia module"
    }

    fn discovery_components(&self) -> HashMap<String, DiscoveryComponent> {
        HashMap::from([
            ("nvidia-gpu_usage".into(), DiscoveryComponent {
                unique_id: "nvidia-gpu_usage".into(),
                name: "GPU usage".into(),
                spec: SensorSpec {
                    state_topic: "nvidia/gpu_usage".into(),
                    unit_of_measurement: Some("%".into()),
                    suggested_display_precision: Some(0),
                    ..Default::default()
                }.into(),
                ..Default::default()
            }),
            ("nvidia-vram_usage_mib".into(), DiscoveryComponent {
                unique_id: "nvidia-vram_usage_mib".into(),
                name: "VRAM usage".into(),
                spec: SensorSpec {
                    state_topic: "nvidia/vram_usage_mib".into(),
                    unit_of_measurement: Some("MiB".into()),
                    suggested_display_precision: Some(0),
                    device_class: Some(SensorDeviceClass::DataSize),
                    ..Default::default()
                }.into(),
                ..Default::default()
            }),
            ("nvidia-vram_total_mib".into(), DiscoveryComponent {
                unique_id: "nvidia-vram_total_mib".into(),
                name: "VRAM total".into(),
                spec: SensorSpec {
                    state_topic: "nvidia/vram_total_mib".into(),
                    unit_of_measurement: Some("MiB".into()),
                    suggested_display_precision: Some(0),
                    device_class: Some(SensorDeviceClass::DataSize),
                    ..Default::default()
                }.into(),
                ..Default::default()
            }),
            ("nvidia-vram_usage_percent".into(), DiscoveryComponent {
                unique_id: "nvidia-vram_usage_percent".into(),
                name: "VRAM usage (%)".into(),
                spec: SensorSpec {
                    state_topic: "nvidia/vram_usage_percent".into(),
                    unit_of_measurement: Some("%".into()),
                    suggested_display_precision: Some(1),
                    ..Default::default()
                }.into(),
                ..Default::default()
            }),
            ("nvidia-gpu_temp".into(), DiscoveryComponent {
                unique_id: "nvidia-gpu_temp".into(),
                name: "GPU temperature".into(),
                spec: SensorSpec {
                    state_topic: "nvidia/gpu_temp".into(),
                    unit_of_measurement: Some("°C".into()),
                    device_class: Some(SensorDeviceClass::Temperature),
                    suggested_display_precision: Some(0),
                    ..Default::default()
                }.into(),
                ..Default::default()
            }),
        ])
    }

    async fn update(&mut self, mqtt: &MqttScope) {
        let dev = self.device().unwrap();

        let mem_info = dev.memory_info().unwrap();
        let util = dev.utilization_rates().unwrap();

        let gpu_usage = util.gpu;
        let vram_usage_mib = mem_info.used as f32 / 1024.0 / 1024.0;
        let vram_total_mib = mem_info.total as f32 / 1024.0 / 1024.0;
        let vram_usage_percent = (vram_usage_mib / vram_total_mib) * 100.0;
        let gpu_temp = dev.temperature(TemperatureSensor::Gpu);

        if let Err(err) = gpu_temp.as_ref() {
            warn!("Failed to read GPU temperature: {err}");
        }

        mqtt.publish("nvidia/gpu_usage", gpu_usage.to_string())
            .with_qos(QoS::AtMostOnce)
            .await
            .unwrap();
        mqtt.publish("nvidia/vram_usage_mib", vram_usage_mib.to_string())
            .with_qos(QoS::AtMostOnce)
            .await
            .unwrap();
        mqtt.publish("nvidia/vram_total_mib", vram_total_mib.to_string())
            .with_qos(QoS::AtMostOnce)
            .await
            .unwrap();
        mqtt.publish("nvidia/vram_usage_percent", vram_usage_percent.to_string())
            .with_qos(QoS::AtMostOnce)
            .await
            .unwrap();
        mqtt.publish(
            "nvidia/gpu_temp",
            gpu_temp.as_ref().ok().map_or_else(|| "".to_string(), |it| it.to_string()),
        )
        .with_qos(QoS::AtMostOnce)
        .await
        .unwrap();
    }
}
