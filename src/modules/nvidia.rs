use std::default::Default;
use std::collections::HashMap;
use async_trait::async_trait;
use crate::modules::Module;
use nvml_wrapper::error::NvmlError;
use nvml_wrapper::{Device, Nvml};
use nvml_wrapper::enum_wrappers::device::TemperatureSensor;
use rumqttc::{AsyncClient, QoS};
use tracing::warn;
use crate::discovery::{DiscoveryComponent, SensorDeviceClass, SensorSpec};

pub struct NvidiaModule {
    nvml: Nvml,
    device_index: u32,
    previous_gpu_usage: Option<u32>,
    previous_vram_usage_mib: Option<f32>,
    previous_vram_total_mib: Option<f32>,
    previous_vram_usage_percent: Option<f32>,
    previous_gpu_temp: Option<u32>,
}

impl NvidiaModule {
    pub fn new(device_index: u32) -> Result<Self, NvmlError> {
        let nvml = Nvml::init()?;

        Ok(NvidiaModule {
            nvml,
            device_index,
            previous_gpu_usage: None,
            previous_vram_usage_mib: None,
            previous_vram_total_mib: None,
            previous_vram_usage_percent: None,
            previous_gpu_temp: None,
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

    fn discovery_components(&self, hostname: &str) -> HashMap<String, DiscoveryComponent> {
        HashMap::from([
            ("nvidia-gpu_usage".into(), DiscoveryComponent {
                unique_id: format!("orchard-{hostname}-nvidia-gpu_usage"),
                name: "GPU usage".into(),
                spec: SensorSpec {
                    state_topic: format!("orchard/{hostname}/nvidia/gpu_usage"),
                    unit_of_measurement: Some("%".into()),
                    suggested_display_precision: Some(0),
                    ..Default::default()
                }.into(),
                ..Default::default()
            }),
            ("nvidia-vram_usage_mib".into(), DiscoveryComponent {
                unique_id: format!("orchard-{hostname}-nvidia-vram_usage_mib"),
                name: "VRAM usage".into(),
                spec: SensorSpec {
                    state_topic: format!("orchard/{hostname}/nvidia/vram_usage_mib"),
                    unit_of_measurement: Some("MiB".into()),
                    suggested_display_precision: Some(0),
                    device_class: Some(SensorDeviceClass::DataSize),
                    ..Default::default()
                }.into(),
                ..Default::default()
            }),
            ("nvidia-vram_total_mib".into(), DiscoveryComponent {
                unique_id: format!("orchard-{hostname}-nvidia-vram_total_mib"),
                name: "VRAM total".into(),
                spec: SensorSpec {
                    state_topic: format!("orchard/{hostname}/nvidia/vram_total_mib"),
                    unit_of_measurement: Some("MiB".into()),
                    suggested_display_precision: Some(0),
                    device_class: Some(SensorDeviceClass::DataSize),
                    ..Default::default()
                }.into(),
                ..Default::default()
            }),
            ("nvidia-vram_usage_percent".into(), DiscoveryComponent {
                unique_id: format!("orchard-{hostname}-nvidia-vram_usage_percent"),
                name: "VRAM usage (%)".into(),
                spec: SensorSpec {
                    state_topic: format!("orchard/{hostname}/nvidia/vram_usage_percent"),
                    unit_of_measurement: Some("%".into()),
                    suggested_display_precision: Some(1),
                    ..Default::default()
                }.into(),
                ..Default::default()
            }),
            ("nvidia-gpu_temp".into(), DiscoveryComponent {
                unique_id: format!("orchard-{hostname}-nvidia-gpu_temp"),
                name: "GPU temperature".into(),
                spec: SensorSpec {
                    state_topic: format!("orchard/{hostname}/nvidia/gpu_temp"),
                    unit_of_measurement: Some("°C".into()),
                    device_class: Some(SensorDeviceClass::Temperature),
                    suggested_display_precision: Some(0),
                    ..Default::default()
                }.into(),
                ..Default::default()
            }),
        ])
    }

    async fn update(&mut self, hostname: &str, mqttc: &AsyncClient) {
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

        if self.previous_gpu_usage != Some(gpu_usage) {
            mqttc.publish(
                format!("orchard/{hostname}/nvidia/gpu_usage"),
                QoS::AtMostOnce,
                true,
                gpu_usage.to_string(),
            ).await.unwrap();
            self.previous_gpu_usage = Some(gpu_usage);
        }

        if self.previous_vram_usage_mib != Some(vram_usage_mib) {
            mqttc.publish(
                format!("orchard/{hostname}/nvidia/vram_usage_mib"),
                QoS::AtMostOnce,
                true,
                vram_usage_mib.to_string(),
            ).await.unwrap();
            self.previous_vram_usage_mib = Some(vram_usage_mib);
        }

        if self.previous_vram_total_mib != Some(vram_total_mib) {
            mqttc.publish(
                format!("orchard/{hostname}/nvidia/vram_total_mib"),
                QoS::AtMostOnce,
                true,
                vram_total_mib.to_string(),
            ).await.unwrap();
            self.previous_vram_total_mib = Some(vram_total_mib);
        }

        if self.previous_vram_usage_percent != Some(vram_usage_percent) {
            mqttc.publish(
                format!("orchard/{hostname}/nvidia/vram_usage_percent"),
                QoS::AtMostOnce,
                true,
                vram_usage_percent.to_string(),
            ).await.unwrap();
            self.previous_vram_usage_percent = Some(vram_usage_percent);
        }

        if self.previous_gpu_temp != gpu_temp.as_ref().ok().copied() {
            mqttc.publish(
                format!("orchard/{hostname}/nvidia/gpu_temp"),
                QoS::AtMostOnce,
                true,
                gpu_temp.as_ref().ok().map_or_else(|| "".to_string(), |it| it.to_string()),
            ).await.unwrap();
            self.previous_gpu_temp = gpu_temp.ok();
        }
    }
}
