use std::collections::HashMap;
use async_trait::async_trait;
use rumqttc::{AsyncClient, QoS};
use serde::Deserialize;
use sysinfo::{Component, Components, CpuRefreshKind, MemoryRefreshKind, ProcessRefreshKind, RefreshKind, System};
use tracing::{debug, error, info, warn};
use crate::discovery::{DiscoveryComponent, SensorDeviceClass, SensorSpec};
use crate::modules::Module;

/// Function returning `true` to use with `#[serde(default = "return_true")]`.
#[allow(unused)]
fn return_true() -> bool { true }

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SysInfoModuleConfig {
    /// Whether to try to read CPU temperature.
    #[serde(default = "return_true")]
    read_temperature: bool,
    /// Which temperature sensor label to use.
    ///
    /// By default, tries to find `* Tctl` or `* Tccd1`. If fails to find either, logs an error and skips temperature reading.
    /// Found component labels are logged as debug messages when the module starts as well as logged whenever an error occurs.
    #[serde(default)]
    temperature_sensor_label: Option<String>,
}

impl Default for SysInfoModuleConfig {
    fn default() -> Self {
        SysInfoModuleConfig {
            read_temperature: true,
            temperature_sensor_label: None,
        }
    }
}

pub struct SysInfoModule {
    refresh_kind: RefreshKind,
    sys: System,
    previous_cpu_usage: Option<f32>,
    previous_ram_total: Option<u64>,
    previous_ram_used: Option<u64>,
    previous_temp: Option<f32>,
    temp_component: Option<String>,
}

impl SysInfoModule {
    pub fn new(config: &SysInfoModuleConfig) -> SysInfoModule {
        let refresh_kind = RefreshKind::nothing()
            .with_cpu(CpuRefreshKind::nothing().with_cpu_usage())
            .with_memory(MemoryRefreshKind::everything());

        let components = Components::new_with_refreshed_list();
        let components_str = components.iter().map(|c: &Component|
            format!("{} @ {}", c.label(), c.temperature().map_or_else(|| "unknown".to_string(), |t| format!("{t}°C")))
        ).collect::<Vec<_>>().join(", ");
        debug!("Found temperature components: {components_str}");
        let temp_component = if config.read_temperature {
            if let Some(label) = config.temperature_sensor_label.as_ref() {
                let c = components.iter().find(|c| c.label() == label).map(|c| c.label().to_string());
                if c.is_none() {
                    error!("Couldn't find the temperature component with label of {label}. Found components: {components_str}")
                }
                c
            } else {
                let c = Self::try_find_temp_component(&components);
                if c.is_none() {
                    warn!("Couldn't find the main temperature component. You can set one manually in sysinfo.temperature_sensor_label. Found components: {components_str}")
                }
                c
            }
        } else { None };

        if let Some(c) = temp_component.as_ref() {
            info!("Using {c} temperature component.");
        } else {
            info!("Temperature sensor disabled.");
        }

        SysInfoModule {
            refresh_kind,
            sys: System::new_with_specifics(refresh_kind),
            previous_cpu_usage: None,
            previous_ram_total: None,
            previous_ram_used: None,
            previous_temp: None,
            temp_component,
        }
    }

    fn try_find_temp_component(components: &Components) -> Option<String> {
        let tctl = components.iter().find(|c| c.label().ends_with(" Tctl"));
        if let Some(tctl) = tctl { return Some(tctl.label().to_string()); }

        let tccd1 = components.iter().find(|c| c.label().ends_with(" Tccd1"));
        if let Some(tccd1) = tccd1 { return Some(tccd1.label().to_string()); }

        None
    }
}

#[async_trait]
impl Module for SysInfoModule {
    fn name(&self) -> &'static str {
        "System info module"
    }

    fn discovery_components(&self, hostname: &str) -> HashMap<String, DiscoveryComponent> {
        let mut h = HashMap::from([
            ("cpu-usage".into(), DiscoveryComponent {
                unique_id: format!("orchard-{hostname}-cpu-usage"),
                name: "CPU usage".into(),
                spec: SensorSpec {
                    state_topic: format!("orchard/{hostname}/cpu/usage"),
                    unit_of_measurement: Some("%".into()),
                    suggested_display_precision: Some(1),
                    ..Default::default()
                }.into(),
                icon: Some("mdi:cpu-64-bit".into()),
                ..Default::default()
            }),
            ("ram-total_mib".into(), DiscoveryComponent {
                unique_id: format!("orchard-{hostname}-ram-total_mib"),
                name: "RAM available".into(),
                spec: SensorSpec {
                    state_topic: format!("orchard/{hostname}/ram/total_mib"),
                    unit_of_measurement: Some("MiB".into()),
                    suggested_display_precision: Some(0),
                    device_class: Some(SensorDeviceClass::DataSize),
                    ..Default::default()
                }.into(),
                icon: Some("mdi:memory".into()),
                ..Default::default()
            }),
            ("ram-usage_mib".into(), DiscoveryComponent {
                unique_id: format!("orchard-{hostname}-ram-usage_mib"),
                name: "RAM usage".into(),
                spec: SensorSpec {
                    state_topic: format!("orchard/{hostname}/ram/usage_mib"),
                    unit_of_measurement: Some("MiB".into()),
                    suggested_display_precision: Some(0),
                    device_class: Some(SensorDeviceClass::DataSize),
                    ..Default::default()
                }.into(),
                icon: Some("mdi:memory".into()),
                ..Default::default()
            }),
            ("ram-usage_percent".into(), DiscoveryComponent {
                unique_id: format!("orchard-{hostname}-ram-usage_percent"),
                name: "RAM usage (%)".into(),
                spec: SensorSpec {
                    state_topic: format!("orchard/{hostname}/ram/usage_percent"),
                    unit_of_measurement: Some("%".into()),
                    suggested_display_precision: Some(1),
                    ..Default::default()
                }.into(),
                icon: Some("mdi:memory".into()),
                ..Default::default()
            }),
        ]);
        if self.temp_component.is_some() {
            h.insert("cpu-temp".into(), DiscoveryComponent {
                unique_id: format!("orchard-{hostname}-cpu-temp"),
                name: "CPU temperature".into(),
                spec: SensorSpec {
                    state_topic: format!("orchard/{hostname}/cpu/temp"),
                    unit_of_measurement: Some("°C".into()),
                    suggested_display_precision: Some(1),
                    device_class: Some(SensorDeviceClass::Temperature),
                    ..Default::default()
                }.into(),
                ..Default::default()
            });
        }
        h
    }

    async fn update(&mut self, hostname: &str, mqttc: &AsyncClient) {
        self.sys.refresh_specifics(self.refresh_kind);

        let cpu_usage_percent = self.sys.global_cpu_usage();
        let ram_total_b = self.sys.total_memory();
        let ram_total_mib = ram_total_b as f64 / 1024.0 / 1024.0;
        let ram_usage_b = self.sys.used_memory();
        let ram_usage_mib = ram_usage_b as f64 / 1024.0 / 1024.0;
        let ram_usage_percent = ram_usage_mib / ram_total_mib * 100.0;

        if self.previous_cpu_usage != Some(cpu_usage_percent) {
            mqttc.publish(
                format!("orchard/{hostname}/cpu/usage"),
                QoS::AtMostOnce,
                true,
                cpu_usage_percent.to_string(),
            ).await.unwrap();
            self.previous_cpu_usage = Some(cpu_usage_percent);
        }

        let mut update_usage_percent = false;

        if self.previous_ram_total != Some(ram_total_b) {
            mqttc.publish(
                format!("orchard/{hostname}/ram/total_mib"),
                QoS::AtMostOnce,
                true,
                ram_total_mib.to_string(),
            ).await.unwrap();
            self.previous_ram_total = Some(ram_total_b);
            update_usage_percent = true;
        }

        if self.previous_ram_used != Some(ram_usage_b) {
            mqttc.publish(
                format!("orchard/{hostname}/ram/usage_mib"),
                QoS::AtMostOnce,
                true,
                ram_usage_mib.to_string(),
            ).await.unwrap();
            self.previous_ram_used = Some(ram_usage_b);
            update_usage_percent = true;
        }

        if update_usage_percent {
            mqttc.publish(
                format!("orchard/{hostname}/ram/usage_percent"),
                QoS::AtMostOnce,
                true,
                ram_usage_percent.to_string(),
            ).await.unwrap();
        }

        if let Some(temp_component_label) = self.temp_component.as_ref() {
            let components = Components::new_with_refreshed_list();
            let temp_component = components.iter().find(|c| c.label() == temp_component_label);

            if let Some(temp_component) = temp_component {
                let temp = temp_component.temperature();

                if self.previous_temp != temp {
                    mqttc.publish(
                        format!("orchard/{hostname}/cpu/temp"),
                        QoS::AtMostOnce,
                        true,
                        temp.map_or_else(|| "".to_string(), |t| t.to_string())
                    ).await.unwrap();
                }
            } else {
                error!("Temperature component {temp_component_label} unavailable!");
            }
        }
    }
}
