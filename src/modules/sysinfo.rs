use std::collections::HashMap;
use async_trait::async_trait;
use rumqttc::{AsyncClient, QoS};
use sysinfo::{CpuRefreshKind, MemoryRefreshKind, ProcessRefreshKind, RefreshKind, System};
use tracing::debug;
use crate::discovery::{DiscoveryComponent, SensorSpec};
use crate::modules::Module;

pub struct SysInfoModule {
    refresh_kind: RefreshKind,
    sys: System,
    previous_cpu_usage: Option<f32>,
    previous_ram_total: Option<u64>,
    previous_ram_used: Option<u64>,
}

impl SysInfoModule {
    pub fn new() -> SysInfoModule {
        let refresh_kind = RefreshKind::nothing()
            .with_cpu(CpuRefreshKind::nothing().with_cpu_usage())
            .with_memory(MemoryRefreshKind::everything());
        SysInfoModule {
            refresh_kind,
            sys: System::new_with_specifics(refresh_kind),
            previous_cpu_usage: None,
            previous_ram_total: None,
            previous_ram_used: None,
        }
    }
}

#[async_trait]
impl Module for SysInfoModule {
    fn name(&self) -> &'static str {
        "System info module"
    }

    fn discovery_components(&self, hostname: &str) -> HashMap<String, DiscoveryComponent> {
        HashMap::from([
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
                    device_class: Some("data_size".into()),
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
                    device_class: Some("data_size".into()),
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
        ])
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

        if self.previous_ram_used != Some(ram_total_b) {
            mqttc.publish(
                format!("orchard/{hostname}/ram/usage_mib"),
                QoS::AtMostOnce,
                true,
                ram_usage_mib.to_string(),
            ).await.unwrap();
            self.previous_ram_used = Some(ram_total_b);
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
    }
}
