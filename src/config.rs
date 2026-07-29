use std::borrow::Cow;
use std::path::{Display, Path, PathBuf};
use cfg_if::cfg_if;
use serde::Deserialize;
use tracing::{info, warn};
use crate::modules::media::MediaModule;
use crate::modules::Module;
use crate::modules::nvidia::NvidiaModule;
#[cfg(feature = "obs")]
use crate::modules::obs::{ObsModule, ObsModuleConfig};
use crate::modules::status::StatusModule;
use crate::modules::sysinfo::{SysInfoModule, SysInfoModuleConfig};
use crate::modules::system_control::SystemControlModule;
use crate::modules::volume::VolumeModule;

#[derive(Deserialize)]
pub struct Config {
    pub modules: Vec<ModuleConfig>
}

impl Default for Config {
    fn default() -> Self {
        Config {
            modules: vec![
                ModuleConfig::Status,
                ModuleConfig::SysInfo(SysInfoModuleConfig::default()),
            ]
        }
    }
}

impl Config {
    pub fn get_config_path() -> PathBuf {
        let config_dir = dirs::config_dir().unwrap();

        cfg_if!(
            if #[cfg(target_os = "linux")] {
                let dir_name = "orchard";
            } else if #[cfg(target_os = "windows")] {
                let dir_name = "Orchard";
            } else {
                let dir_name = "dev.thechilli.orchard";
            }
        );

        let config_file_name = "config.toml";

        config_dir.join(dir_name).join(config_file_name)
    }

    pub fn load(path: Option<&Path>) -> Result<Self, String> {
        let path: Cow<_> = if let Some(path) = path {
            if !path.exists() {
                return Err(format!("Config path {} does not exist", path.display()));
            }
            path.into()
        } else {
            let config_path = Self::get_config_path();
            if !config_path.exists() {
                warn!("Config file not found, using default configuration");
                warn!("You can create a config file at {} to customize the configuration", config_path.display());
                return Ok(Config::default());
            }
            config_path.into()
        };

        let config_content = std::fs::read_to_string(&path)
            .map_err(|e| format!("Failed to read config file: {}", e))?;

        let conf: Config = toml::from_str(&config_content)
            .map_err(|e|format!("Failed to parse config file: {}", e))?;

        info!("Loaded config with {} modules from {}", conf.modules.len(), path.display());

        Ok(conf)
    }

    pub fn load_modules(&self) -> Vec<Box<dyn Module>> {
        self.modules.iter().flat_map(|m|
            m.generate().inspect_err(
                |e| {
                    warn!("Failed to load module {:?}: {}", m, e);
                }
            ).ok()
        ).collect()
    }
}

#[derive(Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ModuleConfig {
    Status,
    Volume,
    Media,
    SystemControl,
    SysInfo(SysInfoModuleConfig),
    Nvidia {
        device: u32,
    },
    #[cfg(feature = "obs")]
    Obs(ObsModuleConfig),
}

impl ModuleConfig {
    pub fn generate(&self) -> Result<Box<dyn Module>, String> {
        match self {
            ModuleConfig::Status => Ok(Box::new(StatusModule::new())),
            ModuleConfig::Volume => Ok(Box::new(VolumeModule::new())),
            #[cfg(target_os = "linux")]
            ModuleConfig::Media => Ok(Box::new(MediaModule::new())),
            #[cfg(not(target_os = "linux"))]
            ModuleConfig::Media => Err("Media module is only available on Linux".to_string()),
            ModuleConfig::SystemControl => Ok(Box::new(SystemControlModule::new())),
            ModuleConfig::SysInfo(config) => Ok(Box::new(SysInfoModule::new(config))),
            ModuleConfig::Nvidia { device } => Ok(Box::new(NvidiaModule::new(*device).map_err(|e| e.to_string())?)),
            #[cfg(feature = "obs")]
            ModuleConfig::Obs(config) => Ok(Box::new(ObsModule::new(config))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;
    use super::*;

    #[test]
    fn test_config_deserialize() {
        let conf: Config = toml::from_str(r#"
        [[modules]]
        type = "status"

        [[modules]]
        type = "volume"
        "#).unwrap();

        assert_eq!(conf.modules.len(), 2);
        assert_matches!(conf.modules[0], ModuleConfig::Status);
        assert_matches!(conf.modules[1], ModuleConfig::Volume);
    }
}
