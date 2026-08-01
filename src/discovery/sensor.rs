use orchard_macros::short_names;
use serde::Serialize;
use serde_with::{DeserializeFromStr, SerializeDisplay, skip_serializing_none};
use crate::mqtt::scope::{MqttScope, Scopeable};

#[skip_serializing_none]
#[short_names]
#[derive(Debug, Clone, Serialize, Default)]
pub struct SensorSpec {
    #[short_name("stat_t")]
    pub state_topic: String,
    #[short_name("val_tpl")]
    pub value_template: Option<String>,
    #[short_name("dev_cla")]
    pub device_class: Option<SensorDeviceClass>,
    #[short_name("ops")]
    pub options: Option<Vec<String>>,
    #[short_name("sug_dsp_prc")]
    pub suggested_display_precision: Option<i32>,
    #[short_name("unit_of_meas")]
    pub unit_of_measurement: Option<String>,
}

impl Scopeable for SensorSpec {
    fn scope(mut self, scope: &MqttScope) -> Self {
        self.state_topic = scope.scope_topic(&self.state_topic);
        self
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    SerializeDisplay,
    DeserializeFromStr,
    strum::Display,
    strum::EnumString,
)]
#[strum(serialize_all = "snake_case")]
/// See also: https://www.home-assistant.io/integrations/sensor/#device-class
pub enum SensorDeviceClass {
    /// Absolute humidity in g/m³, mg/m³.
    AbsoluteHumidity,
    /// Apparent power in mVA, VA or kVA.
    ApparentPower,
    /// Air Quality Index (unitless).
    Aqi,
    /// Area in m², cm², km², mm², in², ft², yd², mi², ac, ha.
    Area,
    /// Atmospheric pressure in cbar, bar, hPa, mmHg, inHg, kPa, mbar, Pa or psi.
    AtmosphericPressure,
    /// Percentage of battery that is left in %.
    Battery,
    /// Blood glucose concentration in mg/dL, mmol/L.
    BloodGlucoseConcentration,
    /// Carbon Dioxide (CO₂) concentration in ppm.
    CarbonDioxide,
    /// Carbon Monoxide (CO) concentration in ppb, ppm, µg/m³, mg/m³.
    CarbonMonoxide,
    /// Current in A, mA.
    Current,
    /// Data rate in bit/s, kbit/s, Mbit/s, Gbit/s, B/s, kB/s, MB/s, GB/s, KiB/s, MiB/s or GiB/s.
    DataRate,
    /// Data size in bit, kbit, Mbit, Gbit, B, kB, MB, GB, TB, PB, EB, ZB, YB, KiB, MiB, GiB, TiB, PiB, EiB, ZiB or YiB.
    DataSize,
    /// Date string (ISO 8601).
    Date,
    /// Generic distance in km, m, cm, mm, mi, nmi, yd, or in.
    Distance,
    /// Duration in d, h, min, s, ms, or µs.
    Duration,
    /// Energy in J, kJ, MJ, GJ, mWh, Wh, kWh, MWh, GWh, TWh, cal, kcal, Mcal, or Gcal.
    Energy,
    /// Energy per distance in kWh/100km, Wh/km, mi/kWh, or km/kWh.
    EnergyPerDistance,
    /// Stored energy in J, kJ, MJ, GJ, mWh, Wh, kWh, MWh, GWh, TWh, cal, kcal, Mcal, or Gcal.
    EnergyStorage,
    /// Has a limited set of (non-numeric) states.
    Enum,
    /// Frequency in Hz, kHz, MHz, or GHz.
    Frequency,
    /// Gas volume in L, m³, ft³, CCF, or MCF.
    Gas,
    /// Percentage of humidity in the air in %.
    Humidity,
    /// The current light level in lx.
    Illuminance,
    /// Irradiance in W/m² or BTU/(h⋅ft²).
    Irradiance,
    /// Percentage of water in a substance in %.
    Moisture,
    /// The monetary value (ISO 4217).
    Monetary,
    /// Concentration of Nitrogen Dioxide in ppb, ppm, µg/m³.
    NitrogenDioxide,
    /// Concentration of Nitrogen Monoxide in ppb, µg/m³.
    NitrogenMonoxide,
    /// Concentration of Nitrous Oxide in µg/m³.
    NitrousOxide,
    /// Concentration of Ozone in ppb, ppm, or µg/m³.
    Ozone,
    /// Potential hydrogen (pH) value of a water solution.
    Ph,
    /// Concentration of particulate matter less than 1 micrometer in µg/m³.
    Pm1,
    /// Concentration of particulate matter less than 2.5 micrometers in µg/m³.
    Pm25,
    /// Concentration of particulate matter less than 4 micrometers in µg/m³.
    Pm4,
    /// Concentration of particulate matter less than 10 micrometers in µg/m³.
    Pm10,
    /// Power factor (unitless), unit may be None or %.
    PowerFactor,
    /// Power in mW, W, kW, MW, GW or TW.
    Power,
    /// Accumulated precipitation in cm, in or mm.
    Precipitation,
    /// Precipitation intensity in in/d, in/h, mm/d or mm/h.
    PrecipitationIntensity,
    /// Pressure in mPa, Pa, hPa, kPa, bar, cbar, mbar, mmHg, inHg, inH₂O or psi.
    Pressure,
    /// Reactive energy in varh or kvarh.
    ReactiveEnergy,
    /// Reactive power in mvar, var, or kvar.
    ReactivePower,
    /// Signal strength in dB or dBm.
    SignalStrength,
    /// Sound pressure in dB or dBA.
    SoundPressure,
    /// Generic speed in ft/s, in/d, in/h, in/s, km/h, kn, m/s, mph, mm/d, or mm/s.
    Speed,
    /// Concentration of sulphur dioxide in ppb or µg/m³.
    SulphurDioxide,
    /// Temperature in °C, °F or K.
    Temperature,
    /// Temperature difference between two measurements in °C, °F, or K.
    TemperatureDelta,
    /// Datetime object or timestamp string (ISO 8601).
    Timestamp,
    /// Concentration of volatile organic compounds in µg/m³ or mg/m³.
    VolatileOrganicCompounds,
    /// Ratio of volatile organic compounds in ppm or ppb.
    VolatileOrganicCompoundsParts,
    /// Voltage in V, mV, µV, kV, MV.
    Voltage,
    /// Generic volume in L, mL, gal, fl. oz., m³, ft³, CCF, or MCF.
    Volume,
    /// Volume flow rate in m³/h, m³/min, m³/s, ft³/min, L/h, L/min, L/s, gal/d, gal/h, gal/min, or mL/s.
    VolumeFlowRate,
    /// Generic stored volume in L, mL, gal, fl. oz., m³, ft³, CCF, or MCF.
    VolumeStorage,
    /// Water consumption in L, gal, m³, ft³, CCF, or MCF.
    Water,
    /// Generic mass in kg, g, mg, µg, oz, lb, or st.
    Weight,
    /// Wind direction in °.
    WindDirection,
    /// Wind speed in Beaufort, ft/s, km/h, kn, m/s, or mph.
    WindSpeed,
    /// An unrecognized sensor device class string.
    #[strum(default, to_string = "{0}")]
    Other(String),
}

#[cfg(test)]
mod tests {
    use super::SensorDeviceClass;

    #[test]
    fn deserializes_known_device_class() {
        let value: SensorDeviceClass = serde_json::from_str("\"temperature\"").unwrap();
        assert_eq!(value, SensorDeviceClass::Temperature);
    }

    #[test]
    fn deserializes_unknown_device_class_to_other() {
        let value: SensorDeviceClass = serde_json::from_str("\"abc\"").unwrap();
        assert_eq!(value, SensorDeviceClass::Other("abc".into()));
    }

    #[test]
    fn serializes_other_device_class_as_plain_string() {
        let value = SensorDeviceClass::Other("abc".into());
        let json = serde_json::to_string(&value).unwrap();
        assert_eq!(json, "\"abc\"");
    }
}
