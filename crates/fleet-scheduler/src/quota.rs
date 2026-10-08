//! Concurrent namespace resource caps. Reservations have no duration or expiry.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Quota {
    pub gpus: Option<f64>,
    pub cpus: Option<f64>,
}

impl Quota {
    pub fn validate(&self) -> anyhow::Result<()> {
        for cap in [self.gpus, self.cpus].into_iter().flatten() {
            anyhow::ensure!(cap.is_finite() && cap >= 0.0, "resource caps must be finite and non-negative");
        }
        Ok(())
    }

    pub fn fits(&self, gpus: f64, cpus: f64) -> bool {
        self.gpus.is_none_or(|cap| gpus <= cap + 1e-9) && self.cpus.is_none_or(|cap| cpus <= cap + 1e-9)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Allocation {
    pub id: String,
    pub namespace: String,
    pub tag: Option<String>,
    pub gpus: f64,
    pub cpus: f64,
}

impl Allocation {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.id.is_empty() && self.id.len() <= 160, "invalid allocation id");
        anyhow::ensure!(self.gpus.is_finite() && self.cpus.is_finite() && self.gpus >= 0.0 && self.cpus >= 0.0,
            "allocated resources must be finite and non-negative");
        Ok(())
    }
}

#[derive(Debug, Default, Serialize)]
pub struct Usage {
    pub gpus: f64,
    pub cpus: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fractional_caps_zero_unlimited_and_invalid_values() {
        let q = Quota { gpus: Some(0.5), cpus: Some(4.0) };
        assert!(q.fits(0.5, 4.0));
        assert!(!q.fits(1.0, 1.0));
        assert!(!q.fits(0.0, 5.0));
        assert!(Quota::default().fits(1000.0, 1000.0));
        assert!(!Quota { gpus: Some(0.0), cpus: None }.fits(0.1, 1.0));
        assert!(Quota { gpus: Some(-1.0), cpus: None }.validate().is_err());
        assert!(Quota { gpus: None, cpus: Some(f64::NAN) }.validate().is_err());
    }
}
