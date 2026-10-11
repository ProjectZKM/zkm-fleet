//! Concurrent namespace resource caps. Reservations have no duration or expiry.
pub use fleet_proto::model::v1::{Allocation, Quota, Usage};

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fractional_caps_zero_unlimited_and_invalid_values() {
        let q = Quota { gpus: Some(0.5), cpus: Some(4.0),
        };
        assert!(q.fits(0.5, 4.0));
        assert!(!q.fits(1.0, 1.0));
        assert!(!q.fits(0.0, 5.0));
        assert!(Quota::default().fits(1000.0, 1000.0));
        assert!(!Quota { gpus: Some(0.0), cpus: None }.fits(0.1, 1.0));
        assert!(Quota { gpus: Some(-1.0), cpus: None }.validate().is_err());
        assert!(Quota { gpus: None, cpus: Some(f64::NAN) }.validate().is_err());
    }
}
