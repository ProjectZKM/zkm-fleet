//! What this machine runs on, reported once so `fleetctl node NAME` can show it.
//! Detection is best effort: a missing tool leaves the field out, never fails the agent.
use fleet_proto::fleet::model::v1::{GpuDevice, NodeHardware};
use std::process::Command;

pub fn detect() -> NodeHardware {
    let cpu_model = std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|s| cpu_model(&s))
        .or_else(|| run("lscpu", &[]).and_then(|s| lscpu_model(&s)));
    NodeHardware { cpu_arch: std::env::consts::ARCH.into(), cpu_model, gpus: gpus() }
}

fn run(program: &str, args: &[&str]) -> Option<String> {
    let out = Command::new(program).args(args).output().ok()?;
    out.status.success().then(|| String::from_utf8_lossy(&out.stdout).into_owned())
}

/// The first `model name` in /proc/cpuinfo (x86); ARM kernels usually omit it.
fn cpu_model(cpuinfo: &str) -> Option<String> {
    cpuinfo.lines().find_map(|l| {
        let (k, v) = l.split_once(':')?;
        (k.trim() == "model name").then(|| v.trim().to_owned()).filter(|v| !v.is_empty())
    })
}

/// lscpu's `Model name` (also names ARM cores, e.g. Neoverse-V2).
fn lscpu_model(lscpu: &str) -> Option<String> {
    lscpu.lines().find_map(|l| {
        let (k, v) = l.split_once(':')?;
        (k.trim() == "Model name").then(|| v.trim().to_owned()).filter(|v| !v.is_empty() && v != "-")
    })
}

fn gpus() -> Vec<GpuDevice> {
    const ARGS: &str = "--format=csv,noheader,nounits";
    // Drivers before compute_cap was queryable reject the whole query; ask again without it.
    run("nvidia-smi", &["--query-gpu=index,name,compute_cap,memory.total,driver_version", ARGS])
        .map(|s| parse_gpus(&s, true))
        .or_else(|| run("nvidia-smi", &["--query-gpu=index,name,memory.total,driver_version", ARGS]).map(|s| parse_gpus(&s, false)))
        .unwrap_or_default()
}

fn parse_gpus(csv: &str, with_capability: bool) -> Vec<GpuDevice> {
    let known = |v: &str| (!v.is_empty() && !v.starts_with('[') && v != "N/A").then(|| v.to_owned());
    csv.lines()
        .filter_map(|line| {
            let f: Vec<&str> = line.split(',').map(str::trim).collect();
            let (index, name, capability, memory, driver) = match (with_capability, f.as_slice()) {
                (true, [i, n, c, m, d]) => (i, n, known(c), m, d),
                (false, [i, n, m, d]) => (i, n, None, m, d),
                _ => return None,
            };
            Some(GpuDevice {
                index: index.parse().ok()?,
                name: name.to_string(),
                architecture: capability.as_deref().and_then(architecture).map(str::to_owned),
                compute_capability: capability,
                memory_mib: memory.parse().ok(),
                driver_version: known(driver),
            })
        })
        .collect()
}

/// NVIDIA's architecture for a CUDA compute capability ("major.minor").
fn architecture(capability: &str) -> Option<&'static str> {
    let (major, minor) = capability.split_once('.')?;
    Some(match (major.parse::<u32>().ok()?, minor.parse::<u32>().ok()?) {
        (3, _) => "Kepler",
        (5, _) => "Maxwell",
        (6, _) => "Pascal",
        (7, 0 | 2) => "Volta",
        (7, 5) => "Turing",
        (8, 9) => "Ada Lovelace",
        (8, _) => "Ampere",
        (9, _) => "Hopper",
        (10..=12, _) => "Blackwell",
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gpus_come_with_their_architecture() {
        let csv = "0, NVIDIA GeForce RTX 5090, 12.0, 32607, 580.95.05\n1, NVIDIA H100 80GB HBM3, 9.0, 81559, 580.95.05\n";
        let g = parse_gpus(csv, true);
        assert_eq!(g.len(), 2);
        assert_eq!((g[0].index, g[0].name.as_str()), (0, "NVIDIA GeForce RTX 5090"));
        assert_eq!(g[0].compute_capability.as_deref(), Some("12.0"));
        assert_eq!(g[0].architecture.as_deref(), Some("Blackwell"));
        assert_eq!(g[0].memory_mib, Some(32607));
        assert_eq!(g[0].driver_version.as_deref(), Some("580.95.05"));
        assert_eq!(g[1].architecture.as_deref(), Some("Hopper"));
    }

    #[test]
    fn an_older_driver_reports_cards_without_a_capability() {
        let g = parse_gpus("0, Tesla T4, 15360, 470.82.01\n", false);
        assert_eq!(g[0].name, "Tesla T4");
        assert_eq!((g[0].compute_capability.clone(), g[0].architecture.clone()), (None, None));
        // Unsupported values and malformed lines are dropped, not guessed.
        let g = parse_gpus("0, X, [N/A], [N/A], N/A\nnot a row\n", true);
        assert_eq!(g.len(), 1);
        assert_eq!((g[0].memory_mib, g[0].driver_version.clone(), g[0].architecture.clone()), (None, None, None));
    }

    #[test]
    fn capabilities_map_to_architectures() {
        for (c, a) in [("7.0", Some("Volta")), ("7.5", Some("Turing")), ("8.0", Some("Ampere")), ("8.6", Some("Ampere")),
            ("8.9", Some("Ada Lovelace")), ("9.0", Some("Hopper")), ("10.0", Some("Blackwell")), ("12.0", Some("Blackwell")),
            ("13.0", None), ("x", None)] {
            assert_eq!(architecture(c), a, "{c}");
        }
    }

    #[test]
    fn cpu_models_come_from_cpuinfo_or_lscpu() {
        assert_eq!(cpu_model("processor\t: 0\nmodel name\t: AMD EPYC 9654 96-Core Processor\n").as_deref(),
            Some("AMD EPYC 9654 96-Core Processor"));
        assert_eq!(cpu_model("processor\t: 0\nCPU part\t: 0xd4f\n"), None);
        assert_eq!(lscpu_model("Architecture:  aarch64\nModel name:    Neoverse-V2\n").as_deref(), Some("Neoverse-V2"));
        assert_eq!(lscpu_model("Model name:    -\n"), None);
    }

    #[test]
    fn detection_names_this_machine() {
        assert_eq!(detect().cpu_arch, std::env::consts::ARCH);
    }
}
