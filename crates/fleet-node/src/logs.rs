//! Bounded reads of an app's log for `fleetctl app logs`.
use std::{
    fs::File,
    io::{self, Read, Seek, SeekFrom},
    path::Path,
};

/// The most bytes one reply carries; the scheduler enforces the same limit.
pub const MAX_BYTES: u64 = 256 * 1024;

/// The last `lines` lines of `path`, from at most its last `MAX_BYTES`.
pub fn tail(path: &Path, lines: u32) -> io::Result<Vec<u8>> {
    let mut file = File::open(path)?;
    let start = file.metadata()?.len().saturating_sub(MAX_BYTES);
    file.seek(SeekFrom::Start(start))?;
    let mut data = Vec::new();
    file.take(MAX_BYTES).read_to_end(&mut data)?;
    // Drop the partial first line when the window starts mid-file.
    let mut from = if start > 0 {
        data.iter()
            .position(|b| *b == b'\n')
            .map_or(data.len(), |i| i + 1)
    } else {
        0
    };
    let body_end = data.len() - usize::from(data.last() == Some(&b'\n'));
    let mut seen = 0;
    for i in (from..body_end).rev() {
        if data[i] == b'\n' {
            seen += 1;
            if seen == lines {
                from = i + 1;
                break;
            }
        }
    }
    Ok(data.split_off(from))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn log(name: &str, content: &[u8]) -> std::path::PathBuf {
        let path =
            std::env::temp_dir().join(format!("fleet-node-log-{}-{name}", std::process::id()));
        File::create(&path).unwrap().write_all(content).unwrap();
        path
    }

    #[test]
    fn tail_returns_the_last_lines() {
        let path = log("tail", b"a\nb\nc\nd\n");
        assert_eq!(tail(&path, 2).unwrap(), b"c\nd\n");
        assert_eq!(tail(&path, 99).unwrap(), b"a\nb\nc\nd\n");
        assert_eq!(tail(&log("open", b"a\nb\nc"), 1).unwrap(), b"c");
    }

    #[test]
    fn a_huge_log_is_bounded_and_starts_on_a_line_boundary() {
        let line = b"0123456789abcdef\n";
        let path = log("big", &line.repeat(40_000));
        assert_eq!(tail(&path, 2000).unwrap().len(), 17 * 2000);
        let all = tail(&path, 100_000).unwrap();
        assert!(all.len() as u64 <= MAX_BYTES && all.starts_with(line));
    }

    #[test]
    fn a_missing_file_is_not_found() {
        let missing = std::env::temp_dir().join("fleet-node-log-does-not-exist");
        assert_eq!(
            tail(&missing, 5).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
    }
}
