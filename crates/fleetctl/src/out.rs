//! Output: a formatted view for people, or the same data as JSON (`--json`).
//!
//! Every command hands its result to `OutputWriter` as a JSON value: a record (`key  value`
//! lines), rows (an aligned table with named columns) or a short message. The
//! formatted view leaves out empty fields and shows times, durations and sizes the way
//! people read them; the JSON is the API's answer as it came.

use serde_json::Value;
use std::io::Write;

pub struct OutputWriter {
    pub json: bool,
}

/// A table column: its heading and the field it shows (a JSON pointer such as
/// `/address`, or a plain key).
pub struct TableColumn {
    pub head: &'static str,
    pub key: &'static str,
}

pub const fn col(head: &'static str, key: &'static str) -> TableColumn {
    TableColumn { head, key }
}

impl OutputWriter {
    /// One record.
    pub fn record(&self, v: &Value) {
        if self.json {
            pretty(v);
        } else {
            std::io::stdout().lock().write_all(kv(v, "").as_bytes()).expect("writing output");
        }
    }

    /// Rows under headings; JSON prints `raw` (the API's own rows) when given.
    pub fn table(&self, rows: &[Value], cols: &[TableColumn], raw: Option<&Value>) {
        if self.json {
            match raw {
                Some(r) => pretty(r),
                None => pretty(&Value::Array(rows.to_vec())),
            }
            return;
        }
        let cells: Vec<Vec<String>> = rows
            .iter()
            .map(|r| cols.iter().map(|c| cell(c.key, field(r, c.key))).collect())
            .collect();
        let widths: Vec<usize> = cols
            .iter()
            .enumerate()
            .map(|(i, c)| cells.iter().map(|r| r[i].chars().count()).chain([c.head.len()]).max().unwrap_or(0))
            .collect();
        let line = |vals: Vec<&str>| {
            let parts: Vec<String> = vals.iter().zip(&widths).map(|(v, w)| format!("{v:<w$}")).collect();
            writeln!(std::io::stdout().lock(), "{}", parts.join("  ").trim_end()).expect("writing output");
        };
        line(cols.iter().map(|c| c.head).collect());
        for r in &cells {
            line(r.iter().map(String::as_str).collect());
        }
    }

    /// A short message, or `v` as JSON.
    pub fn done(&self, text: &str, v: Value) {
        if self.json {
            pretty(&v);
        } else {
            writeln!(std::io::stdout().lock(), "{text}").expect("writing output");
        }
    }

    /// One JSON object per line (a stream, e.g. log chunks).
    pub fn line(&self, v: &Value) {
        writeln!(std::io::stdout().lock(), "{}", serde_json::to_string(v).unwrap_or_default()).expect("writing output");
    }
}

fn pretty(v: &Value) {
    writeln!(std::io::stdout().lock(), "{}", serde_json::to_string_pretty(v).unwrap_or_default()).expect("writing output");
}

fn field<'a>(v: &'a Value, key: &str) -> &'a Value {
    if key.starts_with('/') {
        v.pointer(key).unwrap_or(&Value::Null)
    } else {
        &v[key]
    }
}

/// `key  value` lines, aligned; nested records indented, lists of records one per line.
pub fn kv(v: &Value, indent: &str) -> String {
    let Some(obj) = v.as_object() else {
        return format!("{indent}{}\n", cell("", v));
    };
    let shown: Vec<(&String, &Value)> = obj.iter().filter(|(_, v)| !is_empty(v)).collect();
    let nested = |v: &Value| v.is_object() || v.as_array().is_some_and(|a| a.iter().any(Value::is_object));
    let width = shown.iter().filter(|(_, v)| !nested(v)).map(|(k, _)| k.len()).max().unwrap_or(0);
    let mut s = String::new();
    for (k, v) in shown {
        match v {
            Value::Object(_) => {
                s += &format!("{indent}{k}\n");
                s += &kv(v, &format!("{indent}  "));
            }
            Value::Array(items) if items.iter().any(Value::is_object) => {
                s += &format!("{indent}{k}\n");
                for item in items {
                    s += &format!("{indent}  - {}\n", inline(item));
                }
            }
            _ => s += &format!("{indent}{k:<width$}  {}\n", cell(k, v)),
        }
    }
    s
}

/// A record on one line: `key=value` for its non-empty fields.
fn inline(v: &Value) -> String {
    match v.as_object() {
        Some(obj) => obj
            .iter()
            .filter(|(_, v)| !is_empty(v))
            .map(|(k, v)| format!("{k}={}", cell(k, v)))
            .collect::<Vec<_>>()
            .join(" "),
        None => cell("", v),
    }
}

fn is_empty(v: &Value) -> bool {
    match v {
        Value::Null => true,
        Value::String(s) => s.is_empty(),
        Value::Array(a) => a.is_empty(),
        Value::Object(o) => o.is_empty(),
        _ => false,
    }
}

/// One value as people read it, by what its field is: `*_at` are seconds since the
/// epoch, `*_ms` milliseconds (a time when that large, else a duration), `bytes` a
/// size.
pub fn cell(key: &str, v: &Value) -> String {
    match v {
        Value::Null => "-".into(),
        Value::Bool(b) => if *b { "yes" } else { "no" }.into(),
        Value::String(s) if s.is_empty() => "-".into(),
        Value::String(s) => s.clone(),
        Value::Number(n) => {
            let f = n.as_f64().unwrap_or(0.0);
            if key.ends_with("_ms") && f > 1e11 {
                utc((f / 1000.0) as i64)
            } else if key.ends_with("_ms") {
                duration(f / 1000.0)
            } else if key.ends_with("_at") && f > 1e9 {
                utc(f as i64)
            } else if key == "bytes" {
                size(f)
            } else {
                n.to_string()
            }
        }
        Value::Array(items) if items.iter().all(|i| !i.is_object() && !i.is_array()) => {
            let parts: Vec<String> = items.iter().map(|i| cell("", i)).collect();
            if parts.is_empty() {
                "-".into()
            } else {
                parts.join(",")
            }
        }
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

/// Seconds as people read them: `12.3 s`, `4m 05s`, `2h 07m`.
fn duration(secs: f64) -> String {
    let whole = secs as u64;
    if secs < 120.0 {
        format!("{secs:.1} s")
    } else if whole < 7200 {
        format!("{}m {:02}s", whole / 60, whole % 60)
    } else {
        format!("{}h {:02}m", whole / 3600, whole % 3600 / 60)
    }
}

fn size(b: f64) -> String {
    let units = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = b;
    let mut u = 0;
    while v >= 1024.0 && u + 1 < units.len() {
        v /= 1024.0;
        u += 1;
    }
    if u == 0 {
        format!("{b:.0} B")
    } else {
        format!("{v:.1} {}", units[u])
    }
}

/// `YYYY-MM-DD hh:mm:ss UTC` (days to a civil date: Howard Hinnant's algorithm).
pub fn utc(secs: i64) -> String {
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!("{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02} UTC", rem / 3600, rem % 3600 / 60, rem % 60)
}
