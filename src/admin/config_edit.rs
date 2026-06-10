use std::path::Path;

use serde_json::Value;
use sha2::{Digest, Sha256};
use toml_edit::{Array, ArrayOfTables, DocumentMut, Item, Table};

// Fields a [[queues]] entry may carry besides its name.
const QUEUE_FIELDS: &[&str] = &[
    "max_lease_duration_ms",
    "default_max_attempts",
    "max_attempts_ceiling",
    "default_priority",
    "max_payload_bytes",
    "allowed_encodings",
    "allowed_job_types",
    "max_schedule_horizon_ms",
    "max_custom_entries",
    "max_custom_total_bytes",
    "max_custom_key_bytes",
    "dedup_window_ms",
    "max_queue_depth",
];

pub(super) fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

// A missing file reads as empty: its etag is the hash of "", and the first
// write creates it.
pub(super) fn read_file(path: &Path) -> std::io::Result<String> {
    match std::fs::read_to_string(path) {
        Ok(contents) => Ok(contents),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(String::new()),
        Err(e) => Err(e),
    }
}

pub(super) fn file_etag(path: &Path) -> std::io::Result<String> {
    Ok(sha256_hex(read_file(path)?.as_bytes()))
}

pub(super) fn write_atomic(path: &Path, contents: &str) -> std::io::Result<()> {
    let file_name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let tmp = path.with_file_name(format!("{file_name}.tmp"));
    std::fs::write(&tmp, contents)?;
    // Windows cannot rename over an existing file. The remove-then-rename
    // window is tolerated by the watcher, which keeps the running config
    // when the file is briefly missing or unparsable.
    if path.exists() {
        std::fs::remove_file(path)?;
    }
    std::fs::rename(&tmp, path)
}

fn toml_value(v: &Value) -> Result<toml_edit::Value, String> {
    match v {
        Value::String(s) => Ok(s.as_str().into()),
        Value::Bool(b) => Ok((*b).into()),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into())
            } else if let Some(f) = n.as_f64() {
                Ok(f.into())
            } else {
                Err("number is out of range".into())
            }
        }
        Value::Array(items) => {
            let mut arr = Array::new();
            for item in items {
                match item {
                    Value::String(_) | Value::Bool(_) | Value::Number(_) => {
                        arr.push(toml_value(item)?);
                    }
                    _ => return Err("arrays may only contain scalars".into()),
                }
            }
            Ok(arr.into())
        }
        Value::Null | Value::Object(_) => {
            Err("value must be a string, number, bool, or array of scalars".into())
        }
    }
}

// `section.key` edits a table entry; `queues.<name>.<field>` edits the
// [[queues]] entry with that name. A null value removes the key.
pub(super) fn apply_change(doc: &mut DocumentMut, path: &str, value: &Value) -> Result<(), String> {
    let segments: Vec<&str> = path.split('.').collect();
    match segments.as_slice() {
        ["queues", name, field] => upsert_queue_field(doc, name, field, value),
        [section, key] => set_section_value(doc, section, key, value),
        _ => Err(format!("unsupported config path {path:?}")),
    }
}

fn set_section_value(
    doc: &mut DocumentMut,
    section: &str,
    key: &str,
    value: &Value,
) -> Result<(), String> {
    if value.is_null() {
        if let Some(table) = doc.get_mut(section).and_then(Item::as_table_mut) {
            table.remove(key);
        }
        return Ok(());
    }

    let item = Item::Value(toml_value(value)?);
    let table = doc
        .entry(section)
        .or_insert(Item::Table(Table::new()))
        .as_table_mut()
        .ok_or_else(|| format!("{section} is not a table"))?;
    table[key] = item;
    Ok(())
}

// Locates the [[queues]] entry with this name, declaring it if absent.
fn queue_entry<'a>(doc: &'a mut DocumentMut, name: &str) -> Result<&'a mut Table, String> {
    let arr = doc
        .entry("queues")
        .or_insert(Item::ArrayOfTables(ArrayOfTables::new()))
        .as_array_of_tables_mut()
        .ok_or_else(|| "queues is not an array of tables".to_string())?;

    let found = arr
        .iter()
        .position(|t| t.get("name").and_then(Item::as_str) == Some(name));
    let idx = match found {
        Some(idx) => idx,
        None => {
            let mut table = Table::new();
            table["name"] = toml_edit::value(name);
            arr.push(table);
            arr.len() - 1
        }
    };
    Ok(arr.get_mut(idx).expect("entry just located or pushed"))
}

pub(super) fn ensure_queue(doc: &mut DocumentMut, name: &str) -> Result<(), String> {
    queue_entry(doc, name).map(|_| ())
}

pub(super) fn upsert_queue_field(
    doc: &mut DocumentMut,
    name: &str,
    field: &str,
    value: &Value,
) -> Result<(), String> {
    if !QUEUE_FIELDS.contains(&field) {
        return Err(format!("unknown queue override field {field:?}"));
    }
    // Convert before creating the entry so a bad value cannot leave behind an
    // empty [[queues]] table.
    let item = match value.is_null() {
        true => None,
        false => Some(Item::Value(toml_value(value)?)),
    };

    let table = queue_entry(doc, name)?;
    match item {
        Some(item) => table[field] = item,
        None => {
            table.remove(field);
        }
    }
    Ok(())
}

pub(super) fn remove_queue(doc: &mut DocumentMut, name: &str) -> bool {
    let Some(arr) = doc.get_mut("queues").and_then(Item::as_array_of_tables_mut) else {
        return false;
    };
    let before = arr.len();
    arr.retain(|t| t.get("name").and_then(Item::as_str) != Some(name));
    let removed = arr.len() != before;
    if arr.is_empty() {
        doc.remove("queues");
    }
    removed
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn doc(text: &str) -> DocumentMut {
        text.parse().expect("test TOML parses")
    }

    #[test]
    fn set_and_remove_a_section_value_preserving_comments() {
        let mut d = doc("# top comment\n[limits]\n# keep me\ndefault_priority = 3\n");
        apply_change(&mut d, "limits.default_priority", &json!(7)).unwrap();
        apply_change(&mut d, "limits.max_enqueue_batch", &json!(128)).unwrap();
        let out = d.to_string();
        assert!(out.contains("# top comment"));
        assert!(out.contains("# keep me"));
        assert!(out.contains("default_priority = 7"));
        assert!(out.contains("max_enqueue_batch = 128"));

        apply_change(&mut d, "limits.max_enqueue_batch", &Value::Null).unwrap();
        assert!(!d.to_string().contains("max_enqueue_batch"));
    }

    #[test]
    fn set_creates_a_missing_section() {
        let mut d = doc("");
        apply_change(&mut d, "server.strict_queues", &json!(true)).unwrap();
        assert!(d.to_string().contains("strict_queues = true"));
    }

    #[test]
    fn ensure_queue_declares_once_and_preserves_existing_fields() {
        let mut d = doc("");
        ensure_queue(&mut d, "emails").unwrap();
        assert!(d.to_string().contains("name = \"emails\""));

        upsert_queue_field(&mut d, "emails", "default_priority", &json!(5)).unwrap();
        ensure_queue(&mut d, "emails").unwrap();
        let out = d.to_string();
        assert_eq!(out.matches("name = \"emails\"").count(), 1);
        assert!(out.contains("default_priority = 5"));
    }

    #[test]
    fn queue_upsert_creates_edits_and_removes_entries() {
        let mut d = doc("[[queues]]\nname = \"other\"\n");
        upsert_queue_field(&mut d, "emails", "default_priority", &json!(5)).unwrap();
        upsert_queue_field(&mut d, "emails", "allowed_encodings", &json!(["json"])).unwrap();
        let out = d.to_string();
        assert!(out.contains("name = \"emails\""));
        assert!(out.contains("default_priority = 5"));
        assert!(out.contains("allowed_encodings"));

        upsert_queue_field(&mut d, "emails", "default_priority", &Value::Null).unwrap();
        assert!(!d.to_string().contains("default_priority"));

        assert!(remove_queue(&mut d, "emails"));
        assert!(!remove_queue(&mut d, "emails"));
        let out = d.to_string();
        assert!(!out.contains("emails"));
        assert!(out.contains("other"), "unrelated entries survive");
    }

    #[test]
    fn unknown_queue_fields_and_non_scalar_values_are_rejected() {
        let mut d = doc("");
        assert!(upsert_queue_field(&mut d, "q", "nope", &json!(1)).is_err());
        assert!(apply_change(&mut d, "limits.default_priority", &json!({"a": 1})).is_err());
        assert!(apply_change(&mut d, "limits.allowed", &json!([[1]])).is_err());
        assert!(apply_change(&mut d, "a.b.c.d", &json!(1)).is_err());
        assert_eq!(d.to_string(), "", "rejected changes leave no residue");
    }
}
