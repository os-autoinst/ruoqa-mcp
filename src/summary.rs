//! Collapse a raw openQA `jobs` array into a compact triage summary (port of
//! `_summarize_jobs`). The raw array is ~10 KB/job and truncates in MCP
//! clients; this keeps only id/test/arch per job plus counts. Each job
//! buckets by its `result` when that is truthy and not `"none"`, otherwise by
//! its `state` (so in-progress jobs land under `running`/`scheduled` rather
//! than a `"none"` catch-all); a job lacking both falls under `"unknown"`.

use std::collections::HashMap;

use serde_json::{Value, json};

pub fn summarize_jobs(jobs: &[Value]) -> Value {
    let mut by_result: HashMap<&str, u64> = HashMap::new();
    let mut by_state: HashMap<&str, u64> = HashMap::new();
    let mut by_arch: HashMap<&str, u64> = HashMap::new();
    let mut buckets: HashMap<&str, Vec<Value>> = HashMap::new();

    for job in jobs {
        let result = job
            .get("result")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty());
        let state = job
            .get("state")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty());
        let arch = job
            .get("settings")
            .and_then(Value::as_object)
            .and_then(|s| s.get("ARCH"))
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty());

        let key = match result {
            Some(r) if r != "none" => r,
            _ => state.unwrap_or("unknown"),
        };
        buckets.entry(key).or_default().push(json!({
            "id": job.get("id").cloned().unwrap_or(Value::Null),
            "test": job.get("test").cloned().unwrap_or(Value::Null),
            "arch": arch,
        }));

        if let Some(r) = result {
            *by_result.entry(r).or_insert(0) += 1;
        }
        if let Some(s) = state {
            *by_state.entry(s).or_insert(0) += 1;
        }
        if let Some(a) = arch {
            *by_arch.entry(a).or_insert(0) += 1;
        }
    }

    json!({
        "total": jobs.len(),
        "by_result": by_result,
        "by_state": by_state,
        "by_arch": by_arch,
        "jobs": buckets,
    })
}

/// Narrow a raw `job_groups` array: keep rows whose `name` contains `name`
/// (case-insensitively), then, if `summary`, reduce each to `{id, name,
/// parent_id}` — the raw rows carry a `template` column that is ~95% of the
/// payload.
pub fn shape_job_groups(groups: &[Value], name: Option<&str>, summary: bool) -> Value {
    let needle = name.map(str::to_lowercase);
    let rows = groups.iter().filter(|group| {
        needle.as_ref().is_none_or(|needle| {
            group
                .get("name")
                .and_then(Value::as_str)
                .is_some_and(|n| n.to_lowercase().contains(needle))
        })
    });
    if summary {
        rows.map(|group| {
            json!({
                "id": group.get("id").cloned().unwrap_or(Value::Null),
                "name": group.get("name").cloned().unwrap_or(Value::Null),
                "parent_id": group.get("parent_id").cloned().unwrap_or(Value::Null),
            })
        })
        .collect()
    } else {
        rows.cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_jobs() -> Vec<Value> {
        serde_json::from_value(json!([
            {"id": 1, "test": "boot", "result": "passed", "state": "done", "settings": {"ARCH": "x86_64"}},
            {"id": 2, "test": "kdump", "result": "softfailed", "state": "done", "settings": {"ARCH": "aarch64"}},
            {"id": 3, "test": "install", "result": "failed", "state": "done", "settings": {"ARCH": "x86_64"}},
            {"id": 4, "test": "skipped_one", "result": "skipped", "state": "cancelled", "settings": {"ARCH": "s390x"}},
            {"id": 5, "test": "wip", "result": "none", "state": "running", "settings": {"ARCH": "x86_64"}},
            {"id": 6, "test": "nosettings", "result": "passed", "state": "done"}
        ]))
        .unwrap()
    }

    #[test]
    fn summary_matches_documented_shape() {
        let summary = summarize_jobs(&sample_jobs());
        assert_eq!(summary["total"], 6);
        assert_eq!(
            summary["by_result"],
            json!({"passed": 2, "softfailed": 1, "failed": 1, "skipped": 1, "none": 1})
        );
        assert_eq!(
            summary["by_state"],
            json!({"done": 4, "cancelled": 1, "running": 1})
        );
        assert_eq!(
            summary["by_arch"],
            json!({"x86_64": 3, "aarch64": 1, "s390x": 1})
        );

        let passed = summary["jobs"]["passed"].as_array().unwrap();
        assert_eq!(passed.len(), 2);
        assert_eq!(passed[0]["id"], 1);
        assert_eq!(passed[1]["id"], 6);
        assert_eq!(passed[1]["arch"], Value::Null); // missing settings -> null, no panic

        assert_eq!(summary["jobs"]["softfailed"][0]["test"], "kdump");
        assert_eq!(summary["jobs"]["skipped"][0]["arch"], "s390x");
        assert_eq!(summary["jobs"]["running"][0]["id"], 5);
        assert!(summary["jobs"].get("none").is_none());
    }

    fn sample_groups() -> Vec<Value> {
        serde_json::from_value(json!([
            {"id": 1, "name": "SLE 15 SP6 Maintenance", "parent_id": 7, "template": "big"},
            {"id": 2, "name": "Tumbleweed", "parent_id": null, "template": "big"},
            {"id": 3, "name": "sle micro"}
        ]))
        .unwrap()
    }

    #[test]
    fn job_groups_summary_keeps_only_id_name_parent_id() {
        let rows = shape_job_groups(&sample_groups(), None, true);
        assert_eq!(
            rows,
            json!([
                {"id": 1, "name": "SLE 15 SP6 Maintenance", "parent_id": 7},
                {"id": 2, "name": "Tumbleweed", "parent_id": null},
                {"id": 3, "name": "sle micro", "parent_id": null},
            ])
        );
    }

    #[test]
    fn job_groups_raw_mode_keeps_rows_unchanged() {
        let groups = sample_groups();
        assert_eq!(shape_job_groups(&groups, None, false), json!(groups));
    }

    #[test]
    fn job_groups_name_filter_is_a_case_insensitive_substring() {
        let rows = shape_job_groups(&sample_groups(), Some("SLE"), true);
        let ids: Vec<_> = rows.as_array().unwrap().iter().map(|r| &r["id"]).collect();
        assert_eq!(ids, [1, 3]);
    }

    #[test]
    fn empty_jobs_yields_zero_total() {
        let summary = summarize_jobs(&[]);
        assert_eq!(summary["total"], 0);
        assert_eq!(summary["by_result"], json!({}));
        assert_eq!(summary["jobs"], json!({}));
    }
}
