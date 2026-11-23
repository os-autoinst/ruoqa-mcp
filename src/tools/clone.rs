//! `clone_job`: a port of upstream's `OpenQA::Script::CloneJob`
//! (`openqa-clone-job --within-instance`). The dependency walk is a pure,
//! synchronous function over already-fetched jobs; the async tool drives it
//! by fetching whichever job it reports as missing and re-running.

use std::collections::{BTreeMap, HashMap};
use std::sync::LazyLock;

use regex::Regex;
use serde_json::{Map, Value, json};

use crate::form::Form;
use crate::tools::MAX_CLONE_JOBS;

pub(crate) type Settings = BTreeMap<String, String>;

/// Applied to every cloned job regardless of its depth in the cluster.
const GLOBAL_SETTINGS: [&str; 3] = ["WORKER_CLASS", "_GROUP", "_GROUP_ID"];

/// Upstream's `SETTINGS_REGEX`, anchored and dot-all so a multi-line value
/// survives instead of being cut at the first newline.
static SETTING_ARG: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?s)^([A-Z0-9_]+(?:\[\])?)(?::([\w _*.,:/#@]+(?:[\w _*.,:/#@+-]+[\w _*.,:/#@])?))?(\+)?=(.*)$",
    )
    .expect("static pattern")
});

/// One `KEY=VALUE`, `KEY+=VALUE` or `KEY:TEST=VALUE` override.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SettingArg {
    pub(crate) key: String,
    pub(crate) scope: Option<String>,
    pub(crate) append: bool,
    pub(crate) value: String,
}

impl SettingArg {
    /// The error names only the part before the first `=`: values may be secrets.
    pub(crate) fn parse(raw: &str) -> Result<Self, String> {
        let caps = SETTING_ARG.captures(raw).ok_or_else(|| {
            let head = raw.split('=').next().unwrap_or_default();
            format!(
                "{head:?} is not a valid setting; expected KEY=VALUE, KEY+=VALUE or \
                 KEY:TEST[+]=VALUE with KEY matching [A-Z0-9_]+"
            )
        })?;
        Ok(Self {
            key: caps[1].to_string(),
            scope: caps.get(2).map(|m| m.as_str().to_string()),
            append: caps.get(3).is_some(),
            value: caps[4].to_string(),
        })
    }

    fn is_global(&self) -> bool {
        GLOBAL_SETTINGS.contains(&self.key.as_str())
    }
}

pub(crate) fn apply_settings(
    args: &[SettingArg],
    depth: u32,
    settings: &mut Settings,
    parental_inheritance: bool,
) {
    settings.remove("NAME");
    for arg in args {
        match &arg.scope {
            Some(scope) => {
                if settings.get("TEST").map(String::as_str).unwrap_or_default() != scope {
                    continue;
                }
            }
            None => {
                if !arg.is_global() && depth > 1 && !parental_inheritance {
                    continue;
                }
            }
        }
        if arg.value.is_empty() {
            settings.remove(&arg.key);
            continue;
        }
        let value = if arg.append {
            format!(
                "{}{}",
                settings
                    .get(&arg.key)
                    .map(String::as_str)
                    .unwrap_or_default(),
                arg.value
            )
        } else {
            arg.value.clone()
        };
        settings.insert(arg.key.clone(), value);
        match arg.key.as_str() {
            "_GROUP" => settings.remove("_GROUP_ID"),
            "_GROUP_ID" => settings.remove("_GROUP"),
            _ => None,
        };
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct Deps {
    pub(crate) chained: Vec<i64>,
    pub(crate) directly_chained: Vec<i64>,
    pub(crate) parallel: Vec<i64>,
}

impl Deps {
    fn from_json(value: Option<&Value>) -> Self {
        let ids = |name: &str| -> Vec<i64> {
            value
                .and_then(|v| v.get(name))
                .and_then(Value::as_array)
                .map(|a| a.iter().filter_map(Value::as_i64).collect())
                .unwrap_or_default()
        };
        Self {
            chained: ids("Chained"),
            directly_chained: ids("Directly chained"),
            parallel: ids("Parallel"),
        }
    }
}

/// The parts of `GET /api/v1/jobs/<id>` the clone walk needs.
#[derive(Debug, Default, Clone)]
pub(crate) struct Job {
    pub(crate) group_id: Option<i64>,
    pub(crate) settings: Settings,
    pub(crate) parents: Deps,
    pub(crate) children: Deps,
    pub(crate) missing_assets: Vec<String>,
}

impl Job {
    /// Parse the `{"job": {...}}` envelope.
    pub(crate) fn from_json(value: &Value) -> Result<Self, String> {
        let job = value.get("job").ok_or("response has no \"job\" object")?;
        let settings = job
            .get("settings")
            .and_then(Value::as_object)
            .ok_or("job has no \"settings\" object")?
            .iter()
            .filter_map(|(k, v)| match v {
                Value::Null => None,
                Value::String(s) => Some((k.clone(), s.clone())),
                other => Some((k.clone(), other.to_string())),
            })
            .collect();
        Ok(Self {
            group_id: job
                .get("group_id")
                .and_then(Value::as_i64)
                .filter(|id| *id != 0),
            settings,
            parents: Deps::from_json(job.get("parents")),
            children: Deps::from_json(job.get("children")),
            missing_assets: job
                .get("missing_assets")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, Copy)]
#[allow(
    clippy::struct_excessive_bools,
    reason = "one switch per tool argument, mirroring the CLI flags"
)]
pub(crate) struct CloneOptions {
    pub(crate) skip_deps: bool,
    pub(crate) skip_chained_deps: bool,
    pub(crate) clone_children: bool,
    /// 0 means unlimited.
    pub(crate) max_depth: u32,
    pub(crate) parental_inheritance: bool,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Plan {
    /// Job ids in the order the walk reached them.
    pub(crate) order: Vec<i64>,
    pub(crate) settings: BTreeMap<i64, Settings>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PlanError {
    /// The walk reached a job that is not in the cache yet.
    Need(i64),
    TooManyJobs,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Relation {
    Root,
    Parents,
    Children,
}

struct Walk<'a> {
    opts: CloneOptions,
    args: &'a [SettingArg],
    cache: &'a HashMap<i64, Job>,
    base: &'a str,
    plan: Plan,
}

/// `base_url` is the server's root URL, used for `CLONED_FROM`.
pub(crate) fn plan(
    root: i64,
    opts: CloneOptions,
    args: &[SettingArg],
    cache: &HashMap<i64, Job>,
    base_url: &str,
) -> Result<Plan, PlanError> {
    let mut walk = Walk {
        opts,
        args,
        cache,
        base: base_url.trim_end_matches('/'),
        plan: Plan::default(),
    };
    walk.visit(root, 1, Relation::Root)?;
    Ok(walk.plan)
}

impl Walk<'_> {
    fn visit(&mut self, id: i64, depth: u32, relation: Relation) -> Result<(), PlanError> {
        if self.plan.settings.contains_key(&id) {
            return Ok(());
        }
        if self.plan.order.len() >= MAX_CLONE_JOBS {
            return Err(PlanError::TooManyJobs);
        }
        let cache = self.cache;
        let job = cache.get(&id).ok_or(PlanError::Need(id))?;
        self.plan.order.push(id);
        self.plan.settings.insert(id, job.settings.clone());

        for relation in [Relation::Parents, Relation::Children] {
            let deps = if relation == Relation::Parents {
                &job.parents
            } else {
                &job.children
            };
            let lists = [
                (&deps.chained, true, false),
                (&deps.directly_chained, true, false),
                (&deps.parallel, false, true),
            ];
            for (list, is_chained, is_parallel) in lists {
                let skip = match relation {
                    Relation::Children => {
                        (self.opts.max_depth != 0 && depth > self.opts.max_depth)
                            || !(self.opts.clone_children || is_parallel)
                    }
                    _ => self.opts.skip_deps || (self.opts.skip_chained_deps && is_chained),
                };
                if skip {
                    continue;
                }
                for &dep in list {
                    self.visit(dep, depth + 1, relation)?;
                }
            }
            if relation == Relation::Parents {
                self.assign_existing(id, "_PARALLEL", &deps.parallel);
                self.assign_existing(id, "_START_AFTER", &deps.chained);
                self.assign_existing(id, "_START_DIRECTLY_AFTER", &deps.directly_chained);
            }
        }

        let cloned_from = format!("{}/tests/{id}", self.base);
        let apply_depth = if relation == Relation::Children {
            0
        } else {
            depth
        };
        let settings = self.settings_mut(id);
        settings.insert("CLONED_FROM".to_string(), cloned_from);
        if let Some(group_id) = job.group_id {
            settings.insert("_GROUP_ID".to_string(), group_id.to_string());
        }
        let (args, inherit) = (self.args, self.opts.parental_inheritance);
        apply_settings(args, apply_depth, self.settings_mut(id), inherit);
        Ok(())
    }

    fn settings_mut(&mut self, id: i64) -> &mut Settings {
        self.plan.settings.entry(id).or_default()
    }

    fn assign_existing(&mut self, id: i64, name: &str, deps: &[i64]) {
        let present: Vec<String> = deps
            .iter()
            .filter(|dep| self.plan.settings.contains_key(dep))
            .map(i64::to_string)
            .collect();
        if !present.is_empty() {
            self.settings_mut(id)
                .insert(name.to_string(), present.join(","));
        }
    }
}

/// The `POST jobs` form: `KEY:<original id>=VALUE` for every setting of every
/// planned job. `is_clone_job` makes openQA skip merging template settings.
pub(crate) fn compose_form(plan: &Plan) -> Form {
    let mut form = Form::new();
    for id in &plan.order {
        for (key, value) in &plan.settings[id] {
            form = form.push(&format!("{key}:{id}"), value);
        }
    }
    form.push("is_clone_job", 1)
}

fn numbered(key: &str, prefix: &str) -> bool {
    key.strip_prefix(prefix)
        .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
}

/// Upstream's `asset_type_from_setting`, as a yes/no.
fn is_asset_setting(key: &str, value: &str) -> bool {
    key == "ISO"
        || numbered(key, "ISO_")
        || numbered(key, "HDD_")
        || (matches!(key, "UEFI_PFLASH_CODE" | "UEFI_PFLASH_VARS") && !value.starts_with('/'))
        || numbered(key, "REPO_")
        || numbered(key, "ASSET_")
        || matches!(key, "KERNEL" | "INITRD")
}

/// Secrets that will clone as `[redacted]` and assets that no longer exist,
/// judged on the final settings. `None` when there is nothing to report.
pub(crate) fn warnings(plan: &Plan, cache: &HashMap<i64, Job>) -> Option<Value> {
    let published: Vec<&str> = plan
        .settings
        .values()
        .flat_map(|s| ["PUBLISH_HDD_1", "PUBLISH_PFLASH_VARS"].map(|k| s.get(k)))
        .flatten()
        .map(String::as_str)
        .collect();

    let mut redacted = Map::new();
    let mut missing = Map::new();
    for id in &plan.order {
        let settings = &plan.settings[id];
        let keys: Vec<&str> = settings
            .iter()
            .filter(|(_, v)| *v == "[redacted]")
            .map(|(k, _)| k.as_str())
            .collect();
        if !keys.is_empty() {
            redacted.insert(id.to_string(), json!(keys));
        }
        let Some(job) = cache.get(id) else { continue };
        let relevant: Vec<&str> = job
            .missing_assets
            .iter()
            .map(String::as_str)
            .filter(|entry| {
                let name = entry.split_once('/').map_or(*entry, |(_, name)| name);
                settings
                    .iter()
                    .any(|(k, v)| v == name && is_asset_setting(k, v))
                    && !published.contains(&name)
            })
            .collect();
        if !relevant.is_empty() {
            missing.insert(id.to_string(), json!(relevant));
        }
    }

    let mut out = Map::new();
    if !redacted.is_empty() {
        out.insert("redacted".to_string(), Value::Object(redacted));
    }
    if !missing.is_empty() {
        out.insert("missing_assets".to_string(), Value::Object(missing));
    }
    (!out.is_empty()).then_some(Value::Object(out))
}

/// Where a `github_ref` clone points the job's test code.
pub(crate) struct Refspec<'a> {
    /// `<owner>/<repo>` of the fork the branch lives in.
    pub(crate) repo: &'a str,
    pub(crate) branch: &'a str,
    pub(crate) pull_number: Option<u64>,
}

/// The arguments `openqa-clone-custom-git-refspec` passes to `openqa-clone-job`.
pub(crate) fn refspec_args(refspec: &Refspec<'_>, productdir: Option<&str>) -> Vec<String> {
    let Refspec {
        repo,
        branch,
        pull_number,
    } = refspec;
    let build_ref = pull_number.map_or_else(|| (*branch).to_string(), |n| n.to_string());
    let mut args = vec![
        "_GROUP=0".to_string(),
        format!("TEST+=@{repo}#{branch}"),
        format!("BUILD={repo}#{build_ref}"),
        format!("CASEDIR=https://github.com/{repo}.git#{branch}"),
    ];
    if let Some(productdir) = productdir {
        args.push(format!("PRODUCTDIR={productdir}"));
    }
    args
}

/// Point the original job's `PRODUCTDIR` at the checked-out fork: strip the
/// old case directory (absolute `PRODUCTDIR`) or everything through the
/// case directory's basename (relative), prefix the repo name, and collapse
/// the first `//`.
pub(crate) fn derive_productdir(
    old_casedir: &str,
    old_productdir: &str,
    repo_basename: &str,
) -> String {
    let rest = if old_productdir.starts_with('/') {
        old_productdir
            .strip_prefix(old_casedir)
            .unwrap_or(old_productdir)
    } else {
        let basename = old_casedir.rsplit('/').next().unwrap_or(old_casedir);
        old_productdir
            .find(basename)
            .map_or(old_productdir, |at| &old_productdir[at + basename.len()..])
    };
    format!("{repo_basename}/{rest}").replacen("//", "/", 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(raw: &[&str]) -> Vec<SettingArg> {
        raw.iter()
            .map(|r| SettingArg::parse(r).expect("valid setting"))
            .collect()
    }

    fn settings(pairs: &[(&str, &str)]) -> Settings {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    const ARGV: [&str; 5] = [
        "WORKER_CLASS=local",
        "HDD_1=new.qcow2",
        "HDDSIZEGB=40",
        "FOO=value:with:colon",
        "WORKER_CLASS:cre:ate+hpc#foo@bar+=-parent",
    ];

    fn child() -> Settings {
        settings(&[
            ("NAME", "00000810-sle-15-hpc_test@64bit"),
            ("TEST", "hpc_test"),
            ("HDD_1", "sle-15-with-hpc.qcow2"),
            ("HDDSIZEGB", "20"),
            ("WORKER_CLASS", "qemu_x86_64"),
        ])
    }

    fn parent() -> Settings {
        settings(&[
            ("NAME", "00000810-sle-15-cre@64bit"),
            ("TEST", "cre:ate+hpc#foo@bar"),
            ("HDD_1", "sle-15-with-hpc.qcow2"),
            ("HDDSIZEGB", "20"),
            ("WORKER_CLASS", "qemu_x86_64"),
        ])
    }

    #[test]
    fn child_takes_every_setting_and_loses_name() {
        let mut s = child();
        apply_settings(&args(&ARGV), 1, &mut s, false);
        assert_eq!(
            s,
            settings(&[
                ("TEST", "hpc_test"),
                ("HDD_1", "new.qcow2"),
                ("HDDSIZEGB", "40"),
                ("WORKER_CLASS", "local"),
                ("FOO", "value:with:colon"),
            ])
        );
    }

    #[test]
    fn parent_takes_only_global_and_scoped_settings() {
        let mut s = parent();
        apply_settings(&args(&ARGV), 2, &mut s, false);
        assert_eq!(s["WORKER_CLASS"], "local-parent");
        assert_eq!(s["HDD_1"], "sle-15-with-hpc.qcow2");
        assert_eq!(s["HDDSIZEGB"], "20");
        assert!(!s.contains_key("FOO"));
        assert!(!s.contains_key("NAME"));
    }

    #[test]
    fn parental_inheritance_lets_parents_take_unscoped_settings() {
        let mut s = parent();
        apply_settings(&args(&ARGV), 2, &mut s, true);
        assert_eq!(s["HDD_1"], "new.qcow2");
        assert_eq!(s["FOO"], "value:with:colon");
    }

    #[test]
    fn scope_parses_with_colons_plus_and_hash() {
        let a = SettingArg::parse("WORKER_CLASS:cre:ate+hpc#foo@bar+=-parent").unwrap();
        assert_eq!(a.key, "WORKER_CLASS");
        assert_eq!(a.scope.as_deref(), Some("cre:ate+hpc#foo@bar"));
        assert!(a.append);
        assert_eq!(a.value, "-parent");
    }

    #[test]
    fn invalid_forms_are_rejected() {
        for bad in [
            "foo",
            "WORKER_CLASS:foo++=abc",
            "WORKER_CLASS:foo-+=abc",
            "WORKER_CLASS:+foo=abc",
            "WORKER_CLASS:-foo=abc",
        ] {
            assert!(SettingArg::parse(bad).is_err(), "{bad} should be rejected");
        }
    }

    #[test]
    fn invalid_form_error_does_not_echo_the_value() {
        let err = SettingArg::parse("lower=hunter2").unwrap_err();
        assert!(err.contains("lower"));
        assert!(!err.contains("hunter2"));
    }

    #[test]
    fn multi_line_value_is_kept_whole() {
        let a = SettingArg::parse("YAML=a: 1\nb: 2\n").unwrap();
        assert_eq!(a.value, "a: 1\nb: 2\n");
    }

    #[test]
    fn group_and_group_id_override_each_other() {
        let mut s = Settings::new();
        apply_settings(&args(&["_GROUP=foo", "_GROUP_ID=bar"]), 0, &mut s, false);
        assert_eq!(s, settings(&[("_GROUP_ID", "bar")]));
        let mut s = Settings::new();
        apply_settings(&args(&["_GROUP_ID=bar", "_GROUP=foo"]), 0, &mut s, false);
        assert_eq!(s, settings(&[("_GROUP", "foo")]));
    }

    #[test]
    fn empty_value_deletes_the_setting() {
        let mut s = settings(&[("ISO_1", "x.iso"), ("ADDONS", "a")]);
        apply_settings(&args(&["ISO_1=", "ADDONS="]), 0, &mut s, false);
        assert!(s.is_empty());
    }

    #[test]
    fn append_extends_the_existing_value() {
        let mut s = settings(&[("TEST", "main")]);
        apply_settings(&args(&["TEST+=-pr", "NEW+=x"]), 1, &mut s, false);
        assert_eq!(s["TEST"], "main-pr");
        assert_eq!(s["NEW"], "x");
    }

    const BASE: &str = "https://bar/";

    fn job(group: Option<i64>, test: &str, parents: Deps, children: Deps) -> Job {
        Job {
            group_id: group,
            settings: settings(&[("TEST", test)]),
            parents,
            children,
            missing_assets: vec![],
        }
    }

    /// Upstream's 41 (parallel parent) / 42 (main) / 43 (chained child) fixture.
    fn cluster() -> HashMap<i64, Job> {
        HashMap::from([
            (
                41,
                job(
                    None,
                    "parent",
                    Deps::default(),
                    Deps {
                        parallel: vec![42],
                        ..Deps::default()
                    },
                ),
            ),
            (
                42,
                job(
                    Some(21),
                    "main",
                    Deps {
                        parallel: vec![41],
                        ..Deps::default()
                    },
                    Deps {
                        chained: vec![43],
                        ..Deps::default()
                    },
                ),
            ),
            (
                43,
                job(
                    None,
                    "child",
                    Deps {
                        chained: vec![42],
                        ..Deps::default()
                    },
                    Deps::default(),
                ),
            ),
        ])
    }

    fn opts() -> CloneOptions {
        CloneOptions {
            skip_deps: false,
            skip_chained_deps: false,
            clone_children: false,
            max_depth: 1,
            parental_inheritance: false,
        }
    }

    fn run(root: i64, opts: CloneOptions, raw: &[&str], cache: &HashMap<i64, Job>) -> Plan {
        plan(root, opts, &args(raw), cache, BASE).expect("plan")
    }

    fn sorted(plan: &Plan) -> Vec<i64> {
        plan.settings.keys().copied().collect()
    }

    #[test]
    fn default_clones_main_and_parallel_parent_only() {
        let p = run(42, opts(), &["FOO=bar"], &cluster());
        assert_eq!(sorted(&p), vec![41, 42]);
        assert_eq!(p.order, vec![42, 41]);
        let main = &p.settings[&42];
        assert_eq!(main["_PARALLEL"], "41");
        assert_eq!(main["_GROUP_ID"], "21");
        assert_eq!(main["CLONED_FROM"], "https://bar/tests/42");
        assert_eq!(main["FOO"], "bar");
        let parent = &p.settings[&41];
        assert_eq!(parent["CLONED_FROM"], "https://bar/tests/41");
        assert!(
            !parent.contains_key("FOO"),
            "depth 2 takes no unscoped args"
        );
    }

    #[test]
    fn clone_children_adds_the_chained_child_with_full_args() {
        let o = CloneOptions {
            clone_children: true,
            ..opts()
        };
        let p = run(42, o, &["FOO=bar"], &cluster());
        assert_eq!(sorted(&p), vec![41, 42, 43]);
        let child = &p.settings[&43];
        assert_eq!(child["_START_AFTER"], "42");
        assert_eq!(child["FOO"], "bar", "children are applied at depth 0");
        assert_eq!(child["CLONED_FROM"], "https://bar/tests/43");
        assert!(!p.settings[&41].contains_key("FOO"));
    }

    #[test]
    fn parental_inheritance_gives_the_parent_the_args() {
        let o = CloneOptions {
            parental_inheritance: true,
            ..opts()
        };
        let p = run(42, o, &["FOO=bar"], &cluster());
        assert_eq!(sorted(&p), vec![41, 42]);
        assert_eq!(p.settings[&41]["FOO"], "bar");
    }

    #[test]
    fn skip_deps_drops_parents_but_not_children() {
        let o = CloneOptions {
            skip_deps: true,
            clone_children: true,
            ..opts()
        };
        let p = run(42, o, &[], &cluster());
        assert_eq!(sorted(&p), vec![42, 43]);
        assert_eq!(p.settings[&43]["_START_AFTER"], "42");
        assert!(!p.settings[&42].contains_key("_PARALLEL"));
    }

    #[test]
    fn skip_chained_deps_drops_chained_parents_only() {
        let mut cache = cluster();
        cache.insert(
            41,
            job(
                None,
                "parent",
                Deps::default(),
                Deps {
                    chained: vec![42],
                    ..Deps::default()
                },
            ),
        );
        cache.insert(
            42,
            job(
                Some(21),
                "main",
                Deps {
                    chained: vec![41],
                    ..Deps::default()
                },
                Deps {
                    chained: vec![43],
                    ..Deps::default()
                },
            ),
        );
        let o = CloneOptions {
            skip_chained_deps: true,
            clone_children: true,
            ..opts()
        };
        let p = run(42, o, &[], &cache);
        assert_eq!(sorted(&p), vec![42, 43]);

        let keep = run(42, opts(), &[], &cache);
        assert_eq!(sorted(&keep), vec![41, 42]);
        assert_eq!(keep.settings[&42]["_START_AFTER"], "41");
    }

    #[test]
    fn skip_chained_deps_keeps_parallel_parents() {
        let o = CloneOptions {
            skip_chained_deps: true,
            ..opts()
        };
        assert_eq!(sorted(&run(42, o, &[], &cluster())), vec![41, 42]);
    }

    #[test]
    fn parallel_server_client_cycle_terminates() {
        let both = |peer: i64| {
            job(
                None,
                "t",
                Deps {
                    parallel: vec![peer],
                    ..Deps::default()
                },
                Deps {
                    parallel: vec![peer],
                    ..Deps::default()
                },
            )
        };
        let cache = HashMap::from([(1, both(2)), (2, both(1))]);
        let p = run(1, opts(), &[], &cache);
        assert_eq!(sorted(&p), vec![1, 2]);
        assert_eq!(p.settings[&1]["_PARALLEL"], "2");
        assert_eq!(p.settings[&2]["_PARALLEL"], "1");
    }

    #[test]
    fn cloning_a_parallel_client_skips_its_sibling_clients() {
        // server 1 has parallel children 2 and 3; cloning 2 reaches the server
        // at depth 2, which then does not descend to its other children
        let cache = HashMap::from([
            (
                1,
                job(
                    None,
                    "server",
                    Deps::default(),
                    Deps {
                        parallel: vec![2, 3],
                        ..Deps::default()
                    },
                ),
            ),
            (
                2,
                job(
                    None,
                    "client",
                    Deps {
                        parallel: vec![1],
                        ..Deps::default()
                    },
                    Deps::default(),
                ),
            ),
            (
                3,
                job(
                    None,
                    "client2",
                    Deps {
                        parallel: vec![1],
                        ..Deps::default()
                    },
                    Deps::default(),
                ),
            ),
        ]);
        assert_eq!(sorted(&run(2, opts(), &[], &cache)), vec![1, 2]);
        let all = CloneOptions {
            max_depth: 0,
            ..opts()
        };
        assert_eq!(sorted(&run(2, all, &[], &cache)), vec![1, 2, 3]);
    }

    #[test]
    fn walk_requests_missing_jobs_in_dfs_order() {
        let full = cluster();
        let o = CloneOptions {
            clone_children: true,
            ..opts()
        };
        let mut cache = HashMap::new();
        let mut requested = Vec::new();
        let finished = loop {
            match plan(42, o, &[], &cache, BASE) {
                Ok(p) => break p,
                Err(PlanError::Need(id)) => {
                    requested.push(id);
                    cache.insert(id, full[&id].clone());
                }
                Err(PlanError::TooManyJobs) => panic!("unexpected cap"),
            }
        };
        assert_eq!(requested, vec![42, 41, 43]);
        assert_eq!(finished.order, vec![42, 41, 43]);
    }

    #[test]
    fn more_than_the_cap_is_refused_before_fetching_the_extra_job() {
        let n = i64::try_from(MAX_CLONE_JOBS).unwrap();
        let mut cache = HashMap::new();
        for id in 1..=n {
            cache.insert(
                id,
                job(
                    None,
                    "t",
                    Deps::default(),
                    Deps {
                        parallel: vec![id + 1],
                        ..Deps::default()
                    },
                ),
            );
        }
        let all = CloneOptions {
            max_depth: 0,
            ..opts()
        };
        assert_eq!(
            plan(1, all, &[], &cache, BASE),
            Err(PlanError::TooManyJobs),
            "job {} is never asked for",
            n + 1
        );
    }

    #[test]
    fn form_has_job_scoped_keys_and_the_clone_flag_but_no_name() {
        let mut cache = cluster();
        cache
            .get_mut(&42)
            .unwrap()
            .settings
            .insert("NAME".into(), "n".into());
        let p = run(42, opts(), &[], &cache);
        let form = compose_form(&p);
        let pairs = form.pairs();
        assert!(pairs.contains(&("TEST:42", "main")));
        assert!(pairs.contains(&("TEST:41", "parent")));
        assert!(pairs.contains(&("_PARALLEL:42", "41")));
        assert!(pairs.contains(&("CLONED_FROM:41", "https://bar/tests/41")));
        assert!(pairs.contains(&("is_clone_job", "1")));
        assert!(pairs.iter().all(|(k, _)| !k.starts_with("NAME:")));
    }

    fn with_assets(mut j: Job, missing: &[&str], extra: &[(&str, &str)]) -> Job {
        j.missing_assets = missing.iter().map(|s| (*s).to_string()).collect();
        for (k, v) in extra {
            j.settings.insert((*k).to_string(), (*v).to_string());
        }
        j
    }

    #[test]
    fn redacted_secrets_are_reported_from_the_final_settings() {
        let mut cache = cluster();
        cache.get_mut(&42).unwrap().settings.extend(settings(&[
            ("_SECRET_A", "[redacted]"),
            ("_SECRET_B", "[redacted]"),
        ]));
        let o = opts();
        let p = run(42, o, &["_SECRET_B=mine"], &cache);
        let w = warnings(&p, &cache).unwrap();
        assert_eq!(w, json!({"redacted": {"42": ["_SECRET_A"]}}));
    }

    #[test]
    fn missing_assets_still_referenced_are_reported() {
        let mut cache = cluster();
        let j = with_assets(
            cache[&42].clone(),
            &["hdd/gone.qcow2", "iso/old.iso"],
            &[("HDD_1", "gone.qcow2"), ("ISO", "old.iso")],
        );
        cache.insert(42, j);
        let p = run(42, opts(), &[], &cache);
        let w = warnings(&p, &cache).unwrap();
        assert_eq!(
            w,
            json!({"missing_assets": {"42": ["hdd/gone.qcow2", "iso/old.iso"]}})
        );
    }

    #[test]
    fn overridden_or_cleared_asset_settings_are_not_reported() {
        let mut cache = cluster();
        let j = with_assets(
            cache[&42].clone(),
            &["hdd/gone.qcow2", "iso/old.iso"],
            &[("HDD_1", "gone.qcow2"), ("ISO", "old.iso")],
        );
        cache.insert(42, j);
        let p = run(42, opts(), &["HDD_1=other.qcow2", "ISO="], &cache);
        assert!(warnings(&p, &cache).is_none());
    }

    #[test]
    fn asset_published_by_a_cloned_parent_is_not_reported_but_a_skipped_one_is() {
        let mut cache = cluster();
        let mut parent = cache[&41].clone();
        parent
            .settings
            .insert("PUBLISH_HDD_1".into(), "gen.qcow2".into());
        cache.insert(41, parent);
        let main = with_assets(
            cache[&42].clone(),
            &["hdd/gen.qcow2"],
            &[("HDD_1", "gen.qcow2")],
        );
        cache.insert(42, main);

        let p = run(42, opts(), &[], &cache);
        assert!(warnings(&p, &cache).is_none());

        let skip = CloneOptions {
            skip_deps: true,
            ..opts()
        };
        let p = run(42, skip, &[], &cache);
        assert_eq!(
            warnings(&p, &cache).unwrap(),
            json!({"missing_assets": {"42": ["hdd/gen.qcow2"]}})
        );
    }

    #[test]
    fn asset_setting_names_match_upstream() {
        for key in [
            "ISO", "ISO_2", "HDD_1", "REPO_0", "ASSET_3", "KERNEL", "INITRD",
        ] {
            assert!(is_asset_setting(key, "x"), "{key}");
        }
        assert!(is_asset_setting("UEFI_PFLASH_VARS", "uefi-vars.qcow2"));
        assert!(!is_asset_setting(
            "UEFI_PFLASH_VARS",
            "/usr/share/OVMF/vars.fd"
        ));
        for key in ["ISO_", "ISO_a", "HDDSIZEGB", "HDD_1_X", "TEST"] {
            assert!(!is_asset_setting(key, "x"), "{key}");
        }
    }

    #[test]
    fn job_parses_from_the_api_envelope() {
        let j = Job::from_json(&json!({"job": {
            "id": 7, "group_id": 3,
            "settings": {"TEST": "t", "N": 5, "GONE": null},
            "parents": {"Chained": [1], "Directly chained": [2], "Parallel": [3]},
            "children": {"Parallel": [9]},
            "missing_assets": ["hdd/a.qcow2"]
        }}))
        .unwrap();
        assert_eq!(j.group_id, Some(3));
        assert_eq!(j.settings, settings(&[("TEST", "t"), ("N", "5")]));
        assert_eq!(j.parents.chained, vec![1]);
        assert_eq!(j.parents.directly_chained, vec![2]);
        assert_eq!(j.parents.parallel, vec![3]);
        assert_eq!(j.children.parallel, vec![9]);
        assert_eq!(j.missing_assets, vec!["hdd/a.qcow2"]);
        assert!(Job::from_json(&json!({"nope": 1})).is_err());
    }

    #[test]
    fn productdir_cases_match_upstream() {
        assert_eq!(
            derive_productdir("/my/case/dir", "/my/case/dir/product", "repo"),
            "repo/product"
        );
        assert_eq!(
            derive_productdir("my/distri", "distri/products/sle", "repo"),
            "repo/products/sle"
        );
        assert_eq!(
            derive_productdir("/my/distri", "products/sle", "repo"),
            "repo/products/sle"
        );
        assert_eq!(
            derive_productdir(
                "/openqa/cache/openqa1-opensuse/tests/opensuse",
                "/openqa/cache/openqa1-opensuse/tests/opensuse/products/opensuse",
                "repo"
            ),
            "repo/products/opensuse"
        );
    }

    #[test]
    fn refspec_args_match_upstream_order_and_values() {
        let pr = refspec_args(
            &Refspec {
                repo: "user/repo",
                branch: "my/branch",
                pull_number: Some(9128),
            },
            Some("repo/product"),
        );
        assert_eq!(
            pr,
            [
                "_GROUP=0",
                "TEST+=@user/repo#my/branch",
                "BUILD=user/repo#9128",
                "CASEDIR=https://github.com/user/repo.git#my/branch",
                "PRODUCTDIR=repo/product",
            ]
        );
        let branch = refspec_args(
            &Refspec {
                repo: "user/repo",
                branch: "my/branch",
                pull_number: None,
            },
            None,
        );
        assert_eq!(branch[2], "BUILD=user/repo#my/branch");
        assert_eq!(branch.len(), 4);
    }
}
