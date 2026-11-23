//! The one GitHub lookup `clone_job`'s `github_ref` needs: a pull request's
//! head repository and branch. Never logs or echoes the token.

use std::sync::OnceLock;
use std::time::Duration;

use percent_encoding::percent_decode_str;
use reqwest::StatusCode;
use reqwest::header::{ACCEPT, HeaderMap};
use rmcp::ErrorData;
use rmcp::model::CallToolResult;
use serde_json::Value;
use url::Url;

use crate::error::{status_kind, tool_error};
use crate::tools::artifact::Bail;

const API_BASE: &str = "https://api.github.com";
const TIMEOUT: Duration = Duration::from_secs(30);
const MAX_BODY_BYTES: usize = 1024 * 1024;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum GithubRef {
    Pull {
        owner: String,
        repo: String,
        number: u64,
    },
    Branch {
        owner: String,
        repo: String,
        branch: String,
    },
}

fn valid_name(s: &str) -> bool {
    !s.is_empty()
        && s != "."
        && s != ".."
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

/// Accepts `https://github.com/<owner>/<repo>/pull/<n>[/...]` and
/// `https://github.com/<owner>/<repo>/tree/<branch>` (http too).
pub(crate) fn parse_github_ref(raw: &str) -> Result<GithubRef, String> {
    let bad = |why: &str| {
        format!(
            "github_ref must be https://github.com/<owner>/<repo>/pull/<n> or \
             https://github.com/<owner>/<repo>/tree/<branch>: {why}"
        )
    };
    let url = Url::parse(raw.trim()).map_err(|_| bad("not a URL"))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(bad("scheme must be http or https"));
    }
    if url.host_str() != Some("github.com")
        || url.port().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(bad("host must be github.com"));
    }
    let mut segments: Vec<&str> = url.path_segments().map_or_else(Vec::new, Iterator::collect);
    while segments.last() == Some(&"") {
        segments.pop();
    }
    let [owner, repo, kind, rest @ ..] = segments.as_slice() else {
        return Err(bad("path is too short"));
    };
    if !valid_name(owner) || !valid_name(repo) {
        return Err(bad("invalid owner or repository name"));
    }
    let (owner, repo) = ((*owner).to_string(), (*repo).to_string());
    match (*kind, rest) {
        ("pull", [number, ..]) => {
            if !number.bytes().all(|b| b.is_ascii_digit()) {
                return Err(bad("pull request number is not numeric"));
            }
            let number = number
                .parse()
                .map_err(|_| bad("pull request number is not numeric"))?;
            Ok(GithubRef::Pull {
                owner,
                repo,
                number,
            })
        }
        ("tree", [_, ..]) => {
            let branch = percent_decode_str(&rest.join("/"))
                .decode_utf8()
                .map_err(|_| bad("branch is not valid UTF-8"))?
                .into_owned();
            Ok(GithubRef::Branch {
                owner,
                repo,
                branch,
            })
        }
        _ => Err(bad(
            "expected /pull/<n> or /tree/<branch> after the repository",
        )),
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PullHead {
    /// `<owner>/<repo>` of the repository the PR branch lives in.
    pub(crate) repo: String,
    pub(crate) branch: String,
}

/// Lazily-built client: no TLS setup until the first PR URL is used.
pub struct GitHub {
    api_base: String,
    token: Option<String>,
    client: OnceLock<reqwest::Client>,
}

impl std::fmt::Debug for GitHub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GitHub")
            .field("api_base", &self.api_base)
            .field("token", &self.token.is_some())
            .finish_non_exhaustive()
    }
}

impl GitHub {
    /// `api_base` is the API root, e.g. `https://api.github.com`.
    #[must_use]
    pub fn new(api_base: &str, token: Option<String>) -> Self {
        Self {
            api_base: api_base.trim_end_matches('/').to_string(),
            token: token.filter(|t| !t.is_empty()),
            client: OnceLock::new(),
        }
    }

    /// `api.github.com`, authenticated when `GITHUB_TOKEN` is set.
    #[must_use]
    pub fn from_env() -> Self {
        Self::new(API_BASE, std::env::var("GITHUB_TOKEN").ok())
    }

    fn client(&self) -> Result<&reqwest::Client, reqwest::Error> {
        if let Some(client) = self.client.get() {
            return Ok(client);
        }
        let built = reqwest::Client::builder()
            .timeout(TIMEOUT)
            .user_agent(concat!("ruoqa-mcp/", env!("CARGO_PKG_VERSION")))
            .redirect(reqwest::redirect::Policy::limited(3))
            .build()?;
        Ok(self.client.get_or_init(|| built))
    }

    pub(crate) async fn pull_head(
        &self,
        owner: &str,
        repo: &str,
        number: u64,
    ) -> Result<PullHead, Bail> {
        let client = self.client().map_err(|e| {
            tool_error(
                "connection",
                None,
                format!("could not set up the GitHub client: {}", e.without_url()),
                None,
            )
        })?;
        let url = format!("{}/repos/{owner}/{repo}/pulls/{number}", self.api_base);
        let mut request = client
            .get(&url)
            .header(ACCEPT, "application/vnd.github+json");
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        let mut response = request.send().await.map_err(transport_failure)?;
        let status = response.status();
        let headers = response.headers().clone();

        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(transport_failure)? {
            if body.len() + chunk.len() > MAX_BODY_BYTES {
                return Err(tool_error(
                    "response_too_large",
                    None,
                    format!("GitHub reply exceeds {MAX_BODY_BYTES} bytes"),
                    None,
                ));
            }
            body.extend_from_slice(&chunk);
        }

        if !status.is_success() {
            return Err(status_failure(
                status,
                &headers,
                &String::from_utf8_lossy(&body),
            ));
        }
        pull_head_from(&body)
    }
}

fn transport_failure(e: reqwest::Error) -> Result<CallToolResult, ErrorData> {
    let kind = if e.is_timeout() {
        "timeout"
    } else {
        "connection"
    };
    tool_error(
        kind,
        None,
        format!("GitHub request failed: {}", e.without_url()),
        None,
    )
}

fn status_failure(status: StatusCode, headers: &HeaderMap, body: &str) -> Bail {
    let limited = matches!(status.as_u16(), 403 | 429)
        && (headers
            .get("x-ratelimit-remaining")
            .is_some_and(|v| v == "0")
            || headers.contains_key("retry-after"));
    let (kind, hint) = if limited {
        (
            "rate_limited",
            "rate limit reached; set GITHUB_TOKEN (unauthenticated requests are limited to \
             60 per hour per IP address)",
        )
    } else {
        let kind = status_kind(status.as_u16());
        let hint = match kind {
            "not_found" => {
                "the pull request does not exist, or its repository is private and \
                 GITHUB_TOKEN is not set"
            }
            "unauthorized" => "GITHUB_TOKEN was rejected",
            _ => "unexpected reply",
        };
        (kind, hint)
    };
    tool_error(
        kind,
        Some(status.as_u16()),
        format!("GitHub API {status}: {hint}"),
        Some(body),
    )
}

fn pull_head_from(body: &[u8]) -> Result<PullHead, Bail> {
    let invalid = |why: &str| {
        tool_error(
            "invalid_response",
            None,
            format!("GitHub pull request reply: {why}"),
            None,
        )
    };
    let value: Value = serde_json::from_slice(body).map_err(|_| invalid("not valid JSON"))?;
    let head = value.get("head").ok_or_else(|| invalid("no head"))?;
    let repo = head
        .get("repo")
        .and_then(|r| r.get("full_name"))
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("the head repository is gone (deleted fork?)"))?;
    let branch = head
        .get("ref")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("no head branch"))?;
    Ok(PullHead {
        repo: repo.to_string(),
        branch: branch.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pull(owner: &str, repo: &str, number: u64) -> GithubRef {
        GithubRef::Pull {
            owner: owner.into(),
            repo: repo.into(),
            number,
        }
    }

    #[test]
    fn pull_urls_parse_with_trailing_slash_and_suffix() {
        for raw in [
            "https://github.com/user/repo/pull/9128",
            "https://github.com/user/repo/pull/9128/",
            "https://github.com/user/repo/pull/9128/files",
            "http://github.com/user/repo/pull/9128",
            "  https://github.com/user/repo/pull/9128  ",
        ] {
            assert_eq!(
                parse_github_ref(raw),
                Ok(pull("user", "repo", 9128)),
                "{raw}"
            );
        }
    }

    #[test]
    fn branch_urls_keep_slashes() {
        assert_eq!(
            parse_github_ref("https://github.com/user/repo/tree/my/branch"),
            Ok(GithubRef::Branch {
                owner: "user".into(),
                repo: "repo".into(),
                branch: "my/branch".into()
            })
        );
        assert_eq!(
            parse_github_ref("https://github.com/u/r/tree/a%23b"),
            Ok(GithubRef::Branch {
                owner: "u".into(),
                repo: "r".into(),
                branch: "a#b".into()
            })
        );
    }

    #[test]
    fn anything_else_is_rejected() {
        for raw in [
            "https://gitlab.com/user/repo/pull/1",
            "https://github.com.evil.example/user/repo/pull/1",
            "https://user@github.com/user/repo/pull/1",
            "https://github.com:8443/user/repo/pull/1",
            "ftp://github.com/user/repo/pull/1",
            "https://github.com/user/repo/pull/",
            "https://github.com/user/repo/pull/x",
            "https://github.com/user/repo/tree",
            "https://github.com/user/repo/issues/3",
            "https://github.com/user/pull/3",
            "https://github.com/us er/repo/pull/3",
            "https://github.com/../repo/pull/3",
            "https://github.com/user/re%2Fpo/pull/3",
            "github.com/user/repo/pull/3",
            "",
        ] {
            assert!(parse_github_ref(raw).is_err(), "{raw:?} should be rejected");
        }
    }

    #[test]
    fn debug_never_shows_the_token() {
        let gh = GitHub::new("https://api.github.com/", Some("s3cret".into()));
        let shown = format!("{gh:?}");
        assert!(!shown.contains("s3cret"));
        assert!(shown.contains("https://api.github.com"));
    }

    #[test]
    fn pull_head_reads_the_head_repo_and_ref() {
        let body = br#"{"head": {"ref": "my/branch", "label": "x:y", "repo": {"full_name": "user/repo"}}}"#;
        assert_eq!(
            pull_head_from(body).ok(),
            Some(PullHead {
                repo: "user/repo".into(),
                branch: "my/branch".into()
            })
        );
        assert!(pull_head_from(br#"{"head": {"ref": "b", "repo": null}}"#).is_err());
        assert!(pull_head_from(b"nope").is_err());
    }
}
