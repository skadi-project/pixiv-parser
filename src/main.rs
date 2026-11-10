use anyhow::{Context, Result, anyhow};
use clap::{Parser, ValueEnum};
use futures::{StreamExt, stream};
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::header::{
    CONTENT_LENGTH, CONTENT_TYPE, COOKIE, HeaderMap, HeaderValue, REFERER, USER_AGENT,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const UA: &str =
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126.0 Safari/537.36";
const SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, ValueEnum)]
#[serde(rename_all = "lowercase")]
enum SortOrder {
    /// Newest first by numeric Pixiv work ID.
    Id,
    /// Oldest first by numeric Pixiv work ID.
    Oldest,
    /// Keep the order returned by the API.
    Api,
}

impl FromStr for SortOrder {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "id" => Ok(Self::Id),
            "oldest" => Ok(Self::Oldest),
            "api" => Ok(Self::Api),
            _ => Err("sort must be id, oldest, or api".into()),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct FileConfig {
    session: Option<String>,
    out: Option<PathBuf>,
    concurrency: Option<usize>,
    delay_ms: Option<u64>,
    limit: Option<usize>,
    sort: Option<SortOrder>,
    dry_run: Option<bool>,
    json_summary: Option<bool>,
    failed_file: Option<PathBuf>,
    no_r18: Option<bool>,
    resume: Option<bool>,
    retry_failed: Option<bool>,
}

#[derive(Debug)]
struct ResolvedConfig {
    session: String,
    out: PathBuf,
    concurrency: usize,
    delay_ms: u64,
    limit: Option<usize>,
    sort: SortOrder,
    dry_run: bool,
    json_summary: bool,
    failed_file: Option<PathBuf>,
    no_r18: bool,
    resume: bool,
    retry_failed: bool,
}

#[derive(Parser, Debug)]
#[command(about = "Pixiv artist original-image downloader")]
struct Args {
    /// Full profile URLs or numeric user IDs
    #[arg(required = true)]
    targets: Vec<String>,
    /// TOML configuration file
    #[arg(long)]
    config: Option<PathBuf>,
    /// Falls back to SESSION from .env
    #[arg(long)]
    session: Option<String>,
    #[arg(long)]
    out: Option<PathBuf>,
    #[arg(long, value_parser = parse_concurrency)]
    concurrency: Option<usize>,
    #[arg(long)]
    delay_ms: Option<u64>,
    #[arg(long, value_parser = parse_limit)]
    limit: Option<usize>,
    #[arg(long, value_enum)]
    sort: Option<SortOrder>,
    #[arg(long)]
    dry_run: bool,
    #[arg(long)]
    json_summary: bool,
    #[arg(long)]
    failed_file: Option<PathBuf>,
    #[arg(long)]
    no_r18: bool,
    /// Reuse the persisted plan and verify all completed files
    #[arg(long, conflicts_with = "retry_failed")]
    resume: bool,
    /// Retry only persisted failed resolutions and files, without profile discovery
    #[arg(long, conflicts_with = "resume")]
    retry_failed: bool,
}

fn parse_concurrency(v: &str) -> std::result::Result<usize, String> {
    let n: usize = v.parse().map_err(|_| "Expected an integer from 1 to 32")?;
    if (1..=32).contains(&n) {
        Ok(n)
    } else {
        Err("Concurrency must be from 1 to 32".into())
    }
}

fn parse_limit(v: &str) -> std::result::Result<usize, String> {
    let n: usize = v.parse().map_err(|_| "Expected a positive integer")?;
    if n > 0 {
        Ok(n)
    } else {
        Err("Limit must be greater than zero".into())
    }
}

fn log(s: impl AsRef<str>, enabled: bool) {
    if enabled {
        println!("{}", s.as_ref());
    }
}

async fn load_file_config(path: Option<&Path>) -> Result<FileConfig> {
    let Some(path) = path else {
        return Ok(FileConfig::default());
    };
    let text = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("Could not read config {}", path.display()))?;
    toml::from_str(&text).context("Invalid TOML configuration")
}

fn env_string(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

fn env_bool(name: &str) -> Result<Option<bool>> {
    let Some(value) = env_string(name) else {
        return Ok(None);
    };
    value
        .parse::<bool>()
        .map(Some)
        .with_context(|| format!("{name} must be true or false"))
}

fn env_usize(name: &str) -> Result<Option<usize>> {
    let Some(value) = env_string(name) else {
        return Ok(None);
    };
    value
        .parse::<usize>()
        .map(Some)
        .with_context(|| format!("{name} must be an integer"))
}

fn env_sort(name: &str) -> Result<Option<SortOrder>> {
    let Some(value) = env_string(name) else {
        return Ok(None);
    };
    value
        .parse()
        .map(Some)
        .map_err(|error: String| anyhow!("{name}: {error}"))
}

async fn resolve_config(args: &Args, file: FileConfig) -> Result<ResolvedConfig> {
    let session = args
        .session
        .clone()
        .or(file.session)
        .or_else(|| env_string("SESSION"))
        .ok_or_else(|| anyhow!("Укажите --session, session в TOML или SESSION"))?;
    let out = args
        .out
        .clone()
        .or(file.out)
        .or_else(|| env_string("OUT").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("downloads"));
    let concurrency = args
        .concurrency
        .or(file.concurrency)
        .or(env_usize("CONCURRENCY")?)
        .unwrap_or(4);
    if !(1..=32).contains(&concurrency) {
        return Err(anyhow!("Concurrency must be from 1 to 32"));
    }
    let delay_ms = args
        .delay_ms
        .or(file.delay_ms)
        .or_else(|| env_string("DELAY_MS").and_then(|x| x.parse().ok()))
        .unwrap_or(300);
    let limit = args
        .limit
        .or(file.limit)
        .or(env_usize("LIMIT")?)
        .map(|value| {
            if value == 0 {
                Err(anyhow!("Limit must be greater than zero"))
            } else {
                Ok(value)
            }
        })
        .transpose()?;
    let sort = args
        .sort
        .or(file.sort)
        .or(env_sort("SORT")?)
        .unwrap_or(SortOrder::Id);
    let dry_run = args.dry_run || file.dry_run.or(env_bool("DRY_RUN")?).unwrap_or(false);
    let json_summary = args.json_summary
        || file
            .json_summary
            .or(env_bool("JSON_SUMMARY")?)
            .unwrap_or(false);
    let failed_file = args
        .failed_file
        .clone()
        .or(file.failed_file)
        .or_else(|| env_string("FAILED_FILE").map(PathBuf::from));
    let no_r18 = args.no_r18 || file.no_r18.or(env_bool("NO_R18")?).unwrap_or(false);
    let resume = args.resume || file.resume.or(env_bool("RESUME")?).unwrap_or(false);
    let retry_failed = args.retry_failed
        || file
            .retry_failed
            .or(env_bool("RETRY_FAILED")?)
            .unwrap_or(false);
    if resume && retry_failed {
        return Err(anyhow!(
            "--resume and --retry-failed are mutually exclusive"
        ));
    }
    Ok(ResolvedConfig {
        session,
        out,
        concurrency,
        delay_ms,
        limit,
        sort,
        dry_run,
        json_summary,
        failed_file,
        no_r18,
        resume,
        retry_failed,
    })
}
fn retryable(error: &anyhow::Error) -> bool {
    error.downcast_ref::<reqwest::Error>().is_some_and(|e| {
        e.is_timeout()
            || e.is_connect()
            || e.is_body()
            || e.is_decode()
            || e.status()
                .is_some_and(|s| s.as_u16() == 408 || s.as_u16() == 429 || s.is_server_error())
    })
}
async fn get_with_retry(c: &reqwest::Client, url: &str) -> Result<reqwest::Response> {
    for attempt in 0..4 {
        match c.get(url).send().await.and_then(|r| r.error_for_status()) {
            Ok(r) => return Ok(r),
            Err(e) => {
                let e = anyhow::Error::from(e);
                if attempt == 3 || !retryable(&e) {
                    return Err(e);
                }
                tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
            }
        }
    }
    unreachable!()
}
fn cdn_client() -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .user_agent(UA)
        .default_headers(HeaderMap::from_iter([(
            REFERER,
            HeaderValue::from_static("https://www.pixiv.net/"),
        )]))
        .timeout(Duration::from_secs(30))
        .build()?)
}
fn client_with_headers(session: &str) -> Result<reqwest::Client> {
    let mut h = HeaderMap::new();
    h.insert(USER_AGENT, HeaderValue::from_static(UA));
    h.insert(REFERER, HeaderValue::from_static("https://www.pixiv.net/"));
    h.insert(
        COOKIE,
        HeaderValue::from_str(&format!("PHPSESSID={session}"))?,
    );
    Ok(reqwest::Client::builder()
        .default_headers(h)
        .timeout(Duration::from_secs(30))
        .build()?)
}
fn extract_user_id(s: &str) -> Result<String> {
    if !s.is_empty() && s.chars().all(|c| c.is_ascii_digit()) {
        return Ok(s.into());
    }
    let rest = s.split_once("/users/").map(|(_, x)| x).unwrap_or("");
    let id: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    if id.is_empty() {
        Err(anyhow!("Could not extract a user id"))
    } else {
        Ok(id)
    }
}
fn tier_limit(n: usize) -> usize {
    if n < 120 {
        48
    } else if n <= 300 {
        96
    } else {
        192
    }
}
#[derive(Clone, Debug, PartialEq)]
struct ProfileWork {
    id: u64,
    r18: Option<bool>,
}

fn explicit_r18(value: &Value) -> Option<bool> {
    let object = value.as_object()?;
    let mut explicit = None;
    for key in ["is_r18", "r18"] {
        match object.get(key) {
            Some(Value::Bool(value)) if *value => return Some(true),
            Some(Value::Bool(value)) => explicit = Some(*value),
            Some(Value::Number(value)) if value.as_i64() == Some(1) => return Some(true),
            Some(Value::Number(value)) if value.as_i64() == Some(0) => explicit = Some(false),
            _ => {}
        }
    }
    explicit
}

fn profile_is_allowed(value: &Value, no_r18: bool) -> bool {
    !no_r18 || explicit_r18(value) != Some(true)
}

fn sort_profile_works(works: &mut [ProfileWork], sort: SortOrder) {
    match sort {
        SortOrder::Id => works.sort_unstable_by_key(|work| std::cmp::Reverse(work.id)),
        SortOrder::Oldest => works.sort_unstable_by_key(|work| work.id),
        SortOrder::Api => {}
    }
}

fn limit_profile_works(works: Vec<ProfileWork>, limit: Option<usize>) -> Vec<ProfileWork> {
    match limit {
        Some(limit) => works.into_iter().take(limit).collect(),
        None => {
            let tier = tier_limit(works.len());
            works.into_iter().take(tier).collect()
        }
    }
}

async fn fetch_all(
    c: &reqwest::Client,
    uid: &str,
    sort: SortOrder,
    no_r18: bool,
) -> Result<Vec<ProfileWork>> {
    let v: Value = get_with_retry(
        c,
        &format!("https://www.pixiv.net/ajax/user/{uid}/profile/all"),
    )
    .await?
    .json()
    .await?;
    if v.get("error").and_then(Value::as_bool).unwrap_or(false) {
        return Err(anyhow!("Pixiv API error"));
    }
    let mut works: Vec<ProfileWork> = v
        .pointer("/body/illusts")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("Unexpected profile response"))?
        .iter()
        .filter_map(|(id, value)| {
            let id = id.parse().ok()?;
            let r18 = explicit_r18(value);
            if !profile_is_allowed(value, no_r18) {
                None
            } else {
                Some(ProfileWork { id, r18 })
            }
        })
        .collect();
    sort_profile_works(&mut works, sort);
    Ok(works)
}
async fn fetch_pages(c: &reqwest::Client, id: u64) -> Result<Vec<String>> {
    let v: Value = get_with_retry(c, &format!("https://www.pixiv.net/ajax/illust/{id}/pages"))
        .await?
        .json()
        .await?;
    if v.get("error").and_then(Value::as_bool).unwrap_or(false) {
        return Err(anyhow!("Pixiv API error"));
    }
    v.pointer("/body")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("Unexpected pages response"))?
        .iter()
        .map(|p| {
            p.pointer("/urls/original")
                .and_then(Value::as_str)
                .map(String::from)
                .ok_or_else(|| anyhow!("Page has no original URL"))
        })
        .collect()
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case", tag = "state", content = "error")]
enum FileStatus {
    Pending,
    Completed,
    Failed(String),
}
#[derive(Debug, Serialize, Deserialize, Clone)]
struct ManifestFile {
    page_identity: String,
    url: String,
    path: String,
    size: Option<u64>,
    sha256: Option<String>,
    status: FileStatus,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
struct ManifestWork {
    work_id: u64,
    /// Resolution status; each page has its own transfer status.
    status: FileStatus,
    files: Vec<ManifestFile>,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
struct Manifest {
    schema_version: u32,
    author_id: String,
    works: Vec<ManifestWork>,
}
fn manifest_path(dir: &Path) -> PathBuf {
    dir.join(".pixiv-manifest.json")
}
fn validate_path(p: &str) -> Result<()> {
    // Portable single filename only: no traversal, Windows ADS/devices or state/temp paths.
    let upper = p.split('.').next().unwrap_or("").to_ascii_uppercase();
    if p.is_empty()
        || p == "."
        || p == ".."
        || p.starts_with('.')
        || p.ends_with([' ', '.'])
        || p.chars()
            .any(|c| c.is_control() || "/\\:<>\"|?*".contains(c))
        || p.ends_with(".part")
        || ["CON", "PRN", "AUX", "NUL"].contains(&upper.as_str())
        || (upper.len() == 4
            && (upper.starts_with("COM") || upper.starts_with("LPT"))
            && matches!(upper.as_bytes()[3], b'1'..=b'9'))
    {
        return Err(anyhow!("Unsafe manifest filename"));
    }
    Ok(())
}
fn validate_manifest(m: &Manifest, author: &str) -> Result<()> {
    if m.schema_version != SCHEMA_VERSION || m.author_id != author {
        return Err(anyhow!("Manifest schema or author mismatch"));
    }
    let mut ids = HashSet::new();
    let mut paths = HashSet::new();
    for w in &m.works {
        if !ids.insert(w.work_id) {
            return Err(anyhow!("Duplicate work ID"));
        }
        for (i, f) in w.files.iter().enumerate() {
            validate_path(&f.path)?;
            if f.page_identity != format!("{}:{i}", w.work_id)
                || !paths.insert(f.path.to_ascii_lowercase())
            {
                return Err(anyhow!("Duplicate file or invalid page identity"));
            }
            if f.size.is_some() != f.sha256.is_some()
                || f.sha256
                    .as_ref()
                    .is_some_and(|h| h.len() != 64 || !h.bytes().all(|b| b.is_ascii_hexdigit()))
            {
                return Err(anyhow!("Invalid file integrity metadata"));
            }
            let url = reqwest::Url::parse(&f.url)?;
            if !matches!(url.scheme(), "http" | "https")
                || !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
            {
                return Err(anyhow!("Invalid image URL"));
            }
        }
    }
    Ok(())
}
async fn load_manifest(path: &Path, author: &str) -> Result<Manifest> {
    let b = tokio::fs::read(path).await?;
    let m: Manifest = serde_json::from_slice(&b).context("Invalid manifest")?;
    validate_manifest(&m, author)?;
    Ok(m)
}
async fn ensure_regular_or_missing(path: &Path) -> Result<()> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(md) if md.is_file() => Ok(()),
        Ok(_) => Err(anyhow!("Archive path is not a regular file")),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}
async fn save_manifest(path: &Path, m: &Manifest) -> Result<()> {
    let tmp = path.with_extension("json.tmp");
    ensure_regular_or_missing(path).await?;
    ensure_regular_or_missing(&tmp).await?;
    let bytes = serde_json::to_vec_pretty(m)?;
    let mut f = tokio::fs::File::create(&tmp).await?;
    f.write_all(&bytes).await?;
    f.sync_all().await?;
    drop(f);
    tokio::fs::rename(tmp, path)
        .await
        .context("Atomic manifest replacement failed")?;
    Ok(())
}
fn filename(url: &str) -> Result<String> {
    let u = reqwest::Url::parse(url)?;
    let f = u
        .path_segments()
        .and_then(|mut x| x.next_back())
        .ok_or_else(|| anyhow!("Invalid image filename"))?;
    validate_path(f)?;
    Ok(f.into())
}
async fn hash_file(path: &Path) -> Result<(u64, String)> {
    let mut f = tokio::fs::File::open(path).await?;
    let mut h = Sha256::new();
    let mut n = 0;
    let mut b = [0u8; 65536];
    loop {
        let z = f.read(&mut b).await?;
        if z == 0 {
            break;
        }
        h.update(&b[..z]);
        n += z as u64;
    }
    Ok((n, format!("{:x}", h.finalize())))
}
async fn verified_file(path: &Path, size: Option<u64>, hash: Option<&str>) -> Result<bool> {
    let md = match tokio::fs::symlink_metadata(path).await {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e.into()),
    };
    if !md.is_file() || md.len() == 0 {
        return Ok(false);
    }
    match (size, hash) {
        (Some(size), Some(hash)) if md.len() == size => {
            let (n, actual) = hash_file(path).await?;
            Ok(n == size && actual == hash)
        }
        _ => Ok(false),
    }
}
async fn stream_response(mut r: reqwest::Response, path: &Path) -> Result<(u64, String)> {
    let mut f = tokio::fs::File::create(path).await?;
    let mut h = Sha256::new();
    let mut n = 0;
    while let Some(c) = r.chunk().await? {
        f.write_all(&c).await?;
        h.update(&c);
        n += c.len() as u64;
    }
    f.flush().await?;
    f.sync_all().await?;
    Ok((n, format!("{:x}", h.finalize())))
}
#[derive(Debug)]
enum DownloadOutcome {
    Saved {
        filename: String,
        bytes: u64,
        sha256: String,
    },
    Skipped {
        filename: String,
        bytes: u64,
        sha256: String,
    },
}
async fn download_original(
    c: &reqwest::Client,
    task: &ManifestFile,
    dir: &Path,
) -> Result<DownloadOutcome> {
    validate_path(&task.path)?;
    let filename = task.path.clone();
    let dest = dir.join(&filename);
    ensure_regular_or_missing(&dest).await?;
    if verified_file(&dest, task.size, task.sha256.as_deref()).await? {
        return Ok(DownloadOutcome::Skipped {
            filename,
            bytes: task.size.unwrap(),
            sha256: task.sha256.clone().unwrap(),
        });
    }
    let part = dir.join(format!("{filename}.part"));
    ensure_regular_or_missing(&part).await?;
    for attempt in 0..4 {
        let result: Result<DownloadOutcome> = async {
            let response = c.get(&task.url).send().await?.error_for_status()?;
            let ct = response
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|x| x.to_str().ok())
                .unwrap_or("");
            if !ct
                .split(';')
                .next()
                .unwrap_or("")
                .trim()
                .to_ascii_lowercase()
                .starts_with("image/")
            {
                return Err(anyhow!("Expected image Content-Type"));
            }
            let length = response
                .headers()
                .get(CONTENT_LENGTH)
                .map(|x| {
                    x.to_str()
                        .context("Invalid Content-Length")?
                        .parse::<u64>()
                        .context("Invalid Content-Length")
                })
                .transpose()?;
            // Unknown legacy files can only be adopted after a nonzero CDN size match.
            // A known corrupt file MUST download even when its size matches.
            if task.size.is_none()
                && task.sha256.is_none()
                && let Ok(md) = tokio::fs::symlink_metadata(&dest).await
                && md.is_file()
                && md.len() > 0
                && length == Some(md.len())
            {
                let (bytes, sha256) = hash_file(&dest).await?;
                if Some(bytes) == length {
                    return Ok(DownloadOutcome::Skipped {
                        filename: filename.clone(),
                        bytes,
                        sha256,
                    });
                }
            }
            let (bytes, sha256) = stream_response(response, &part).await?;
            if bytes == 0 || length.is_some_and(|n| n != bytes) {
                return Err(anyhow!(
                    "Empty download or size differs from Content-Length"
                ));
            }
            tokio::fs::rename(&part, &dest).await?;
            Ok(DownloadOutcome::Saved {
                filename: filename.clone(),
                bytes,
                sha256,
            })
        }
        .await;
        match result {
            Ok(outcome) => return Ok(outcome),
            Err(error) => {
                match tokio::fs::remove_file(&part).await {
                    Ok(()) => (),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
                    Err(e) => {
                        return Err(error.context(format!("Could not remove temporary file: {e}")));
                    }
                }
                if attempt == 3 || !retryable(&error) {
                    return Err(error);
                }
                tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
            }
        }
    }
    unreachable!()
}
fn planned_files(id: u64, urls: Vec<String>) -> Result<Vec<ManifestFile>> {
    if urls.is_empty() {
        return Err(anyhow!("No original page URLs returned"));
    }
    urls.into_iter()
        .enumerate()
        .map(|(page, url)| {
            Ok(ManifestFile {
                page_identity: format!("{id}:{page}"),
                path: filename(&url)?,
                url,
                size: None,
                sha256: None,
                status: FileStatus::Pending,
            })
        })
        .collect()
}
fn selected_tasks(
    m: &Manifest,
    selected: &[u64],
    retry: bool,
    newly_resolved: &[u64],
) -> Vec<(usize, usize)> {
    m.works
        .iter()
        .enumerate()
        .filter(|(_, w)| selected.contains(&w.work_id))
        .flat_map(|(wi, w)| {
            w.files
                .iter()
                .enumerate()
                .filter(move |(_, f)| {
                    !retry
                        || newly_resolved.contains(&w.work_id)
                        || matches!(f.status, FileStatus::Failed(_))
                })
                .map(move |(fi, _)| (wi, fi))
        })
        .collect()
}
#[derive(Debug, Default, PartialEq)]
struct DownloadStats {
    saved: usize,
    skipped: usize,
    failed: usize,
}

fn progress_bar(total: usize, label: String, enabled: bool) -> ProgressBar {
    let bar = if enabled {
        ProgressBar::new(total as u64)
    } else {
        ProgressBar::hidden()
    };
    bar.set_style(
        ProgressStyle::with_template(
            "{prefix} [{elapsed_precise}] {bar:30.cyan/blue} {pos}/{len} ({eta}) {msg}",
        )
        .expect("valid progress template"),
    );
    bar.set_prefix(label);
    bar
}

async fn run_downloads(
    m: &mut Manifest,
    tasks: Vec<(usize, usize)>,
    concurrency: usize,
    client: &reqwest::Client,
    out: &Path,
    mp: &Path,
    verbose: bool,
) -> Result<DownloadStats> {
    let tasks: Vec<_> = tasks
        .into_iter()
        .map(|(wi, fi)| (wi, fi, m.works[wi].files[fi].clone()))
        .collect();
    for (wi, fi, _) in &tasks {
        m.works[*wi].files[*fi].status = FileStatus::Pending;
    }
    save_manifest(mp, m).await?;
    let bar = progress_bar(
        tasks.len(),
        format!("Автор {}: файлы", m.author_id),
        verbose,
    );
    // One writer consumes completions while transfers are still in flight. No collect-all phase.
    let mut completions = stream::iter(tasks.into_iter().map(|(wi, fi, task)| async move {
        (wi, fi, download_original(client, &task, out).await)
    }))
    .buffer_unordered(concurrency);
    let mut stats = DownloadStats::default();
    while let Some((wi, fi, result)) = completions.next().await {
        let f = &mut m.works[wi].files[fi];
        match result {
            Ok(DownloadOutcome::Saved {
                filename,
                bytes,
                sha256,
            }) => {
                stats.saved += 1;
                bar.set_message(format!("сохранён {filename} ({bytes} байт)"));
                f.size = Some(bytes);
                f.sha256 = Some(sha256);
                f.status = FileStatus::Completed;
            }
            Ok(DownloadOutcome::Skipped {
                filename,
                bytes,
                sha256,
            }) => {
                stats.skipped += 1;
                bar.set_message(format!("проверен {filename}"));
                f.size = Some(bytes);
                f.sha256 = Some(sha256);
                f.status = FileStatus::Completed;
            }
            Err(_) => {
                // Never persist URL-bearing reqwest errors, credentials or server-provided text.
                stats.failed += 1;
                f.status = FileStatus::Failed("Image transfer or verification failed".into());
                bar.println(format!("Ошибка загрузки: {}", f.path));
            }
        }
        save_manifest(mp, m)
            .await
            .context("Could not persist completed task")?;
        bar.inc(1);
        bar.set_message(format!(
            "сохранено {} / проверено {} / ошибок {}",
            stats.saved, stats.skipped, stats.failed
        ));
    }
    bar.finish_and_clear();
    Ok(stats)
}
fn select_existing(m: &Manifest, retry: bool) -> Vec<u64> {
    m.works
        .iter()
        .filter(|w| {
            !retry
                || matches!(w.status, FileStatus::Failed(_))
                || w.files
                    .iter()
                    .any(|f| matches!(f.status, FileStatus::Failed(_)))
        })
        .map(|w| w.work_id)
        .collect()
}
#[derive(Debug, Serialize, PartialEq)]
struct AuthorSummary {
    target: String,
    author_id: Option<String>,
    selected_works: usize,
    planned_files: usize,
    downloaded_files: usize,
    skipped_files: usize,
    file_failures: usize,
    resolution_failures: usize,
    status: String,
}

impl AuthorSummary {
    fn failed(target: String) -> Self {
        Self {
            target,
            author_id: None,
            selected_works: 0,
            planned_files: 0,
            downloaded_files: 0,
            skipped_files: 0,
            file_failures: 0,
            resolution_failures: 1,
            status: "failed".into(),
        }
    }
}

async fn run_author(target: &str, config: &ResolvedConfig) -> Result<AuthorSummary> {
    let uid = extract_user_id(target)?;
    let out = config.out.join(&uid);
    if !config.dry_run {
        tokio::fs::create_dir_all(&out).await?;
    }
    let mp = manifest_path(&out);
    let api = client_with_headers(&config.session)?;
    let cdn = cdn_client()?;
    let mut m = match load_manifest(&mp, &uid).await {
        Ok(m) => m,
        Err(e)
            if !config.resume
                && !config.retry_failed
                && e.downcast_ref::<std::io::Error>()
                    .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
        {
            Manifest {
                schema_version: SCHEMA_VERSION,
                author_id: uid.clone(),
                works: vec![],
            }
        }
        Err(e) => return Err(e.context("Could not load author manifest")),
    };
    let selected = if config.resume || config.retry_failed {
        select_existing(&m, config.retry_failed)
    } else {
        let works = fetch_all(&api, &uid, config.sort, config.no_r18).await?;
        limit_profile_works(works, config.limit)
            .into_iter()
            .map(|work| work.id)
            .collect()
    };
    // Commit every selected work ID before resolving URLs, so a crash preserves the plan.
    for id in &selected {
        if !m.works.iter().any(|w| w.work_id == *id) {
            m.works.push(ManifestWork {
                work_id: *id,
                status: FileStatus::Pending,
                files: vec![],
            });
        }
    }
    if !config.dry_run {
        save_manifest(&mp, &m).await?;
    }
    let resolve_count = m
        .works
        .iter()
        .filter(|w| selected.contains(&w.work_id) && w.files.is_empty())
        .count();
    let resolve_bar = progress_bar(
        resolve_count,
        format!("Автор {uid}: страницы"),
        !config.json_summary,
    );
    let mut resolved_ids = Vec::new();
    for wi in 0..m.works.len() {
        let id = m.works[wi].work_id;
        if !selected.contains(&id) || !m.works[wi].files.is_empty() {
            continue;
        }
        resolve_bar.set_message(format!("работа {id}"));
        match fetch_pages(&api, id)
            .await
            .and_then(|urls| planned_files(id, urls))
        {
            Ok(files) => {
                let previous = m.works[wi].clone();
                m.works[wi].files = files;
                m.works[wi].status = FileStatus::Completed;
                if validate_manifest(&m, &uid).is_err() {
                    m.works[wi] = previous;
                    m.works[wi].status = FileStatus::Failed("Unsafe or duplicate page plan".into());
                } else {
                    resolved_ids.push(id);
                }
            }
            Err(_) => {
                m.works[wi].status = FileStatus::Failed("Page resolution failed".into());
                resolve_bar.println(format!("Ошибка получения страниц работы {id}"));
            }
        }
        if !config.dry_run {
            save_manifest(&mp, &m).await?;
        }
        resolve_bar.inc(1);
        tokio::time::sleep(Duration::from_millis(config.delay_ms)).await;
    }
    resolve_bar.finish_and_clear();
    let tasks = selected_tasks(&m, &selected, config.retry_failed, &resolved_ids);
    let planned_files = tasks.len();
    let stats = if config.dry_run {
        DownloadStats::default()
    } else {
        run_downloads(
            &mut m,
            tasks,
            config.concurrency,
            &cdn,
            &out,
            &mp,
            !config.json_summary,
        )
        .await?
    };
    let resolution_failures = m
        .works
        .iter()
        .filter(|w| selected.contains(&w.work_id) && matches!(w.status, FileStatus::Failed(_)))
        .count();
    log(
        format!(
            "Done: {} file failures, {resolution_failures} resolution failures",
            stats.failed
        ),
        !config.json_summary,
    );
    Ok(AuthorSummary {
        target: target.into(),
        author_id: Some(uid),
        selected_works: selected.len(),
        planned_files,
        downloaded_files: stats.saved,
        skipped_files: stats.skipped,
        file_failures: stats.failed,
        resolution_failures,
        status: if config.dry_run {
            "dry-run".into()
        } else if stats.failed + resolution_failures > 0 {
            "failed".into()
        } else {
            "ok".into()
        },
    })
}

async fn write_failed_file(path: &Path, summaries: &[AuthorSummary]) -> Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        tokio::fs::create_dir_all(parent).await?;
    }
    let failed: Vec<_> = summaries
        .iter()
        .filter(|summary| summary.status == "failed")
        .map(|summary| {
            serde_json::json!({
                "target": summary.target,
                "author_id": summary.author_id,
                "resolution_failures": summary.resolution_failures,
                "file_failures": summary.file_failures,
            })
        })
        .collect();
    tokio::fs::write(path, serde_json::to_vec_pretty(&failed)?).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let args = Args::parse();
    let file = load_file_config(args.config.as_deref()).await?;
    let config = resolve_config(&args, file).await?;
    let mut summaries = Vec::with_capacity(args.targets.len());
    for target in &args.targets {
        match run_author(target, &config).await {
            Ok(summary) => summaries.push(summary),
            Err(error) => {
                log(
                    format!("FAIL {target}: author run failed"),
                    !config.json_summary,
                );
                summaries.push(AuthorSummary::failed(target.clone()));
                let _ = error;
            }
        }
    }
    if !config.dry_run
        && let Some(path) = config.failed_file.as_deref()
    {
        write_failed_file(path, &summaries).await?;
    }
    if config.json_summary {
        println!("{}", serde_json::to_string_pretty(&summaries)?);
    }
    if summaries.iter().any(|summary| summary.status == "failed") {
        return Err(anyhow!("Some tasks failed; use --retry-failed"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn config_toml_parses_stage4_options() {
        let config: FileConfig = toml::from_str(
            r#"
            session = "from-file"
            out = "archive"
            concurrency = 2
            delay_ms = 125
            limit = 7
            sort = "oldest"
            dry_run = true
            json_summary = true
            failed_file = "failed.json"
            no_r18 = true
            "#,
        )
        .unwrap();
        assert_eq!(config.session.as_deref(), Some("from-file"));
        assert_eq!(config.limit, Some(7));
        assert_eq!(config.sort, Some(SortOrder::Oldest));
        assert!(config.dry_run.unwrap());
        assert!(config.no_r18.unwrap());
    }

    #[test]
    fn limit_uses_explicit_value_or_tier() {
        let works: Vec<_> = (1..=121).map(|id| ProfileWork { id, r18: None }).collect();
        assert_eq!(limit_profile_works(works.clone(), Some(7)).len(), 7);
        assert_eq!(limit_profile_works(works, None).len(), 96);
    }

    #[test]
    fn sort_orders_id_oldest_and_api() {
        let source = || {
            vec![
                ProfileWork { id: 3, r18: None },
                ProfileWork { id: 1, r18: None },
                ProfileWork { id: 2, r18: None },
            ]
        };
        let mut newest = source();
        sort_profile_works(&mut newest, SortOrder::Id);
        assert_eq!(
            newest.iter().map(|work| work.id).collect::<Vec<_>>(),
            [3, 2, 1]
        );
        let mut oldest = source();
        sort_profile_works(&mut oldest, SortOrder::Oldest);
        assert_eq!(
            oldest.iter().map(|work| work.id).collect::<Vec<_>>(),
            [1, 2, 3]
        );
        let mut api = source();
        sort_profile_works(&mut api, SortOrder::Api);
        assert_eq!(
            api.iter().map(|work| work.id).collect::<Vec<_>>(),
            [3, 1, 2]
        );
    }

    #[test]
    fn args_accept_multiple_targets_and_stage4_flags() {
        let args = Args::try_parse_from([
            "app",
            "123",
            "https://www.pixiv.net/en/users/456",
            "--limit",
            "5",
            "--sort",
            "api",
            "--no-r18",
            "--dry-run",
        ])
        .unwrap();
        assert_eq!(args.targets, ["123", "https://www.pixiv.net/en/users/456"]);
        assert_eq!(args.limit, Some(5));
        assert_eq!(args.sort, Some(SortOrder::Api));
        assert!(args.no_r18 && args.dry_run);
    }

    #[test]
    fn no_r18_only_excludes_explicit_true_metadata() {
        let true_value = serde_json::json!({"is_r18": true});
        let false_value = serde_json::json!({"r18": false});
        let unknown_value = serde_json::json!({"title": "unknown"});
        assert_eq!(explicit_r18(&true_value), Some(true));
        assert_eq!(explicit_r18(&false_value), Some(false));
        assert_eq!(explicit_r18(&unknown_value), None);
        assert!(!profile_is_allowed(&true_value, true));
        assert!(profile_is_allowed(&false_value, true));
        assert!(profile_is_allowed(&unknown_value, true));
        assert!(profile_is_allowed(&true_value, false));
    }

    async fn mock_server(
        responses: Vec<&'static str>,
    ) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let mut requests = Vec::new();
            for response in responses {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = vec![0; 8192];
                let n = socket.read(&mut request).await.unwrap();
                requests.push(String::from_utf8_lossy(&request[..n]).to_string());
                socket.write_all(response.as_bytes()).await.unwrap();
            }
            requests
        });
        (format!("http://{address}/image.png"), task)
    }
    fn temp_dir(name: &str) -> PathBuf {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "pixiv-{name}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ))
    }
    fn task(url: &str) -> ManifestFile {
        planned_files(1, vec![url.into()]).unwrap().remove(0)
    }
    #[tokio::test]
    async fn streams_retries_and_keeps_cookie_off_cdn() {
        let (url, server) = mock_server(vec![
            "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nContent-Length: 4\r\nConnection: close\r\n\r\ndata",
        ]).await;
        let dir = temp_dir("download");
        tokio::fs::create_dir_all(&dir).await.unwrap();
        tokio::fs::write(dir.join("image.png.part"), b"stale")
            .await
            .unwrap();
        let mut t = task(&url);
        let result = download_original(&cdn_client().unwrap(), &t, &dir)
            .await
            .unwrap();
        if let DownloadOutcome::Saved { bytes, sha256, .. } = result {
            assert_eq!(bytes, 4);
            t.size = Some(bytes);
            t.sha256 = Some(sha256);
        } else {
            panic!("expected download");
        }
        assert_eq!(
            tokio::fs::read(dir.join("image.png")).await.unwrap(),
            b"data"
        );
        assert!(!dir.join("image.png.part").exists());
        for request in server.await.unwrap() {
            assert!(!request.to_ascii_lowercase().contains("cookie:"));
        }
        assert!(matches!(
            download_original(&cdn_client().unwrap(), &t, &dir)
                .await
                .unwrap(),
            DownloadOutcome::Skipped { .. }
        ));
        tokio::fs::remove_dir_all(dir).await.unwrap();
    }
    #[tokio::test]
    async fn rejects_html_without_final_file() {
        let (url, server) = mock_server(vec!["HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: 4\r\nConnection: close\r\n\r\noops"]).await;
        let dir = temp_dir("html");
        tokio::fs::create_dir_all(&dir).await.unwrap();
        assert!(
            download_original(&cdn_client().unwrap(), &task(&url), &dir)
                .await
                .is_err()
        );
        assert!(!dir.join("image.png").exists());
        assert!(!dir.join("image.png.part").exists());
        server.await.unwrap();
        tokio::fs::remove_dir_all(dir).await.unwrap();
    }
    #[test]
    fn unsafe_paths_and_manifest_identity() {
        for p in [
            "../x",
            "a/b",
            "a\\b",
            "/tmp/x",
            "C:x",
            "x:ads",
            ".pixiv-manifest.json",
            "NUL.png",
            "x.part",
            "x.",
        ] {
            assert!(validate_path(p).is_err(), "{p}");
        }
        let mut m = Manifest {
            schema_version: SCHEMA_VERSION,
            author_id: "1".into(),
            works: vec![],
        };
        assert!(validate_manifest(&m, "2").is_err());
        m.schema_version += 1;
        assert!(validate_manifest(&m, "1").is_err());
    }
    #[tokio::test]
    async fn same_size_corruption_is_not_verified() {
        let dir = temp_dir("hash");
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let p = dir.join("x.png");
        tokio::fs::write(&p, b"data").await.unwrap();
        let (n, hash) = hash_file(&p).await.unwrap();
        assert!(verified_file(&p, Some(n), Some(&hash)).await.unwrap());
        tokio::fs::write(&p, b"oops").await.unwrap();
        assert!(!verified_file(&p, Some(n), Some(&hash)).await.unwrap());
        tokio::fs::remove_dir_all(dir).await.unwrap();
    }
    #[test]
    fn retry_selects_old_failures_not_pending_or_completed() {
        let mut files = planned_files(
            1,
            vec![
                "https://i.pximg.net/one.png".into(),
                "https://i.pximg.net/two.png".into(),
            ],
        )
        .unwrap();
        files[0].status = FileStatus::Failed("failure".into());
        let m = Manifest {
            schema_version: SCHEMA_VERSION,
            author_id: "1".into(),
            works: vec![
                ManifestWork {
                    work_id: 1,
                    status: FileStatus::Completed,
                    files,
                },
                ManifestWork {
                    work_id: 2,
                    status: FileStatus::Pending,
                    files: vec![],
                },
                ManifestWork {
                    work_id: 3,
                    status: FileStatus::Failed("resolve".into()),
                    files: vec![],
                },
            ],
        };
        assert_eq!(select_existing(&m, true), vec![1, 3]);
        assert_eq!(selected_tasks(&m, &[1, 3], true, &[]), vec![(0, 0)]);
        assert_eq!(select_existing(&m, false), vec![1, 2, 3]);
    }
}
