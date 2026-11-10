use anyhow::{Context, Result, anyhow};
use clap::Parser;
use futures::{StreamExt, stream};
use reqwest::header::{
    CONTENT_LENGTH, CONTENT_TYPE, COOKIE, HeaderMap, HeaderValue, REFERER, USER_AGENT,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const UA: &str =
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126.0 Safari/537.36";
const SCHEMA_VERSION: u32 = 1;

#[derive(Parser, Debug)]
#[command(about = "Pixiv artist original-image downloader")]
struct Args {
    /// Full profile URL or numeric user ID
    target: String,
    /// Falls back to SESSION from .env
    #[arg(long)]
    session: Option<String>,
    #[arg(long)]
    out: Option<PathBuf>,
    #[arg(long, default_value_t = 4, value_parser = parse_concurrency)]
    concurrency: usize,
    #[arg(long)]
    delay_ms: Option<u64>,
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
fn log(s: impl AsRef<str>) {
    println!("{}", s.as_ref());
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
async fn fetch_all(c: &reqwest::Client, uid: &str) -> Result<Vec<u64>> {
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
    let mut ids: Vec<u64> = v
        .pointer("/body/illusts")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("Unexpected profile response"))?
        .keys()
        .filter_map(|k| k.parse().ok())
        .collect();
    ids.sort_unstable_by(|a, b| b.cmp(a));
    Ok(ids)
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
async fn run_downloads(
    m: &mut Manifest,
    tasks: Vec<(usize, usize)>,
    concurrency: usize,
    client: &reqwest::Client,
    out: &Path,
    mp: &Path,
) -> Result<usize> {
    let tasks: Vec<_> = tasks
        .into_iter()
        .map(|(wi, fi)| (wi, fi, m.works[wi].files[fi].clone()))
        .collect();
    for (wi, fi, _) in &tasks {
        m.works[*wi].files[*fi].status = FileStatus::Pending;
    }
    save_manifest(mp, m).await?;
    // One writer consumes completions while transfers are still in flight. No collect-all phase.
    let mut completions = stream::iter(tasks.into_iter().map(|(wi, fi, task)| async move {
        (wi, fi, download_original(client, &task, out).await)
    }))
    .buffer_unordered(concurrency);
    let mut failed = 0;
    while let Some((wi, fi, result)) = completions.next().await {
        let f = &mut m.works[wi].files[fi];
        match result {
            Ok(DownloadOutcome::Saved {
                filename,
                bytes,
                sha256,
            }) => {
                log(format!("saved {filename} ({bytes} bytes)"));
                f.size = Some(bytes);
                f.sha256 = Some(sha256);
                f.status = FileStatus::Completed;
            }
            Ok(DownloadOutcome::Skipped {
                filename,
                bytes,
                sha256,
            }) => {
                log(format!("verified {filename}"));
                f.size = Some(bytes);
                f.sha256 = Some(sha256);
                f.status = FileStatus::Completed;
            }
            Err(_) => {
                // Never persist URL-bearing reqwest errors, credentials or server-provided text.
                failed += 1;
                f.status = FileStatus::Failed("Image transfer or verification failed".into());
                log(format!("FAIL {}", f.path));
            }
        }
        save_manifest(mp, m)
            .await
            .context("Could not persist completed task")?;
    }
    Ok(failed)
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
#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let a = Args::parse();
    let session = a
        .session
        .or_else(|| std::env::var("SESSION").ok())
        .ok_or_else(|| anyhow!("Укажите --session или SESSION"))?;
    let uid = extract_user_id(&a.target)?;
    let out = a
        .out
        .or_else(|| std::env::var("OUT").ok().map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("downloads"))
        .join(&uid);
    tokio::fs::create_dir_all(&out).await?;
    let mp = manifest_path(&out);
    let api = client_with_headers(&session)?;
    let cdn = cdn_client()?;
    let mut m = match load_manifest(&mp, &uid).await {
        Ok(m) => m,
        Err(e)
            if !a.resume
                && !a.retry_failed
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
    let selected = if a.resume || a.retry_failed {
        select_existing(&m, a.retry_failed)
    } else {
        let ids = fetch_all(&api, &uid).await?;
        let limit = tier_limit(ids.len());
        ids.into_iter().take(limit).collect()
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
    save_manifest(&mp, &m).await?;
    let delay = a
        .delay_ms
        .or_else(|| std::env::var("DELAY_MS").ok().and_then(|x| x.parse().ok()))
        .unwrap_or(300);
    let mut resolved_ids = Vec::new();
    for wi in 0..m.works.len() {
        let id = m.works[wi].work_id;
        if !selected.contains(&id) || !m.works[wi].files.is_empty() {
            continue;
        }
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
                log(format!("FAIL resolving {id}"));
            }
        }
        save_manifest(&mp, &m).await?;
        tokio::time::sleep(Duration::from_millis(delay)).await;
    }
    let tasks = selected_tasks(&m, &selected, a.retry_failed, &resolved_ids);
    let failures = run_downloads(&mut m, tasks, a.concurrency, &cdn, &out, &mp).await?;
    let unresolved = m
        .works
        .iter()
        .filter(|w| selected.contains(&w.work_id) && matches!(w.status, FileStatus::Failed(_)))
        .count();
    log(format!(
        "Done: {failures} file failures, {unresolved} resolution failures"
    ));
    if failures + unresolved > 0 {
        return Err(anyhow!("Some tasks failed; use --retry-failed"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn concurrency_bounds() {
        for v in ["0", "33", "-1", "abc"] {
            assert!(Args::try_parse_from(["app", "123", "--concurrency", v]).is_err());
        }
        for v in ["1", "4", "32"] {
            assert!(Args::try_parse_from(["app", "123", "--concurrency", v]).is_ok());
        }
        assert!(Args::try_parse_from(["app", "123", "--resume", "--retry-failed"]).is_err());
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
