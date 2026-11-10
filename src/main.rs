use anyhow::{anyhow, Context, Result};
use clap::Parser;
use futures::stream::{self, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::header::{HeaderMap, HeaderValue, COOKIE, REFERER, USER_AGENT};
use serde_json::Value;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

/// Simple timestamped console logger, so it's clear what's happening and when.
fn log(msg: impl AsRef<str>) {
    let now = Instant::now();
    // We just print wall-clock-ish elapsed-since-start via a static OnceLock start time.
    static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    let start = *START.get_or_init(|| now);
    let elapsed = now.duration_since(start).as_secs_f32();
    println!("[{elapsed:>7.2}s] {}", msg.as_ref());
}

const UA: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36";

#[derive(Parser, Debug)]
#[command(about = "Pixiv artist original-image downloader")]
struct Args {
    /// Full profile URL or bare numeric user id
    /// e.g. https://www.pixiv.net/en/users/28480895  or  28480895
    target: String,

    /// PHPSESSID cookie value. Falls back to SESSION from .env.
    #[arg(long)]
    session: Option<String>,

    /// Output directory. Falls back to OUT from .env.
    #[arg(long)]
    out: Option<PathBuf>,

    /// Max concurrent downloads
    #[arg(long, default_value_t = 4)]
    concurrency: usize,

    /// Delay between API calls in ms. Falls back to DELAY_MS from .env.
    #[arg(long)]
    delay_ms: Option<u64>,
}

/// Extract a numeric user id from either a bare id or a full pixiv URL.
fn extract_user_id(input: &str) -> Result<String> {
    if input.chars().all(|c| c.is_ascii_digit()) {
        return Ok(input.to_string());
    }
    // matches .../users/12345 or .../users/12345/...
    let marker = "/users/";
    if let Some(pos) = input.find(marker) {
        let rest = &input[pos + marker.len()..];
        let id: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
        if !id.is_empty() {
            return Ok(id);
        }
    }
    Err(anyhow!("Could not extract a user id from '{input}'"))
}

/// Decide how many illustrations to fetch, per the requested tiers.
/// <120 -> 48, 120..=300 -> 96, >300 -> 192
/// (there's a gap between 120 and 200/300 in the original spec; this fills it
/// by treating everything from 120 up to 300 as the 96 tier)
fn tier_limit(total: usize) -> usize {
    if total < 120 {
        48
    } else if total <= 300 {
        96
    } else {
        192
    }
}

fn client_with_headers(session: &str) -> Result<reqwest::Client> {
    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static(UA));
    headers.insert(REFERER, HeaderValue::from_static("https://www.pixiv.net/"));
    headers.insert(
        COOKIE,
        HeaderValue::from_str(&format!("PHPSESSID={session}"))
            .context("invalid session cookie value")?,
    );
    Ok(reqwest::Client::builder()
        .default_headers(headers)
        .timeout(Duration::from_secs(30))
        .build()?)
}

/// Fetch every illust id belonging to the user (from /profile/all).
async fn fetch_all_illust_ids(client: &reqwest::Client, user_id: &str) -> Result<Vec<u64>> {
    let url = format!("https://www.pixiv.net/ajax/user/{user_id}/profile/all");
    log(format!("GET {url}"));
    let resp: Value = client
        .get(&url)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    if resp.get("error").and_then(Value::as_bool).unwrap_or(false) {
        let msg = resp
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("unknown error");
        return Err(anyhow!("Pixiv API error: {msg}"));
    }

    let illusts = resp
        .pointer("/body/illusts")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("Unexpected response shape from profile/all"))?;

    let mut ids: Vec<u64> = illusts
        .keys()
        .filter_map(|k| k.parse::<u64>().ok())
        .collect();

    // Descending = newest first, since pixiv ids are roughly chronological.
    ids.sort_unstable_by(|a, b| b.cmp(a));
    log(format!("Parsed {} illust ids from profile", ids.len()));
    Ok(ids)
}

#[derive(Debug)]
struct PageUrl {
    original: String,
}

/// Get original-resolution page URLs for one illustration (handles multi-page works).
async fn fetch_pages(client: &reqwest::Client, illust_id: u64) -> Result<Vec<PageUrl>> {
    let url = format!("https://www.pixiv.net/ajax/illust/{illust_id}/pages");
    let resp: Value = client
        .get(&url)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    if resp.get("error").and_then(Value::as_bool).unwrap_or(false) {
        let msg = resp
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("unknown error");
        return Err(anyhow!("Pixiv API error for illust {illust_id}: {msg}"));
    }

    let pages = resp
        .pointer("/body")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("Unexpected response shape from illust/pages"))?;

    let out = pages
        .iter()
        .filter_map(|p| p.pointer("/urls/original").and_then(Value::as_str))
        .map(|s| PageUrl {
            original: s.to_string(),
        })
        .collect();
    Ok(out)
}

enum DownloadOutcome {
    Saved { filename: String, bytes: u64 },
    Skipped { filename: String },
}

async fn download_original(
    client: &reqwest::Client,
    page_url: &str,
    out_dir: &PathBuf,
) -> Result<DownloadOutcome> {
    let filename = page_url
        .rsplit('/')
        .next()
        .unwrap_or("image.jpg")
        .to_string();
    let dest = out_dir.join(&filename);
    if dest.exists() {
        return Ok(DownloadOutcome::Skipped { filename });
    }

    let bytes = client
        .get(page_url)
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    let size = bytes.len() as u64;
    tokio::fs::write(&dest, &bytes).await?;
    Ok(DownloadOutcome::Saved {
        filename,
        bytes: size,
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let args = Args::parse();

    let session = args
        .session
        .or_else(|| std::env::var("SESSION").ok())
        .ok_or_else(|| anyhow!("Укажите --session или переменную SESSION в .env"))?;
    let delay_ms = args
        .delay_ms
        .or_else(|| {
            std::env::var("DELAY_MS")
                .ok()
                .and_then(|value| value.parse().ok())
        })
        .unwrap_or(300);
    let out = args
        .out
        .or_else(|| std::env::var("OUT").ok().map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("downloads"));

    let user_id = extract_user_id(&args.target)?;
    let out_dir = out.join(&user_id);
    tokio::fs::create_dir_all(&out_dir).await?;

    log(format!("Target user id: {user_id}"));
    log(format!("Output directory: {}", out_dir.display()));
    log("Building HTTP client with session cookie...");
    let client = client_with_headers(&session)?;

    log("=== Stage 1/3: fetching illustration list ===");
    let all_ids = fetch_all_illust_ids(&client, &user_id).await?;
    let total = all_ids.len();
    let limit = tier_limit(total);
    let selected: Vec<u64> = all_ids.into_iter().take(limit).collect();

    log(format!(
        "Total illustrations: {total} -> tier limit: {limit} -> will fetch {} works",
        selected.len()
    ));

    // --- Stage 2: resolve page URLs for each selected illust ---
    log("=== Stage 2/3: resolving original image URLs ===");
    let pages_bar = ProgressBar::new(selected.len() as u64);
    pages_bar.set_style(
        ProgressStyle::with_template(
            "  [{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} illusts  ({eta} left)  {msg}",
        )
        .unwrap(),
    );

    let mut all_page_urls: Vec<String> = Vec::new();
    let mut failed_illusts: Vec<u64> = Vec::new();
    for id in &selected {
        pages_bar.set_message(format!("illust {id}"));
        match fetch_pages(&client, *id).await {
            Ok(pages) => {
                pages_bar.println(format!("  ok   illust {id}: {} page(s)", pages.len()));
                all_page_urls.extend(pages.into_iter().map(|p| p.original));
            }
            Err(e) => {
                pages_bar.println(format!("  FAIL illust {id}: {e}"));
                failed_illusts.push(*id);
            }
        }
        pages_bar.inc(1);
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
    pages_bar.finish_with_message("done");

    log(format!(
        "Resolved {} image URLs across {} illusts ({} failed to resolve)",
        all_page_urls.len(),
        selected.len(),
        failed_illusts.len()
    ));

    // --- Stage 3: downloading originals, with live progress + counters ---
    log("=== Stage 3/3: downloading original images ===");
    let dl_bar = ProgressBar::new(all_page_urls.len() as u64);
    dl_bar.set_style(
        ProgressStyle::with_template(
            "  [{elapsed_precise}] {bar:40.green/blue} {pos}/{len} files  ({eta} left)  {msg}",
        )
        .unwrap(),
    );

    let saved_count = Arc::new(AtomicU64::new(0));
    let skipped_count = Arc::new(AtomicU64::new(0));
    let failed_count = Arc::new(AtomicU64::new(0));
    let saved_bytes = Arc::new(AtomicU64::new(0));

    let download_dir = out_dir.clone();
    let bar_ref = dl_bar.clone();
    stream::iter(all_page_urls)
        .for_each_concurrent(args.concurrency, |url| {
            let client = client.clone();
            let out_dir = download_dir.clone();
            let bar = bar_ref.clone();
            let saved_count = saved_count.clone();
            let skipped_count = skipped_count.clone();
            let failed_count = failed_count.clone();
            let saved_bytes = saved_bytes.clone();
            async move {
                match download_original(&client, &url, &out_dir).await {
                    Ok(DownloadOutcome::Saved { filename, bytes }) => {
                        saved_count.fetch_add(1, Ordering::Relaxed);
                        saved_bytes.fetch_add(bytes, Ordering::Relaxed);
                        bar.println(format!(
                            "  saved  {filename}  ({:.1} KB)",
                            bytes as f64 / 1024.0
                        ));
                    }
                    Ok(DownloadOutcome::Skipped { filename }) => {
                        skipped_count.fetch_add(1, Ordering::Relaxed);
                        bar.println(format!("  skip   {filename} (already exists)"));
                    }
                    Err(e) => {
                        failed_count.fetch_add(1, Ordering::Relaxed);
                        bar.println(format!("  FAIL   {url}: {e}"));
                    }
                }
                bar.set_message(format!(
                    "saved {} / skipped {} / failed {}",
                    saved_count.load(Ordering::Relaxed),
                    skipped_count.load(Ordering::Relaxed),
                    failed_count.load(Ordering::Relaxed)
                ));
                bar.inc(1);
            }
        })
        .await;
    dl_bar.finish_with_message("done");

    log("=== Summary ===");
    log(format!(
        "  Saved:   {}",
        saved_count.load(Ordering::Relaxed)
    ));
    log(format!(
        "  Skipped: {}",
        skipped_count.load(Ordering::Relaxed)
    ));
    log(format!(
        "  Failed:  {}",
        failed_count.load(Ordering::Relaxed)
    ));
    log(format!(
        "  Total downloaded: {:.2} MB",
        saved_bytes.load(Ordering::Relaxed) as f64 / (1024.0 * 1024.0)
    ));
    if !failed_illusts.is_empty() {
        log(format!(
            "  Illusts that failed to resolve pages: {:?}",
            failed_illusts
        ));
    }
    log("Done.");
    Ok(())
}
