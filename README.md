# Pixiv artist original-image downloader

## Setup

1. Install Rust if you don't have it: https://rustup.rs
2. Get your `PHPSESSID` cookie:
   - Log into pixiv.net in your browser
   - Open DevTools -> Application (Chrome) / Storage (Firefox) -> Cookies -> pixiv.net
   - Copy the value of `PHPSESSID`
3. Build:
   ```
   cargo build --release
   ```

## Usage

```
export PIXIV_SESSION="your_phpsessid_value"
cargo run --release -- https://www.pixiv.net/en/users/28480895
```

or pass the id directly:

```
cargo run --release -- 28480895 --session your_phpsessid_value --out ./downloads
```

## Tiers implemented

- total illustrations < 120  -> download newest 48
- 120 <= total <= 300        -> download newest 96
- total > 300                -> download newest 192

(Your original spec had a gap between 200 and 300 — I filled it by extending
the 96-tier up to 300. Change `tier_limit()` in main.rs if you meant something
else, e.g. a distinct 200-300 tier.)

## Notes / caveats

- Pixiv's Terms of Service prohibit automated scraping. This is intended for
  personal archiving of your own bookmarks/follows, run at a low rate — not
  for redistribution or public-facing tools.
- The `/ajax/user/{id}/profile/all` endpoint returns *all* illust ids with no
  reliable "created date" sort baked in, so this sorts by numeric id
  descending, which is a good proxy for "newest first" but not perfectly
  exact around bulk-uploaded works.
- R-18 works generally won't resolve without a valid, logged-in session.
- No retry/backoff logic is included — if you hit 429s, raise `--delay-ms`
  and lower `--concurrency`.
- Filenames are taken directly from pixiv's CDN URL (e.g.
  `12345678_p0.png`), so multi-page works land as separate files in the same
  folder automatically.
