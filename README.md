# Movie Madness Lite

A fast, static search-and-browse page for the [Movie Madness](https://www.moviemadness.org/) rental catalog, laid out shelf by shelf the way the store is. Built on [movie-madness-pdx-tmdb](https://github.com/tjsander/movie-madness-pdx-tmdb), with titles, years and posters from TMDB.

## Build

```sh
python3 build.py            # fetch catalog if it changed, look up new TMDB ids, write site/data/
python3 -m http.server -d site 8765
```

`build.py` uses only the Python standard library and reads a TMDB read-access token from the `TMDB_TOKEN` environment variable, or else `~/.config/mmboxmonkey/tmdb-token`.

- **TMDB usage.** Each id is fetched once, throttled to 20 requests/s over 6 connections (TMDB's ceiling is about 50/s), with backoff on 429/5xx. Results are kept in `cache/tmdb.jsonl`. After the first run (about 50 minutes for ~55k titles), a daily rebuild looks up titles new to the catalog, plus 1/120th of the cache, oldest first, once entries are 30 days old. That's about 465 lookups (~25 s) a day, which keeps every entry under ~150 days old, within TMDB's six-month caching limit. Anything that does reach 180 days, e.g. after a long gap between builds, is always refreshed.
- **The token stays local.** The browser never calls the TMDB API. It loads `data/catalog.json` and fetches posters from TMDB's image CDN.
- **Shelves.** The catalog's shelf strings mix levels: `Foreign > Japanese > KUROSAWA, AKIRA` starts at the super-section, while `Hollywood Directors > SPIELBERG, STEVEN` starts at the shelf. The build fetches the store's super-sections (WordPress `genre` terms) and shelves (`location` terms), then files each shelf under the super-section that holds most of its rentals. Each title ends up as super-section › shelf › name shelf.
- **Unmatched rentals.** The build also reads the API's `unmatched.json`: rentals with no TMDB id, about 18.5k of them. Copies with the same cleaned store title and year are grouped into one title. They appear on their shelves with the store's title and no poster, and the **Unmatched** filter in dev view shows only them. Their pop-up links to a TMDB search and to any match the pipeline rejected.
- **Dev view.** Open the page with `?dev=1` to show the Films/TV and Matched/Unmatched filters, which are hidden from everyone else. The browser remembers the setting. `?dev=0`, or the "Dev view ✕" badge, turns it off. Outside dev view both filters are off, so visitors always see every title.
- **Boutique Labels.** A website-only section above "Up front". DVD, Blu-ray and 4K copies whose store title names a boutique label in its edition notes ("(Criterion)", "(Vin Syn)", "(Arrow 4K Restoration)"…) are grouped by label. The patterns are `LABELS` in `build.py`. Labels with fewer than 5 titles share "Other boutique labels". The catalog's `collections` list can hold other curated groups the same way.
- **Catalog.** Downloaded only when the API's `index.json` `updated` changes. Use `--force` to download it anyway, `--no-fetch` to skip it, and `--no-tmdb` to build from the cache alone.

## Output

```
site/index.html                     the whole app: inline CSS + JS, no dependencies
site/data/catalog.json              titles, years, shelves, displays, labels (~1.8 MB gzipped)
site/data/posters.json              poster paths, one per catalog title (~1.2 MB gzipped; loaded in parallel)
site/data/details/{movie,tv,unmatched}/NNN.json  overviews + store titles, loaded when a title is opened
seed/store-terms.json               the store's sections and shelves, used if moviemadness.org can't be reached
```

## Deploying

`.github/workflows/deploy.yml` runs `build.py` daily at about 6am Pacific, on every push to `master`, and when started by hand from the Actions tab. It then publishes `site/` to GitHub Pages. Setup:

1. Add the TMDB read-access token as a repository secret named `TMDB_TOKEN`. The run fails without it rather than publish a site with no posters.
2. In Settings → Pages, set Source to **GitHub Actions**.

`cache/` (TMDB lookups and catalog shards) is kept between runs with the Actions cache. A daily run makes about 465 TMDB refresh lookups (~25 s). If the cache is lost (unused for 7 days), the next run looks everything up again: about 56k lookups, ~50 minutes, still within TMDB's limits.

The link preview tags in `site/index.html` (`og:url`, `og:image`) assume the site lives at `https://tjsander.github.io/movie-madness-lite/`; change them if it doesn't.
