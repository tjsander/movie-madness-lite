#!/usr/bin/env python3
"""Build the Movie Madness Lite static site.

1. Pulls the catalog shards from the movie-madness-pdx-tmdb API (skipped when
   index.json's "updated" hasn't changed since the last run).
2. Looks up each TMDB id once, caching the result in cache/tmdb.jsonl so later
   runs only ask TMDB about new ids, plus a steady share of the oldest entries (see REFRESH_*).
   Requests are throttled well under TMDB's limits and back off on 429/5xx.
3. Writes site/data/catalog.json (everything needed to search and browse) and
   site/data/details/{movie,tv}/NNN.json (overviews and rental links, loaded on
   demand). The TMDB token is only used here, never shipped to the browser.

Usage: python3 build.py [--no-fetch] [--no-tmdb]
"""
import argparse
import gzip
import html
import json
import os
import re
import sys
import threading
import time
import unicodedata
import urllib.error
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

API = "https://tjsander.github.io/movie-madness-pdx-tmdb"
TMDB = "https://api.themoviedb.org/3"
TOKEN_FILE = Path.home() / ".config/mmboxmonkey/tmdb-token"  # or the TMDB_TOKEN environment variable
KINDS = ("movie", "tv")
KIND_CODE = {"movie": 0, "tv": 1, "unmatched": 2}
SHARDS = 1000

ROOT = Path(__file__).resolve().parent
CACHE = ROOT / "cache"
SITE = ROOT / "site"
SEED = ROOT / "seed"  # committed fallbacks for a fresh checkout (e.g. CI) when a live fetch fails

RATE = 20          # TMDB requests per second (TMDB's ceiling is ~50/s per IP)
WORKERS = 6        # concurrent connections (TMDB allows ~20)
CACHE_DAYS = 180   # TMDB's terms allow caching data for up to six months: always refreshed by then
# Each build refreshes 1/REFRESH_CYCLE_DAYS of the cache, oldest first (once past REFRESH_MIN_DAYS),
# so with daily builds every entry comes round about every 120 days at an even ~465 lookups a day
# (~25 s), rather than a whole first run's worth expiring on one day.
REFRESH_CYCLE_DAYS = 120
REFRESH_MIN_DAYS = 30

FORMATS = ["4K UHD", "Blu-Ray 3D", "Blu-Ray", "DVD", "DVD-R", "VHS", "Book", ""]

STORE = "https://www.moviemadness.org/wp-json/wp/v2"

# The store's super-sections (its "genre" terms) are sometimes abbreviated.
GENRE_NAMES = {
    "Hollywood Dir.": "Hollywood Directors",
    "Classic Dir.": "Classic Directors",
    "Horror Dir.": "Horror Directors",
    "Ind. Filmmaker": "Independent Filmmakers",
    "Cult Director": "Cult Directors",
    "Foreign A.a.": "Foreign Academy Awards",
    "Bizazrre": "Bizarre",
}

# Store areas that hold several super-sections. Any section with "TV" in its name goes under
# TV, and "Classic Actors" goes under "Classic" (likewise any "<section> <more>" name).
# AREA_OVERRIDES wins over both: {section: area}, or {section: section} to keep one on its own.
AREA_OVERRIDES = {
    "Foreign Academy Awards": "Academy Award",
}

# Shelf labels in the store's data that are wrong for the shelf the rental is really on;
# "" drops the label, leaving the rental on its section's main shelf.
SHELF_FIXES = {
    "Adult DVD": "",  # "Adult Theme > Adult DVD" is the Adult Theme main shelf
}


def area_of(section, sections):
    if section in AREA_OVERRIDES:
        return AREA_OVERRIDES[section]
    if re.search(r"\bTV\b", section):
        return "TV"
    for p in sorted(sections, key=len, reverse=True):
        if p != section and section.startswith(p + " "):
            return p
    return section


# Website-only curated categories, which the store doesn't shelve this way.
# Boutique labels: DVD, Blu-ray and 4K copies whose store title names a boutique label in its edition notes,
# e.g. "Kuffs (Shout Select) (Blu-Ray)". Labels with fewer than MIN_LABEL_TITLES titles share
# "Other boutique labels".
BOUTIQUE = "Boutique Labels"
BOUTIQUE_FORMATS = {"4K UHD", "Blu-Ray 3D", "Blu-Ray", "DVD"}
MIN_LABEL_TITLES = 5
LABELS = [
    ("Criterion Collection", r"criterion|crit\b\.?(\s*set)?"),
    ("Arrow Video", r"arrow( video)?( (2k|4k) restoration| remaster)?"),
    ("Vinegar Syndrome", r"vinegar syn\w*\.?|vin syn"),
    ("Severin Films", r"severin"),
    ("Kino Lorber", r"kino"),
    ("Imprint", r"imprint"),
    ("Radiance Films", r"radiance"),
    ("Indicator", r"indicator"),
    ("Twilight Time", r"twilight time"),
    ("Shout! / Scream Factory", r"shout( select| factory)?|scream factory"),
    ("88 Films", r"88 films"),
    ("Umbrella Entertainment", r"umbrella"),
    ("Mondo Macabro", r"mondo( macabro| mac| m\.)?"),
    ("Deaf Crocodile", r"deaf croc\w*\.?"),
    ("AGFA", r"agfa"),
    ("Cauldron Films", r"cauldron"),
    ("Synapse Films", r"synapse( films)?"),
    ("Flicker Alley", r"flicker alley"),
    ("Fun City Editions", r"fun city( editions)?"),
    ("Yellow Veil", r"yellow veil"),
    ("Olive Signature", r"olive (signature|remaster|films)"),
    ("Bleeding Skull", r"bleeding skull"),
    ("Terror Vision", r"terror ?vision"),
    ("Visual Vengeance", r"visual vengeance"),
    ("Blue Underground", r"blue underground"),
    ("Code Red", r"code red"),
    ("Third Window", r"third window"),
    ("Film Movement", r"film movement"),
    ("Via Vision", r"via ?vision"),
    ("Raro Video", r"raro"),
]
LABEL_PATTERNS = [(n, re.compile(r"(?:^|[^a-z0-9])(?:" + p + r")(?:[^a-z0-9]|$)", re.I)) for n, p in LABELS]


def label_of(title):
    """The boutique label named in a store title's parenthesised notes, if any."""
    for note in re.findall(r"\(([^)]*)\)", title):
        if note.lower().startswith("aka"):  # "(Aka: Cauldron Of Blood)" is a title, not a label
            continue
        for name, pat in LABEL_PATTERNS:
            if pat.search(note):
                return name
    return None


def tmdb_token():
    """The TMDB v4 read token: $TMDB_TOKEN (CI), else TOKEN_FILE. Never written anywhere."""
    if token := os.environ.get("TMDB_TOKEN", "").strip():
        return token
    return TOKEN_FILE.read_text().strip() if TOKEN_FILE.exists() else None


def log(*a):
    print(*a, file=sys.stderr, flush=True)


def get(url, headers=None, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": "movie-madness-lite", **(headers or {})})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.load(r)


# ---------------------------------------------------------------- catalog

def fetch_catalog(force=False):
    """Download every shard into cache/shards, unless the API hasn't changed."""
    index = get(f"{API}/index.json")
    stamp = CACHE / "shards" / "index.json"
    unmatched = CACHE / "shards" / "unmatched.json"
    if not force and stamp.exists() and json.loads(stamp.read_text()).get("updated") == index["updated"]:
        log(f"catalog unchanged since {index['updated']}")
        if not unmatched.exists():
            unmatched.write_text(json.dumps(get(f"{API}/unmatched.json"), ensure_ascii=False))
        return index
    log(f"downloading {2 * SHARDS} catalog shards (updated {index['updated']})")

    def one(job):
        kind, n = job
        path = CACHE / "shards" / kind / f"{n:03d}.json"
        for attempt in range(4):
            try:
                data = get(f"{API}/tmdb/{kind}/{n:03d}.json")
                path.write_text(json.dumps(data, ensure_ascii=False))
                return
            except (urllib.error.URLError, TimeoutError) as e:
                if attempt == 3:
                    raise
                time.sleep(2 ** attempt)

    for kind in KINDS:
        (CACHE / "shards" / kind).mkdir(parents=True, exist_ok=True)
    with ThreadPoolExecutor(8) as pool:
        list(pool.map(one, [(k, n) for k in KINDS for n in range(SHARDS)]))
    unmatched.write_text(json.dumps(get(f"{API}/unmatched.json"), ensure_ascii=False))
    stamp.write_text(json.dumps(index))
    return index


def load_unmatched():
    """Rentals with no TMDB id, grouped into titles: {("unmatched", lowest rental id): [rental, ...]}.

    Copies of one title (its DVD and VHS, say) share a cleaned store title and year.
    """
    path = CACHE / "shards" / "unmatched.json"
    if not path.exists():
        log("no unmatched.json cached; run without --no-fetch to get it")
        return {}
    groups = {}
    for r in json.loads(path.read_text()):
        key = (re.sub(r"\W+", " ", clean_store_title(r["title"]).lower()).strip(), r.get("year"))
        groups.setdefault(key, []).append(r)
    return {("unmatched", min(r["id"] for r in rs)): rs for rs in groups.values()}


def load_catalog():
    """{(kind, tmdb_id): [rental, ...]}"""
    works = {}
    for kind in KINDS:
        for n in range(SHARDS):
            path = CACHE / "shards" / kind / f"{n:03d}.json"
            for tid, rentals in json.loads(path.read_text()).items():
                works[(kind, int(tid))] = rentals
    return works


def fetch_store_terms(refresh):
    """The store's super-sections ("genre" terms) and shelves ("location" terms), cached."""
    path = CACHE / "store-terms.json"
    if path.exists() and not refresh:
        return json.loads(path.read_text())
    terms = {}
    try:
        for tax in ("genre", "location"):
            names, page = [], 1
            while True:
                batch = get(f"{STORE}/{tax}?per_page=100&page={page}&_fields=name")
                names += [html.unescape(t["name"]).rstrip("* ").strip() for t in batch]
                if len(batch) < 100:
                    break
                page += 1
            terms[tax] = sorted(set(names))
    except (urllib.error.URLError, TimeoutError, ValueError) as e:
        for fallback in (path, SEED / "store-terms.json"):
            if fallback.exists():
                log(f"couldn't refresh store terms ({e}); using {fallback.relative_to(ROOT)}")
                return json.loads(fallback.read_text())
        raise
    path.write_text(json.dumps(terms, ensure_ascii=False))
    log(f"store terms: {len(terms['genre'])} super-sections, {len(terms['location'])} shelves")
    return terms


def shelf_tree(works, terms):
    """Map each catalog shelf string to (super-section, shelf, name shelf).

    The catalog's shelf strings mix levels: "Foreign > Japanese > KUROSAWA, AKIRA" starts at the
    super-section, but "Hollywood Directors > SPIELBERG, STEVEN" or "Japanese > SHIMIZU, TAKASHI"
    start at the shelf. Those get the super-section the same shelf sits under elsewhere.
    """
    genres, locations = set(terms["genre"]), set(terms["location"])
    shown = {g: GENRE_NAMES.get(g, g) for g in genres}
    by_shown = {}
    for g, n in shown.items():
        by_shown.setdefault(n, g)
        by_shown.setdefault(g, g)
    counts = Counter(r["shelf"] for rs in works.values() for r in rs if r.get("shelf"))

    def section_named(loc):
        """The super-section a shelf is the main shelf of: "Classic Serials" -> "Classic Serial"."""
        if loc in by_shown:
            return by_shown[loc]
        for n, g in by_shown.items():
            if len(n) > 4 and (loc.startswith(n) or n.startswith(loc)) and abs(len(loc) - len(n)) <= 3:
                return g
        return None

    # The super-section each shelf mostly sits under, weighted by rentals. The store's data
    # files the odd rental under the wrong one ("War > Westerns"); the shelf is what's physical.
    parent = {}
    for p, n in counts.items():
        parts = p.split(" > ")
        if parts[0] in genres and len(parts) > 1:
            parent.setdefault(parts[1], Counter())[parts[0]] += n

    def genre_of(loc, given=None):
        if section_named(loc):
            return section_named(loc)
        if loc in parent:
            return parent[loc].most_common(1)[0][0]
        if given:
            return given
        for g in sorted(genres, key=lambda g: -len(shown[g])):  # "Comedy Series" -> "Comedy"
            if loc.startswith(shown[g]) or shown[g].startswith(loc):
                return g
        return None

    tree = {}
    for p in counts:
        parts = [x.strip() for x in p.split(" > ") if x.strip()]
        parts = [x for x in (SHELF_FIXES.get(x, x) for x in parts) if x]
        given, rest = (parts[0], parts[1:]) if parts[0] in genres else (None, parts)
        loc = name = ""
        if rest and (rest[0] in locations or len(rest) > 1 or not given):
            loc, name = rest[0], " > ".join(rest[1:])
        elif rest:
            name = rest[0]
        genre = genre_of(loc, given) if loc else given
        if not genre:
            genre, loc = loc, ""
        if loc and section_named(loc) == genre:  # the section's main shelf
            loc = ""
        tree[p] = (shown.get(genre, genre), loc, name)
    return tree


# ---------------------------------------------------------------- TMDB

class RateLimiter:
    """Spaces requests evenly at `rate` per second across threads; a 429 pauses everyone."""

    def __init__(self, rate):
        self.interval = 1 / rate
        self.next = time.monotonic()
        self.lock = threading.Lock()

    def wait(self):
        with self.lock:
            now = time.monotonic()
            self.next = max(self.next, now) + self.interval
            delay = self.next - self.interval - now
        if delay > 0:
            time.sleep(delay)

    def pause(self, seconds):
        with self.lock:
            self.next = max(self.next, time.monotonic() + seconds)


# TMDB's original_language codes whose posters are tagged with another code
POSTER_LANG = {"cn": "zh"}


# Poster scores count every poster as having VOTE_PRIOR extra votes at TMDB's neutral score, so a
# handful of perfect votes doesn't outrank a well-liked poster (4 votes at 10 vs 36 at 7.6).
VOTE_PRIOR, VOTE_NEUTRAL = 5, 3.33


def best_poster(posters, lang, default=None):
    """The highest-scoring poster in a language; ties go to TMDB's default, then to TMDB's order."""
    def score(p):
        n = p.get("vote_count") or 0
        return ((p.get("vote_average") or 0) * n + VOTE_NEUTRAL * VOTE_PRIOR) / (n + VOTE_PRIOR), p["file_path"] == default
    ps = [p for p in posters if p.get("iso_639_1") == lang]
    return max(ps, key=score)["file_path"] if ps else None


def fetch_adult_ids(refresh):
    """TMDB ids it flags as adult (pornographic), from its daily export files: no API calls.

    A new day's files appear around 8am UTC, so fall back a few days; then to the cached copy."""
    path = CACHE / "adult-ids.json"
    cached = json.loads(path.read_text()) if path.exists() else None
    today = datetime.now(timezone.utc).date()
    if refresh and (not cached or cached["date"] != today.isoformat()):
        for back in range(4):
            day = today - timedelta(days=back)
            try:
                ids = {}
                for kind, name in (("movie", "adult_movie_ids"), ("tv", "adult_tv_series_ids")):
                    url = f"https://files.tmdb.org/p/exports/{name}_{day:%m_%d_%Y}.json.gz"
                    req = urllib.request.Request(url, headers={"User-Agent": "movie-madness-lite"})
                    with urllib.request.urlopen(req, timeout=60) as r:
                        ids[kind] = sorted({json.loads(line)["id"] for line in gzip.decompress(r.read()).splitlines() if line.strip()})
                cached = {"date": today.isoformat(), "export": day.isoformat(), **ids}
                CACHE.mkdir(exist_ok=True)
                path.write_text(json.dumps(cached))
                break
            except (urllib.error.URLError, TimeoutError, ConnectionError, OSError, ValueError) as e:
                log(f"TMDB adult export for {day}: {e}")
    if not cached:
        log("no TMDB adult list; adult posters won't be blurred")
        return set()
    log(f"TMDB adult list from {cached['export']}: {len(cached['movie'])} movies, {len(cached['tv'])} TV series")
    return {("movie", i) for i in cached["movie"]} | {("tv", i) for i in cached["tv"]}


# TMDB keywords for erotic films, whose posters are blurred like adult-flagged ones. Not "erotic
# thriller" or "sexploitation": those take in studio R-rated films (Dressed to Kill, Unfaithful).
EROTIC_KEYWORDS = {
    155477: "softcore", 256466: "erotic", 325693: "erotica", 238059: "gay erotica",
    355313: "lesbian erotica", 219371: "vintage erotica", 350793: "greek erotica", 334900: "swedish erotica",
    302868: "erotic comedy", 298666: "erotic romance", 155691: "erotic vignettes", 337325: "erotic anthology",
    381597: "pinku eiga", 374679: "gay pinku",
}
EROTIC_DAYS = 7  # how long the keyword lists are kept before asking TMDB again (~215 requests)
# Hand corrections to the blur list: True blurs a title the keywords miss, False un-blurs a wrong tag.
BLUR_FIXES = {
    # ("movie", 12345): False,
}


def fetch_erotic_ids():
    """TMDB movie ids tagged with an EROTIC_KEYWORDS keyword, via discover (20 per request)."""
    path = CACHE / "erotic-ids.json"
    cached = json.loads(path.read_text()) if path.exists() else None
    fresh = cached and cached.get("keywords") == sorted(EROTIC_KEYWORDS) and \
        datetime.fromisoformat(cached["fetched"]) > datetime.now(timezone.utc) - timedelta(days=EROTIC_DAYS)
    token = tmdb_token()
    if not fresh and token:
        auth = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        limiter = RateLimiter(RATE)
        ids, requests = set(), 0
        try:
            for kid in EROTIC_KEYWORDS:
                page = pages = 1
                while page <= min(pages, 500):  # discover stops at page 500
                    limiter.wait()
                    d = get(f"{TMDB}/discover/movie?with_keywords={kid}&include_adult=false&page={page}", auth)
                    requests += 1
                    ids.update(m["id"] for m in d["results"])
                    pages = d["total_pages"]
                    page += 1
            cached = {"fetched": datetime.now(timezone.utc).isoformat(), "keywords": sorted(EROTIC_KEYWORDS), "movie": sorted(ids)}
            CACHE.mkdir(exist_ok=True)
            path.write_text(json.dumps(cached))
            log(f"TMDB erotic keywords: {len(ids)} films ({requests} requests)")
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError, ValueError) as e:
            log(f"TMDB erotic keyword lists: {e}; " + ("using the cached lists" if cached else "skipping"))
    if not cached:
        return set()
    return {("movie", i) for i in cached["movie"]}


def trim(kind, d):
    """Keep only the TMDB fields the site uses."""
    if kind == "movie":
        title, orig, date, length = d.get("title"), d.get("original_title"), d.get("release_date"), d.get("runtime")
    else:
        title, orig, date = d.get("name"), d.get("original_name"), d.get("first_air_date")
        length = d.get("number_of_seasons")
    posters = (d.get("images") or {}).get("posters") or []
    lang = d.get("original_language")
    poster = best_poster(posters, "en", d.get("poster_path")) or d.get("poster_path")
    poster_orig = best_poster(posters, POSTER_LANG.get(lang, lang)) if lang and lang != "en" else None
    return {
        "title": title or orig or "",
        "orig": orig if orig and orig != title else None,
        "year": int(date[:4]) if date and date[:4].isdigit() else None,
        # the best-voted English poster (TMDB's own pick when it has none), and for a
        # foreign-language title its best-voted original-language poster when that differs
        "poster": poster,
        "poster_orig": poster_orig if poster_orig != poster else None,
        "overview": (d.get("overview") or "").strip() or None,
        "length": length or None,
        "genres": [g["name"] for g in d.get("genres", [])],
        "lang": d.get("original_language"),
        "pop": round(d.get("popularity") or 0, 2),
    }


def enrich(works):
    """Fetch TMDB details for ids not in cache/tmdb.jsonl (or stale); return {(kind,id): info|None}."""
    cache_file = CACHE / "tmdb.jsonl"
    cache = {}
    if cache_file.exists():
        with cache_file.open() as f:
            for line in f:
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:  # a line cut short by an interrupted run
                    continue
                cache[(e["kind"], e["id"])] = e

    now = datetime.now(timezone.utc)
    expired = (now - timedelta(days=CACHE_DAYS)).isoformat()
    settled = (now - timedelta(days=REFRESH_MIN_DAYS)).isoformat()
    cached = [k for k in works if k in cache]
    new = [k for k in works if k not in cache]
    # entries cached before posters were chosen by vote (no "poster_orig") are all refreshed at once
    must = [k for k in cached if cache[k]["fetched"] < expired
            or (cache[k]["info"] is not None and "poster_orig" not in cache[k]["info"])]
    share = -(-len(cached) // REFRESH_CYCLE_DAYS)  # ceil
    must_set = set(must)
    due = sorted((k for k in cached if k not in must_set and expired <= cache[k]["fetched"] < settled),
                 key=lambda k: cache[k]["fetched"])[:share]
    todo = new + must + due
    if must or due:
        log(f"TMDB refresh: {len(due)} oldest entries" + (f", plus {len(must)} past {CACHE_DAYS} days or from an older build" if must else ""))
    token = tmdb_token()
    if todo:
        if not token:
            log(f"no TMDB token (TMDB_TOKEN or {TOKEN_FILE}); skipping {len(todo)} lookups")
            todo = []
        else:
            eta = len(todo) / RATE
            log(f"TMDB: {len(todo)} lookups to do (~{eta / 60:.0f} min at {RATE}/s), {len(cache)} cached")

    if todo:
        auth = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        limiter = RateLimiter(RATE)
        write_lock = threading.Lock()
        done = [0]
        out = cache_file.open("a")

        def fetch_one(kind, tid):
            # The poster lists come in the same request, for English and, when the cache already
            # knows it, the original language. A new foreign title needs a second request for them.
            known = ((cache.get((kind, tid)) or {}).get("info") or {}).get("lang")
            langs = ["en"] + ([POSTER_LANG.get(known, known)] if known and known != "en" else [])
            d = get(f"{TMDB}/{kind}/{tid}?language=en-US&append_to_response=images&include_image_language={','.join(langs)}", auth)
            lang = POSTER_LANG.get(d.get("original_language"), d.get("original_language"))
            if lang and lang not in langs and lang != "xx":
                limiter.wait()
                d.setdefault("images", {}).setdefault("posters", []).extend(get(f"{TMDB}/{kind}/{tid}/images?include_image_language={lang}", auth).get("posters") or [])
            return trim(kind, d)

        def one(key):
            kind, tid = key
            for attempt in range(6):
                limiter.wait()
                try:
                    info = fetch_one(kind, tid)
                    break
                except urllib.error.HTTPError as e:
                    if e.code == 404:  # id removed from TMDB
                        info = None
                        break
                    if e.code == 429 or e.code >= 500:
                        wait = float(e.headers.get("Retry-After") or 2 ** attempt)
                        log(f"TMDB {e.code} on {kind}/{tid}; backing off {wait:.0f}s")
                        limiter.pause(wait)
                        continue
                    if e.code == 401:
                        raise SystemExit("TMDB rejected the token (401)")
                    log(f"TMDB {e.code} on {kind}/{tid}; skipping")
                    return
                except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
                    limiter.pause(2 ** attempt)
            else:
                log(f"giving up on {kind}/{tid} for this run")
                return
            entry = {"kind": kind, "id": tid, "fetched": datetime.now(timezone.utc).isoformat(), "info": info}
            with write_lock:
                cache[key] = entry
                out.write(json.dumps(entry, ensure_ascii=False) + "\n")
                done[0] += 1
                if done[0] % 500 == 0:
                    out.flush()
                    log(f"  {done[0]}/{len(todo)}")

        try:
            with ThreadPoolExecutor(WORKERS) as pool:
                list(pool.map(one, todo))
        finally:
            out.close()
        compact(cache_file, cache)

    return {k: (cache[k]["info"] if k in cache else None) for k in works}


def compact(cache_file, cache):
    """Rewrite the append-only cache with one line per id."""
    tmp = cache_file.with_suffix(".tmp")
    with tmp.open("w") as f:
        for e in cache.values():
            f.write(json.dumps(e, ensure_ascii=False) + "\n")
    tmp.replace(cache_file)


# ---------------------------------------------------------------- output

FORMAT_SUFFIX = re.compile(r"\s*\((?:4K UHD|Blu-Ray 3D|Blu-Ray|DVD-R|DVD|VHS|Book)\)\s*", re.I)


def store_title(t):
    """'Matrix, The (4K UHD)' -> 'Matrix, The'."""
    return re.sub(r"\s+", " ", FORMAT_SUFFIX.sub(" ", t)).strip()


def edition(t):
    """What's left in parentheses after the format, e.g. 'Shout Select', 'OOP'."""
    parts = re.findall(r"\(([^)]*)\)", t)
    parts = [p for p in parts if not re.fullmatch(r"4K UHD|Blu-Ray 3D|Blu-Ray|DVD-R|DVD|VHS|Book|\d{4}", p, re.I)]
    return ", ".join(parts) or None


def clean_store_title(t):
    """Store title without any parenthesised notes, articles moved to the front."""
    t = re.sub(r"\s*\([^)]*\)", "", store_title(t)).strip()
    m = re.match(r"^(.*), (The|A|An)$", t)
    return f"{m.group(2)} {m.group(1)}" if m else t


def build(works, tmdb, index, tree, blur=frozenset()):
    shelves, displays = {}, {}

    def idx(table, name):
        if name not in table:
            table[name] = len(table)
        return table[name]

    out_works, posters, posters_orig = [], [], {}
    explicit = []  # catalog indices of titles TMDB flags as adult or tags as erotic
    labelled = {}  # label -> [(work index, rental index)]
    details = {(k, n): {} for k in (*KINDS, "unmatched") for n in range(SHARDS)}
    for (kind, tid), rentals in works.items():
        info = tmdb.get((kind, tid))
        if kind == "unmatched":  # the store's own data stands in for TMDB's
            year = next((r["year"] for r in rentals if str(r.get("year") or "").isdigit()), None)
            lang = next((r["language"] for r in rentals if r.get("language")), None)
            info = {"year": int(year) if year else None, "lang": lang}
        rentals = sorted(rentals, key=lambda r: FORMATS.index(r.get("format", "")) if r.get("format", "") in FORMATS else 99)
        rs = []
        for r in rentals:
            # New arrivals often have no permanent shelf yet; their display is where they are.
            if r.get("shelf"):
                shelf = idx(shelves, tree[r["shelf"]])
            else:
                shelf = -1 if r.get("display") else idx(shelves, ("Shelf unknown", "", ""))
            rs.append([
                FORMATS.index(r.get("format") or ""),
                shelf,
                idx(displays, r["display"]) if r.get("display") else -1,
            ])
        boutique = False
        for j, r in enumerate(rentals):
            if r.get("format") in BOUTIQUE_FORMATS and (label := label_of(r["title"])):
                labelled.setdefault(label, []).append((len(out_works), j))
                boutique = True
        title = (info or {}).get("title") or clean_store_title(rentals[0]["title"])
        # Poster paths are half the catalog's weight and can't be compressed (random ids), so they
        # live in posters.json, loaded alongside. Titles on the home page (displays, boutique
        # labels) keep theirs here.
        # Titles show their English poster; a foreign title's original-language one is the alternative.
        bare = lambda path: (path or "")[1:].removesuffix(".jpg")
        poster = bare((info or {}).get("poster") or (info or {}).get("poster_orig"))
        posters.append(poster)
        if info and info.get("poster_orig") and info.get("poster"):
            posters_orig[len(out_works)] = bare(info["poster_orig"])
        if (kind, tid) in blur:
            explicit.append(len(out_works))
        on_display = any(r[2] >= 0 for r in rs)
        row = [KIND_CODE[kind], tid, title, (info or {}).get("year") or 0,
               poster if on_display or boutique else "",
               round((info or {}).get("pop") or 0), rs]
        if info and info.get("orig"):
            row.append(info["orig"])
        out_works.append(row)
        details[(kind, tid % SHARDS)][str(tid)] = {
            "overview": (info or {}).get("overview"),
            "length": (info or {}).get("length"),
            "genres": (info or {}).get("genres") or [],
            "lang": (info or {}).get("lang"),
            "rentals": [{"id": r["id"], "slug": r["slug"], "title": store_title(r["title"]),
                         "edition": edition(r["title"])} for r in rentals],
        }
        if kind == "unmatched":
            rejected = sorted({r["rejected"] for r in rentals if r.get("rejected")})
            if rejected:
                details[(kind, tid % SHARDS)][str(tid)]["rejected"] = rejected

    names = {t[0] for t in shelves}
    collections, other = [], []
    for label, items in sorted(labelled.items(), key=lambda x: -len({w for w, _ in x[1]})):
        if len({w for w, _ in items}) >= MIN_LABEL_TITLES:
            collections.append({"group": BOUTIQUE, "name": label, "items": [n for wj in items for n in wj]})
        else:
            other += items
    if other:
        collections.append({"group": BOUTIQUE, "name": "Other boutique labels", "items": [n for wj in sorted(other) for n in wj]})
    catalog = {
        "updated": index["updated"],
        "built": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "formats": FORMATS,
        # [super-section, shelf ("" = the section's main shelf), name shelf ("" = none)]
        "shelves": [list(t) for t in shelves],
        # super-sections that sit inside a larger store area: {section: area}
        "areas": {n: a for n in sorted(names) if (a := area_of(n, names)) != n},
        "displays": list(displays),
        # website-only categories: {group, name, items: flat [work index, rental index, ...]}
        "collections": collections,
        # [kind(0 movie/1 tv/2 unmatched), tmdb id (unmatched: a store rental id), title, year,
        #  poster path (only for titles on a display or boutique label; the rest are in posters.json), popularity,
        #  [[format, shelf (-1: only on a display), display (-1: none)]], original title?]
        "works": out_works,
        # works TMDB flags as adult or tags erotic; the site treats these and everything on the store's
        # "Adult Theme" shelves as adult (posters blurred by default, or hidden)
        "explicit": explicit,
    }
    (SITE / "data").mkdir(parents=True, exist_ok=True)
    (SITE / "data" / "catalog.json").write_text(json.dumps(catalog, ensure_ascii=False, separators=(",", ":")))
    # one path per catalog title, in the same order; "built" ties the two files together
    (SITE / "data" / "posters.json").write_text(json.dumps({"built": catalog["built"], "posters": posters}, separators=(",", ":")))
    # original-language posters for foreign titles, {catalog index: path}; loaded only when a visitor picks them
    (SITE / "data" / "posters-orig.json").write_text(json.dumps({"built": catalog["built"], "posters": posters_orig}, separators=(",", ":")))
    for (kind, n), d in details.items():
        p = SITE / "data" / "details" / kind / f"{n:03d}.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(d, ensure_ascii=False, separators=(",", ":")))
    size = (SITE / "data" / "catalog.json").stat().st_size
    psize = (SITE / "data" / "posters.json").stat().st_size
    log(f"wrote {len(out_works)} titles, {len(shelves)} shelves; catalog.json {size / 1e6:.1f} MB, posters.json {psize / 1e6:.1f} MB, "
        f"{len(posters_orig)} titles with an original-language poster, {len(explicit)} blurred as adult or erotic")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--no-fetch", action="store_true", help="use the cached catalog shards")
    ap.add_argument("--no-tmdb", action="store_true", help="don't call TMDB; use whatever is cached")
    ap.add_argument("--force", action="store_true", help="re-download the catalog even if unchanged")
    args = ap.parse_args()

    if args.no_fetch:
        index = json.loads((CACHE / "shards" / "index.json").read_text())
    else:
        index = fetch_catalog(args.force)
    works = load_catalog()
    if not args.no_fetch and not (CACHE / "shards" / "unmatched.json").exists():
        (CACHE / "shards" / "unmatched.json").write_text(json.dumps(get(f"{API}/unmatched.json"), ensure_ascii=False))
    unmatched = load_unmatched()
    if args.no_tmdb:
        global tmdb_token
        tmdb_token = lambda: None
    terms = fetch_store_terms(refresh=not args.no_fetch or not (CACHE / "store-terms.json").exists())
    tmdb = enrich(works)
    adult = fetch_adult_ids(refresh=not args.no_fetch or not (CACHE / "adult-ids.json").exists())
    blur = adult | fetch_erotic_ids()
    blur = {k for k in blur if BLUR_FIXES.get(k, True)} | {k for k, v in BLUR_FIXES.items() if v}
    everything = {**works, **unmatched}
    build(everything, tmdb, index, shelf_tree(everything, terms), blur)


if __name__ == "__main__":
    main()
