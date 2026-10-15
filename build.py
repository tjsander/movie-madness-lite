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


def trim(kind, d):
    """Keep only the TMDB fields the site uses."""
    if kind == "movie":
        title, orig, date, length = d.get("title"), d.get("original_title"), d.get("release_date"), d.get("runtime")
    else:
        title, orig, date = d.get("name"), d.get("original_name"), d.get("first_air_date")
        length = d.get("number_of_seasons")
    return {
        "title": title or orig or "",
        "orig": orig if orig and orig != title else None,
        "year": int(date[:4]) if date and date[:4].isdigit() else None,
        "poster": d.get("poster_path"),
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
    must = [k for k in cached if cache[k]["fetched"] < expired]
    share = -(-len(cached) // REFRESH_CYCLE_DAYS)  # ceil
    due = sorted((k for k in cached if expired <= cache[k]["fetched"] < settled), key=lambda k: cache[k]["fetched"])[:share]
    todo = new + must + due
    if must or due:
        log(f"TMDB refresh: {len(due)} oldest entries" + (f", plus {len(must)} past {CACHE_DAYS} days" if must else ""))
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

        def one(key):
            kind, tid = key
            for attempt in range(6):
                limiter.wait()
                try:
                    info = trim(kind, get(f"{TMDB}/{kind}/{tid}?language=en-US", auth))
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


def build(works, tmdb, index, tree):
    shelves, displays = {}, {}

    def idx(table, name):
        if name not in table:
            table[name] = len(table)
        return table[name]

    out_works, posters = [], []
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
        poster = ((info or {}).get("poster") or "")[1:].removesuffix(".jpg")
        posters.append(poster)
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
    }
    (SITE / "data").mkdir(parents=True, exist_ok=True)
    (SITE / "data" / "catalog.json").write_text(json.dumps(catalog, ensure_ascii=False, separators=(",", ":")))
    # one path per catalog title, in the same order; "built" ties the two files together
    (SITE / "data" / "posters.json").write_text(json.dumps({"built": catalog["built"], "posters": posters}, separators=(",", ":")))
    for (kind, n), d in details.items():
        p = SITE / "data" / "details" / kind / f"{n:03d}.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(d, ensure_ascii=False, separators=(",", ":")))
    size = (SITE / "data" / "catalog.json").stat().st_size
    psize = (SITE / "data" / "posters.json").stat().st_size
    log(f"wrote {len(out_works)} titles, {len(shelves)} shelves; catalog.json {size / 1e6:.1f} MB, posters.json {psize / 1e6:.1f} MB")


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
    everything = {**works, **unmatched}
    build(everything, tmdb, index, shelf_tree(everything, terms))


if __name__ == "__main__":
    main()
