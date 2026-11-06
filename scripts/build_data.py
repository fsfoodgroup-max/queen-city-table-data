"""Queen City Table nightly data build.

1. Pulls Charlotte-area food places from Overture Maps (only when a new monthly release is out).
2. Keeps local, open, full-service restaurants and drops out-of-town chains.
3. Checks restaurant websites for delivery, online ordering and reservation links.
4. Merges the hand-curated staff picks and writes data/restaurants.json.

Run:  python scripts/build_data.py [--skip-web] [--force-overture] [--web-budget=N]
"""
import json, math, os, re, sys, time, hashlib, datetime, unicodedata
import urllib.request, urllib.robotparser, urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
P = lambda *a: os.path.join(ROOT, *a)
BUCKET = "https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com"
BBOX = dict(xmin=-81.10, xmax=-80.55, ymin=34.95, ymax=35.55)  # Charlotte metro incl. Lake Norman and Fort Mill
UA = "QueenCityTableBot/1.0 (+https://queencitytable.com)"
TODAY = datetime.date.today().isoformat()

def log(*a): print(*a, flush=True)

def load(path, default):
    try:
        with open(path, encoding="utf-8") as f: return json.load(f)
    except FileNotFoundError: return default

def save(path, obj, compact=False):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=None if compact else 1, separators=(",", ":") if compact else None)

def norm(s):
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().lower()
    s = re.sub(r"\s[-|@:]\s.*$", "", s)                     # "Name - Location" suffixes
    s = re.sub(r"[\'\u2019.]", "", s)                       # Jet's / Jets, Bb.q / Bbq
    s = re.sub(r"[^a-z0-9 ]+", " ", s.replace("&", " and "))
    s = re.sub(r"\b(the|restaurant|llc|inc)\b", " ", s)
    return re.sub(r"\s+", " ", s).strip()

# ---------------------------------------------------------------- Overture
def latest_release():
    xml = urllib.request.urlopen(f"{BUCKET}/?list-type=2&prefix=release/&delimiter=/", timeout=30).read().decode()
    rel = sorted(re.findall(r"<Prefix>release/([^/<]+)/</Prefix>", xml))
    return rel[-1]

def pull_overture(release):
    import duckdb
    con = duckdb.connect()
    con.sql("INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial; SET threads=8;")
    con.sql("CREATE SECRET ov (TYPE s3, PROVIDER config, KEY_ID '', SECRET '', REGION 'us-west-2')")
    src = f"read_parquet('s3://overturemaps-us-west-2/release/{release}/theme=places/type=place/*', hive_partitioning=1)"
    log("Pulling Charlotte places from Overture", release)
    con.sql(f"""COPY (SELECT id, names.primary AS name, basic_category, taxonomy.primary AS cat, taxonomy.hierarchy AS hier,
        brand.names.primary AS brand, websites, socials, confidence, operating_status,
        addresses[1].freeform AS street, addresses[1].locality AS city, addresses[1].region AS region,
        ST_X(geometry) AS lon, ST_Y(geometry) AS lat
      FROM {src}
      WHERE bbox.xmin BETWEEN {BBOX['xmin']} AND {BBOX['xmax']} AND bbox.ymin BETWEEN {BBOX['ymin']} AND {BBOX['ymax']}
        AND list_contains(taxonomy.hierarchy, 'food_and_drink'))
      TO '{P('cache','places.parquet')}' (FORMAT parquet)""")
    log("Counting each name's locations outside the Carolinas (chain check)")
    con.sql(f"CREATE TABLE k AS SELECT DISTINCT lower(coalesce(brand, name)) k FROM '{P('cache','places.parquet')}'")
    con.sql(f"""COPY (SELECT lower(coalesce(brand.names.primary, names.primary)) k, count(*) us_total,
        count(*) FILTER (WHERE addresses[1].region NOT IN ('NC','SC')) outside_carolinas
      FROM {src}
      WHERE addresses[1].country='US' AND list_contains(taxonomy.hierarchy, 'food_and_drink')
        AND lower(coalesce(brand.names.primary, names.primary)) IN (SELECT k FROM k)
      GROUP BY 1) TO '{P('cache','name_counts.parquet')}' (FORMAT parquet)""")

def read_places():
    import duckdb
    con = duckdb.connect()
    rows = con.sql(f"""SELECT p.id, p.name, p.basic_category, p.cat, p.hier, p.brand, p.websites, p.confidence,
          p.operating_status, p.city, p.region, p.lon, p.lat, coalesce(n.outside_carolinas, 0) AS outside
        FROM '{P('cache','places.parquet')}' p
        LEFT JOIN '{P('cache','name_counts.parquet')}' n ON n.k = lower(coalesce(p.brand, p.name))""").fetchall()
    cols = ["id","name","basic","cat","hier","brand","websites","confidence","status","city","region","lon","lat","outside"]
    return [dict(zip(cols, r)) for r in rows]

# ---------------------------------------------------------------- classification
EXCLUDE_WORDS = ("bakery","ice_cream","dessert","yogurt","donut","doughnut","cupcake","cookie","candy","chocolate","gelato",
  "shaved_ice","coffee","tea_room","bubble_tea","juice","smoothie","acai","caterer","catering","food_truck","popcorn","pretzel",
  "patisserie","creperie_dessert","kiosk","vending","grocery","market","convenience","liquor","bagel")
CUISINE_RULES = [  # first match wins; checked against category and its parents
  ("bbq", ("barbecue","bbq","smokehouse")),
  ("tasting", ("fine_dining",)),
  ("steak", ("steakhouse",)),
  ("japanese", ("japanese","sushi","ramen","izakaya","poke")),
  ("mexican", ("mexican","tex_mex","taco")),
  ("latin", ("latin_american","cuban","peruvian","colombian","venezuelan","salvadoran","puerto_rican","dominican","brazilian","caribbean","jamaican","honduran","argentin")),
  ("italian", ("italian","pizza")),
  ("med", ("mediterranean","greek","middle_eastern","lebanese","turkish","israeli","persian","falafel","halal")),
  ("asian", ("chinese","vietnamese","thai","indian","korean","asian","filipino","nepal","pakistani","dim_sum","malaysian","indonesian","burmese","pho","noodle")),
  ("seafood", ("seafood","fish","oyster","crab","cajun_seafood")),
  ("southern", ("southern","soul_food","cajun","creole","comfort_food")),
  ("breakfast", ("breakfast","brunch","pancake","waffle")),
  ("veg", ("vegan","vegetarian")),
  ("casualfare", ("burger","sandwich","deli","hot_dog","chicken_wing","wings","chicken_restaurant","sub_shop","cheesesteak")),
  ("global", ("french","spanish","tapas","german","european","african","ethiopian","fusion","irish","british","polish","russian","ukrainian","portuguese","belgian")),
  ("american", ("american","bar_and_grill","gastropub","pub","diner","grill","tavern","brewpub","sports_bar")),
]
OCCASIONS = {
  "tasting": ["date","celebrate","client"], "steak": ["date","celebrate","client"], "seafood": ["date","family","group"],
  "japanese": ["date","group","quick"], "italian": ["family","date","group"], "mexican": ["family","group","quick"],
  "latin": ["family","group","quick"], "bbq": ["family","group","quick"], "med": ["quick","family","group"],
  "asian": ["quick","family","group"], "southern": ["family","group"], "breakfast": ["quick","family"],
  "veg": ["quick","date"], "casualfare": ["quick","family"], "global": ["date","group"], "american": ["group","family","drinks"],
}
CUI_LABEL = {"bbq":"BBQ","tasting":"Tasting menu","steak":"steakhouse","japanese":"Japanese","mexican":"Mexican","latin":"Latin",
  "italian":"Italian","med":"Mediterranean","asian":"Asian","seafood":"Seafood","southern":"Southern","breakfast":"Breakfast and brunch",
  "veg":"Vegetarian","casualfare":"Casual","global":"Global","american":"American"}

NAME_EXCLUDE = re.compile(r"water ice|ice cream|gelato|creamery|bakery|bakeshop|donut|doughnut|cupcake|cookie|coffee|espresso|tea house|boba|juice|smoothie|catering|caterer|food truck|vending|market|grocery|supermarket|frozen yogurt|sno ?ball|shaved ice", re.I)
NAME_CUISINE = [
  ("japanese", r"sushi|ramen|hibachi|izakaya|poke|teriyaki|japanese"),
  ("asian", r"\bpho\b|banh mi|dumpling|noodle|\bthai\b|curry|tandoor|masala|biryani|\bwok\b|szechuan|sichuan|china|chinese|korean|bbq korean|vietnam|indian|nepal|himalayan|bangkok|saigon|hunan|dim sum"),
  ("mexican", r"taco|taqueria|cantina|burrito|mexican|tex mex|tamale|mariscos"),
  ("latin", r"empanada|arepa|pupusa|cuban|peruvian|jerk|caribbean|colombian|venezuelan|salvador|brazil|latin"),
  ("italian", r"pizza|pizzeria|trattoria|osteria|pasta|italian|ristorante"),
  ("bbq", r"\bbbq\b|barbecue|bar b q|smokehouse|smoke house"),
  ("med", r"gyro|kabob|kebab|shawarma|falafel|greek|mediterranean|lebanese|persian|halal"),
  ("seafood", r"crab|oyster|seafood|fish|shrimp|lobster|boil"),
  ("steak", r"steak|chophouse|chop house"),
  ("breakfast", r"brunch|biscuit|breakfast|pancake|waffle"),
  ("southern", r"soul food|southern|fried chicken|chicken and waffles|cajun|creole"),
  ("veg", r"vegan|plant based"),
  ("casualfare", r"burger|wings|\bsubs?\b|deli|sandwich|hot dog|philly|cheesesteak|chicken"),
]

def clean_name(n):
    n = (n or "").strip()
    n = re.split(r",\s", n)[0]
    n = re.sub(r"\s[-|@]\s.*$", "", n)
    return n.strip()

def is_restaurant(p):
    if p["basic"] not in ("restaurant", "casual_eatery"): return False
    if NAME_EXCLUDE.search(p["name"] or ""): return False
    path = " ".join([p["cat"] or ""] + list(p["hier"] or []))
    return not any(w in path for w in EXCLUDE_WORDS)

def cuisine(p):
    path = " ".join([p["cat"] or ""] + list(p["hier"] or [])[::-1])
    found = next((code for code, words in CUISINE_RULES if any(w in path for w in words)), "american")
    if found in ("american", "casualfare"):
        name = norm(p["name"])
        hit = next((code for code, rx in NAME_CUISINE if re.search(rx, name)), None)
        if hit and not (found == "casualfare" and hit == "casualfare"): return hit
    return found

def blurb(cui, hood):
    word = {"steak":"Steakhouse","tasting":"Fine dining","casualfare":"Casual spot","breakfast":"Breakfast and brunch spot",
            "veg":"Vegetarian-friendly restaurant","global":"Restaurant","american":"American restaurant"}.get(cui, f"{CUI_LABEL[cui]} restaurant")
    return f"{word} in {hood}."

# ---------------------------------------------------------------- neighborhoods
def km(a, b, c, d):
    r = math.pi / 180
    x = (d - b) * r * math.cos((a + c) * r / 2); y = (c - a) * r
    return 6371 * math.hypot(x, y)

def place_of(p, nb):
    best = min(nb["centers"], key=lambda c: km(p["lat"], p["lon"], c[2], c[3]))
    if km(p["lat"], p["lon"], best[2], best[3]) <= nb["max_km"]:
        return [best[1], best[0]]
    town = (p["city"] or "").strip().title() or "Charlotte area"
    if town.lower() == "charlotte":   # inside the city but between centers: nearest center anyway
        return [best[1], best[0]]
    return ["beyond", f"{town}, {p['region']}" if p.get("region") in ("SC",) else town]

# ---------------------------------------------------------------- website checks
SIG = {
  "delivery_platform": re.compile(r"doordash\.com|ubereats\.com|grubhub\.com|postmates\.com|seamless\.com|caviar\.com", re.I),
  "ordering": re.compile(r"toasttab\.com|order\.toasttab|chownow\.com|olo\.com|order\.online|square\.site|clover\.com/online|menufy\.com|slicelife\.com|order online|order now", re.I),
  "delivery_word": re.compile(r"\bdelivery\b", re.I),
  "resy": re.compile(r"resy\.com", re.I),
  "opentable": re.compile(r"opentable\.com", re.I),
  "tock": re.compile(r"exploretock\.com", re.I),
}
_robots = {}
def allowed(url):
    parts = urllib.parse.urlsplit(url)
    base = f"{parts.scheme}://{parts.netloc}"
    if base not in _robots:
        rp = urllib.robotparser.RobotFileParser()
        try:
            req = urllib.request.Request(base + "/robots.txt", headers={"User-Agent": UA})
            rp.parse(urllib.request.urlopen(req, timeout=8).read().decode("utf-8", "ignore").splitlines())
        except Exception:
            rp.parse([])
        _robots[base] = rp
    return _robots[base].can_fetch(UA, url)

def check_site(url):
    try:
        if not url.startswith("http"): url = "https://" + url
        if not allowed(url): return {"ok": True, "blocked": True}
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "text/html"})
        with urllib.request.urlopen(req, timeout=12) as r:
            html = r.read(800_000).decode("utf-8", "ignore")
        plat = bool(SIG["delivery_platform"].search(html))
        order = bool(SIG["ordering"].search(html))
        book = next((k for k in ("resy", "opentable", "tock") if SIG[k].search(html)), None)
        return {"ok": True, "delivery": plat or (order and bool(SIG["delivery_word"].search(html))),
                "order": order or plat, "book": book}
    except urllib.error.HTTPError as e:
        return {"ok": e.code not in (404, 410), "code": e.code}
    except Exception as e:
        return {"ok": False, "err": type(e).__name__}

def run_web_checks(concepts, signals, budget, drop_after):
    due = sorted(concepts, key=lambda c: (c["key"] in signals, signals.get(c["key"], {}).get("checked", "")))
    due = [c for c in due if c.get("web")][:budget]
    log(f"Checking {len(due)} restaurant websites")
    done = 0
    with ThreadPoolExecutor(max_workers=16) as ex:
        futs = {ex.submit(check_site, c["web"]): c for c in due}
        for f in as_completed(futs):
            c = futs[f]; r = f.result(); s = signals.get(c["key"], {})
            s["checked"] = TODAY
            if r.get("ok"):
                s["fails"] = 0
                if not r.get("blocked") and "delivery" in r:
                    s.update(delivery=r["delivery"], order=r["order"], book=r["book"])
            else:
                s["fails"] = s.get("fails", 0) + 1
            signals[c["key"]] = s; done += 1
            if done % 200 == 0: log(f"  {done} checked")
    return signals

def add_signals(r, s):
    """Copy website findings onto a restaurant. 'online' means online ordering; 'order' stays the what-to-order text."""
    if s.get("delivery") is not None: r["delivery"] = bool(s["delivery"])
    if s.get("order") is not None: r["online"] = bool(s["order"])
    if s.get("book"): r["book"] = s["book"]

# ---------------------------------------------------------------- build
def main():
    args = set(sys.argv[1:])
    for d in ("cache", "data"): os.makedirs(P(d), exist_ok=True)
    lists = load(P("config","lists.json"), {})
    st = lists.get("settings", {})
    nb = load(P("config","neighborhoods.json"), {})
    keep = {norm(n) for n in lists.get("always_keep", [])}
    drop = {norm(n) for n in lists.get("always_exclude", [])}
    state = load(P("cache","state.json"), {})

    release = latest_release()
    if "--force-overture" in args or state.get("release") != release or not os.path.exists(P("cache","places.parquet")):
        pull_overture(release); state["release"] = release
    places = read_places()
    log(f"{len(places)} food and drink places in the Charlotte area")

    # Names of out-of-town chains, used to catch listings like "Jet's Pizza Huntersville"
    import duckdb
    thr = st.get("chain_threshold_outside_carolinas", 8)
    chain_keys = sorted({norm(k) for (k,) in duckdb.sql(f"SELECT k FROM '{P('cache','name_counts.parquet')}' WHERE outside_carolinas >= {thr}").fetchall()} - keep, key=len, reverse=True)
    chain_keys = [k for k in chain_keys if len(k) >= 5 and " " in k or len(k) >= 7]
    def chain_prefix(key):
        return any(key.startswith(k + " ") for k in chain_keys)

    concepts = {}
    for p in places:
        if not is_restaurant(p): continue
        if (p["status"] or "open") != "open": continue
        if (p["confidence"] or 0) < st.get("min_confidence", 0.8): continue
        p["websites"] = [w for w in (p["websites"] or []) if w and w.strip()]
        if st.get("require_website", True) and not p["websites"]: continue
        key = norm(p["brand"] or p["name"])
        if not key or key in drop or any(key.startswith(d + " ") for d in drop): continue
        if key not in keep and (p["outside"] >= thr or chain_prefix(key)): continue
        c = concepts.setdefault(key, {"key": key, "name": clean_name(p["name"]), "cui": cuisine(p), "places": [], "web": None, "conf": 0})
        if (p["confidence"] or 0) > c["conf"]:
            c["name"], c["conf"], c["web"] = clean_name(p["name"]), p["confidence"], p["websites"][0]
        pl = place_of(p, nb)
        if pl not in c["places"]: c["places"].append(pl)
    concepts = list(concepts.values())
    log(f"{len(concepts)} local restaurants after filters")

    signals = load(P("cache","web_signals.json"), {})
    if "--skip-web" not in args:
        budget = next((int(a.split("=")[1]) for a in args if a.startswith("--web-budget=")), st.get("website_checks_per_night", 400))
        if not signals: budget = max(budget, 5000)   # first run: check every website once
        signals = run_web_checks(concepts, signals, budget, st.get("drop_after_failed_checks", 3))
        save(P("cache","web_signals.json"), signals)
    live = {c["key"] for c in concepts}
    signals = {k: v for k, v in signals.items() if k in live}   # forget restaurants no longer listed
    save(P("cache","web_signals.json"), signals)
    dead = {k for k, s in signals.items() if s.get("fails", 0) >= st.get("drop_after_failed_checks", 3)}

    staff = load(P("data","staff_picks.json"), {})
    out_areas = nb["areas"]
    cuisines = dict(staff.get("cuisines", {}))
    cuisines.update({"american": "American & bar food", "breakfast": "Breakfast & brunch", "veg": "Vegetarian & vegan", "global": "French, Spanish & global"})
    cuisines["asian"] = "Chinese, Vietnamese, Thai & Indian"
    restaurants, seen = [], set()
    by_key = {c["key"]: c for c in concepts}
    for r in staff.get("restaurants", []):
        k = norm(r["name"]); seen.add(k)
        match = by_key.get(k) or next((c for c in concepts if c["key"].startswith(k[:12]) or k.startswith(c["key"][:12])), None)
        s = signals.get(match["key"], {}) if match else {}
        r = dict(r, staff=True)
        if match and match.get("web") and not r.get("web"): r["web"] = match["web"]
        add_signals(r, s)
        restaurants.append(r)
    for c in sorted(concepts, key=lambda c: c["name"].lower()):
        if c["key"] in seen or c["key"] in dead: continue
        if any(c["key"].startswith(k) or k.startswith(c["key"]) for k in seen if len(k) > 6): continue
        s = signals.get(c["key"], {})
        hid = hashlib.md5(c["key"].encode()).hexdigest()[:6]
        r = {"id": re.sub(r"[^a-z0-9]+", "-", c["key"])[:40].strip("-") + "-" + hid, "name": c["name"], "cui": [c["cui"]],
             "places": c["places"][:8], "price": 0, "occ": OCCASIONS.get(c["cui"], ["group"]),
             "blurb": blurb(c["cui"], c["places"][0][1]), "order": "", "honor": "", "web": c["web"]}
        add_signals(r, s)
        restaurants.append(r)

    data = {"updated": TODAY, "release": release, "areas": out_areas, "occasions": staff.get("occasions", {}),
            "cuisines": cuisines, "restaurants": restaurants,
            "sources": "Overture Maps Foundation (CDLA Permissive 2.0 and other open licenses), restaurant websites, Queen City Table staff picks"}
    old = load(P("data","restaurants.json"), {})
    changed = [x for x in restaurants] != old.get("restaurants")
    save(P("data","restaurants.json"), data, compact=True)
    save(P("cache","state.json"), state)
    n_del = sum(1 for r in restaurants if r.get("delivery"))
    log(f"Wrote {len(restaurants)} restaurants ({sum(1 for r in restaurants if r.get('staff'))} staff picks, {n_del} with delivery). Changed: {changed}")

if __name__ == "__main__":
    main()
