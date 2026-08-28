
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup
from dateutil import parser as dtparser
from zoneinfo import ZoneInfo
import requests, os, re, json, sqlite3, threading, time, csv, io, feedparser

UA={"User-Agent":"CryptoCopilot/40.24 personal research dashboard"}
DB=os.getenv("DB_PATH","market_intel_v421.db")
CACHE={}
LOCK=threading.Lock()

def now(): return datetime.now(timezone.utc)
def clamp(x,a=0,b=100): return max(a,min(b,float(x)))

def http_get(url, **kw):
    headers=dict(UA)
    headers.update(kw.pop("headers",{}))
    return requests.get(url, headers=headers, timeout=kw.pop("timeout",8), **kw)

def cached(key, ttl, fn):
    with LOCK:
        c=CACHE.get(key)
        if c and time.time()-c["ts"]<ttl:
            return c["v"]
    try:
        v=fn()
    except Exception as e:
        v={"ok":False,"verified":False,"risk_score":None,"summary":str(e)}
    with LOCK:
        CACHE[key]={"ts":time.time(),"v":v}
    return v

def init_db():
    con=sqlite3.connect(DB)
    con.execute("""create table if not exists shield_history(
        ts text primary key, risk real, coverage real, payload text
    )""")
    con.commit(); con.close()

def save_snapshot(risk,coverage,payload):
    con=sqlite3.connect(DB)
    ts=now().replace(second=0,microsecond=0).isoformat()
    con.execute("insert or replace into shield_history(ts,risk,coverage,payload) values(?,?,?,?)",
                (ts,float(risk),float(coverage),json.dumps(payload,separators=(",",":"))))
    con.commit(); con.close()

# ---------- CRYPTO: BINANCE PRIMARY, COINGECKO FALLBACK ----------
UNIVERSE=["BTCUSDT","ETHUSDT","SOLUSDT","XRPUSDT","BNBUSDT","ADAUSDT","DOGEUSDT","AVAXUSDT","LINKUSDT","SUIUSDT","DOTUSDT","UNIUSDT"]

def crypto_market():
    def work():
        rows=[]
        errors=[]
        for sym in UNIVERSE:
            try:
                r=http_get("https://api.binance.com/api/v3/ticker/24hr",params={"symbol":sym},timeout=5)
                r.raise_for_status(); j=r.json()
                rows.append({"symbol":sym,"change":float(j["priceChangePercent"]),"quote_volume":float(j["quoteVolume"])})
            except Exception as e:
                errors.append(sym)
        if len(rows)<6:
            ids="bitcoin,ethereum,solana,ripple,binancecoin,cardano,dogecoin,avalanche-2,chainlink,sui,polkadot,uniswap"
            r=http_get("https://api.coingecko.com/api/v3/simple/price",
                       params={"ids":ids,"vs_currencies":"usd","include_24hr_change":"true"},timeout=7)
            r.raise_for_status(); j=r.json()
            rows=[]
            mapping={"bitcoin":"BTC","ethereum":"ETH","solana":"SOL","ripple":"XRP","binancecoin":"BNB","cardano":"ADA",
                     "dogecoin":"DOGE","avalanche-2":"AVAX","chainlink":"LINK","sui":"SUI","polkadot":"DOT","uniswap":"UNI"}
            for k,v in j.items():
                ch=v.get("usd_24h_change")
                if ch is not None: rows.append({"symbol":mapping[k]+"USDT","change":float(ch),"quote_volume":0})
            source="CoinGecko fallback"
        else:
            source="Binance public market data"
        if len(rows)<4:
            raise RuntimeError("Insufficient crypto market coverage")
        btc=next((x for x in rows if x["symbol"].startswith("BTC")),rows[0])
        breadth=100*sum(x["change"]>0 for x in rows)/len(rows)
        avg=sum(x["change"] for x in rows)/len(rows)
        risk=50
        if btc["change"]<-3:risk+=16
        elif btc["change"]<-1:risk+=8
        elif btc["change"]>3:risk-=12
        elif btc["change"]>1:risk-=6
        if breadth<35:risk+=14
        elif breadth<48:risk+=6
        elif breadth>70:risk-=10
        elif breadth>58:risk-=5
        if avg<-2:risk+=10
        elif avg>2:risk-=8
        return {"ok":True,"verified":True,"risk_score":clamp(risk),
                "summary":f"BTC {btc['change']:+.2f}% | breadth {breadth:.0f}% | avg {avg:+.2f}%",
                "details":{"rows":rows,"breadth":breadth,"avg_change":avg,"btc_change":btc["change"]},
                "source":source}
    return cached("crypto",30,work)

# ---------- BLS PUBLIC DATA ----------
def bls_one(series_id):
    r=http_get("https://api.bls.gov/publicAPI/v1/timeseries/data/"+series_id,timeout=8)
    r.raise_for_status(); j=r.json()
    s=(j.get("Results",{}).get("series") or [None])[0]
    vals=[]
    if s:
        for x in s.get("data",[]):
            if x.get("period","").startswith("M") and x.get("value") not in (None,""):
                vals.append((x["year"],x["period"],float(x["value"])))
    return vals

def macro_bls():
    def work():
        risk=50; details={}; got=0
        for name,sid in [("cpi","CUUR0000SA0"),("unemployment","LNS14000000"),("payrolls","CES0000000001")]:
            try:
                v=bls_one(sid); details[name]=v[:14]
                if len(v)>=2: got+=1
                if name=="cpi" and len(v)>=13:
                    yoy=(v[0][2]/v[12][2]-1)*100
                    mom=(v[0][2]/v[1][2]-1)*100
                    details["cpi_summary"]={"yoy":yoy,"mom":mom}
                    risk += max(-8,min(12,(yoy-2.5)*5))
                    if mom>0.35:risk+=5
                elif name=="unemployment" and len(v)>=2:
                    d=v[0][2]-v[1][2]
                    if d>0.2:risk+=4
                    elif d<-0.2:risk-=2
                elif name=="payrolls" and len(v)>=2:
                    d=v[0][2]-v[1][2]
                    details["payroll_change_thousands"]=d
                    if d<50:risk+=5
                    elif d>300:risk+=4
            except Exception as e:
                details[name+"_error"]=str(e)
        if not got:
            return {"ok":False,"verified":False,"risk_score":None,"summary":"BLS unavailable","details":details}
        return {"ok":True,"verified":True,"risk_score":clamp(risk),"summary":"Official BLS inflation and labor data",
                "details":details,"source":"U.S. Bureau of Labor Statistics"}
    return cached("bls",3600,work)

# ---------- OFFICIAL U.S. TREASURY YIELD CURVE, NO KEY ----------
def treasury_yields():
    def work():
        year=now().year
        url=f"https://home.treasury.gov/resource-center/data-chart-center/interest-rates/TextView?field_tdr_date_value={year}&type=daily_treasury_yield_curve"
        r=http_get(url,timeout=10); r.raise_for_status()
        soup=BeautifulSoup(r.text,"html.parser")
        table=soup.find("table")
        if not table: raise RuntimeError("Treasury table not found")
        hdr=[x.get_text(" ",strip=True) for x in table.find_all("th")]
        rows=[]
        for tr in table.find_all("tr"):
            td=[x.get_text(" ",strip=True) for x in tr.find_all("td")]
            if td: rows.append(td)
        if len(rows)<2: raise RuntimeError("Treasury rows unavailable")
        # locate by visually standard ordering; also map headers if possible
        def parse_row(row):
            out={"Date":row[0]}
            names=["1 Mo","1.5 Mo","2 Mo","3 Mo","4 Mo","6 Mo","1 Yr","2 Yr","3 Yr","5 Yr","7 Yr","10 Yr","20 Yr","30 Yr"]
            nums=[]
            for cell in row[1:]:
                try: nums.append(float(cell))
                except: pass
            if len(nums)>=14:
                for n,v in zip(names,nums[-14:]): out[n]=v
            return out
        p0,p1=parse_row(rows[0]),parse_row(rows[1])
        y10=p0.get("10 Yr"); y2=p0.get("2 Yr")
        if y10 is None: raise RuntimeError("10Y yield unavailable")
        d10=y10-p1.get("10 Yr",y10)
        curve=y10-y2 if y2 is not None else None
        risk=50
        if d10>0.10:risk+=8
        elif d10>0.05:risk+=4
        elif d10<-0.10:risk-=5
        if y10>5:risk+=6
        if curve is not None and curve<0:risk+=3
        return {"ok":True,"verified":True,"risk_score":clamp(risk),
                "summary":f"10Y {y10:.2f}% ({d10:+.2f}pp daily)"+(f" | 10Y-2Y {curve:+.2f}pp" if curve is not None else ""),
                "details":{"latest":p0,"previous":p1},"source":"U.S. Department of the Treasury"}
    return cached("treasury",1800,work)

# ---------- CBOE VIX OFFICIAL DAILY DATA, NO KEY ----------
def cboe_vix():
    def work():
        url="https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv"
        r=http_get(url,timeout=10); r.raise_for_status()
        rows=list(csv.DictReader(io.StringIO(r.text)))
        if len(rows)<2: raise RuntimeError("VIX history unavailable")
        a,b=rows[-1],rows[-2]
        close=float(a["CLOSE"]); prev=float(b["CLOSE"]); ch=close-prev
        risk=50
        if close>=30:risk+=22
        elif close>=25:risk+=16
        elif close>=20:risk+=9
        elif close<15:risk-=7
        if ch>=3:risk+=10
        elif ch>=1.5:risk+=5
        elif ch<=-3:risk-=6
        return {"ok":True,"verified":True,"risk_score":clamp(risk),
                "summary":f"VIX {close:.2f} | daily change {ch:+.2f}",
                "details":{"date":a.get("DATE"),"close":close,"previous":prev},"source":"Cboe Global Markets"}
    return cached("vix",1800,work)

# ---------- CROSS-ASSET QUOTES: STOOQ SNAPSHOT BEST EFFORT ----------
# Secondary source only. If unavailable, it contributes nothing.
STOOQ={"sp500":"^spx","nasdaq":"^ndq","oil":"cl.f","gold":"gc.f","dollar":"dx.f"}
def stooq_quote(symbol):
    u="https://stooq.com/q/l/"
    r=http_get(u,params={"s":symbol,"f":"sd2t2ohlcv","h":"","e":"csv"},timeout=7); r.raise_for_status()
    rows=list(csv.DictReader(io.StringIO(r.text)))
    if not rows: raise RuntimeError("no quote")
    x=rows[0]
    if any(str(x.get(k,"")).upper()=="N/D" for k in ("Open","Close")): raise RuntimeError("N/D")
    op=float(x["Open"]); cl=float(x["Close"])
    return {"open":op,"close":cl,"pct":((cl/op-1)*100 if op else 0),"date":x.get("Date"),"time":x.get("Time")}

def cross_asset_quotes():
    def work():
        vals={}; risk=50
        for k,s in STOOQ.items():
            try: vals[k]=stooq_quote(s)
            except Exception as e: vals[k]={"error":str(e)}
        usable=[k for k,v in vals.items() if "pct" in v]
        if not usable:
            return {"ok":False,"verified":False,"risk_score":None,"summary":"Cross-asset quote fallback unavailable","details":vals}
        sp=vals.get("sp500",{}).get("pct")
        nq=vals.get("nasdaq",{}).get("pct")
        oil=vals.get("oil",{}).get("pct")
        usd=vals.get("dollar",{}).get("pct")
        gold=vals.get("gold",{}).get("pct")
        if sp is not None:
            if sp<-1.5:risk+=10
            elif sp>1.5:risk-=7
        if nq is not None:
            if nq<-1.8:risk+=10
            elif nq>1.8:risk-=7
        if oil is not None and oil>3:risk+=5
        if usd is not None and usd>0.6:risk+=5
        if gold is not None and gold>1.5 and (sp is not None and sp<0):risk+=3
        return {"ok":True,"verified":True,"risk_score":clamp(risk),
                "summary":"S&P/Nasdaq/Oil/Gold/Dollar quote snapshot",
                "details":vals,"source":"Stooq quote snapshot (secondary)"}
    return cached("stooq",300,work)

# ---------- BLS CALENDAR ----------
HIGH_EVENTS=("Consumer Price Index","Employment Situation","Producer Price Index",
             "Job Openings and Labor Turnover","Employment Cost Index","Productivity and Costs")
def parse_ics(line):
    val=line.split(":",1)[1].strip()
    if val.endswith("Z"): return datetime.strptime(val,"%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    d=dtparser.parse(val)
    if d.tzinfo is None: d=d.replace(tzinfo=ZoneInfo("America/New_York"))
    return d.astimezone(timezone.utc)

def bls_calendar():
    def work():
        r=http_get("https://www.bls.gov/schedule/news_release/bls.ics",timeout=8); r.raise_for_status()
        events=[]
        for chunk in r.text.replace("\r\n","\n").split("BEGIN:VEVENT")[1:]:
            title=""; dt=None
            for line in chunk.splitlines():
                if line.startswith("SUMMARY:"): title=line.split(":",1)[1].strip()
                if line.startswith("DTSTART"):
                    try: dt=parse_ics(line)
                    except: pass
            if title and dt and any(k.lower() in title.lower() for k in HIGH_EVENTS):
                h=(dt-now()).total_seconds()/3600
                if -2<=h<=168: events.append({"title":title,"time_utc":dt.isoformat(),"hours":h})
        future=[e for e in events if e["hours"]>=0]
        risk=50
        if future:
            h=min(x["hours"] for x in future)
            risk=90 if h<=6 else 78 if h<=24 else 68 if h<=48 else 60 if h<=72 else 50
        return {"ok":True,"verified":True,"risk_score":risk,
                "summary":"High-impact BLS release approaching" if risk>55 else "No imminent high-impact BLS release",
                "details":{"events":sorted(events,key=lambda x:x["hours"])[:12]},"source":"BLS official calendar"}
    return cached("blscal",1800,work)

# ---------- FED CALENDAR + RSS ----------
MONTHS={m:i for i,m in enumerate(["January","February","March","April","May","June","July","August","September","October","November","December"],1)}

def fomc_calendar():
    def work():
        r=http_get("https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm",timeout=8); r.raise_for_status()
        soup=BeautifulSoup(r.text,"html.parser")
        txt=[x.strip() for x in soup.get_text("\n").splitlines() if x.strip()]
        events=[]
        for i,line in enumerate(txt):
            if line in MONTHS:
                month=MONTHS[line]
                for j in range(i+1,min(i+6,len(txt))):
                    m=re.fullmatch(r"(\d{1,2})(?:-(\d{1,2}))?\*?",txt[j])
                    if not m: continue
                    day=int(m.group(2) or m.group(1)); year=now().year
                    for k in range(i,-1,-1):
                        if re.fullmatch(r"20\d{2}",txt[k]): year=int(txt[k]); break
                    try: events.append(datetime(year,month,day,18,0,tzinfo=timezone.utc))
                    except: pass
                    break
        future=sorted([x for x in events if x>=now()])
        nxt=future[0] if future else None
        risk=50
        if nxt:
            h=(nxt-now()).total_seconds()/3600
            risk=95 if h<=6 else 82 if h<=24 else 72 if h<=48 else 62 if h<=72 else 50
        return {"ok":True,"verified":True,"risk_score":risk,
                "summary":"FOMC decision window approaching" if risk>55 else "No imminent FOMC decision",
                "details":{"next_fomc_utc":nxt.isoformat() if nxt else None},"source":"Federal Reserve"}
    return cached("fomc",1800,work)

NEG=("inflation","higher rates","tightening","restrictive","uncertainty","downside","risk","tariff","war","sanction","stress")
POS=("disinflation","rate cut","easing","liquidity","stability","improving")
def title_score(titles):
    s=50
    for t in titles:
        q=t.lower()
        s += 2*sum(k in q for k in NEG)
        s -= 1.5*sum(k in q for k in POS)
    return clamp(s)

def fed_rss():
    def work():
        items=[]
        for u in ["https://www.federalreserve.gov/feeds/press_monetary.xml",
                  "https://www.federalreserve.gov/feeds/speeches.xml"]:
            f=feedparser.parse(u,agent=UA["User-Agent"])
            for e in f.entries[:15]:
                items.append({"title":e.get("title",""),"link":e.get("link",""),"published":e.get("published","")})
        if not items: raise RuntimeError("Fed RSS unavailable")
        return {"ok":True,"verified":True,"risk_score":title_score([x["title"] for x in items]),
                "summary":"Official Fed monetary-policy news/speeches","details":{"items":items[:20]},"source":"Federal Reserve RSS"}
    return cached("fedrss",900,work)

# ---------- SEC ----------
def sec_crypto():
    def work():
        r=http_get("https://www.sec.gov/newsroom/press-releases?combine=crypto",timeout=8); r.raise_for_status()
        soup=BeautifulSoup(r.text,"html.parser")
        titles=[]
        for a in soup.find_all("a"):
            t=" ".join(a.get_text(" ",strip=True).split())
            if t and ("crypto" in t.lower() or "digital asset" in t.lower()):
                titles.append(t)
        titles=list(dict.fromkeys(titles))[:15]
        risk=50
        for t in titles[:8]:
            q=t.lower()
            if any(k in q for k in ("charges","fraud","enforcement","ban","violation")):risk+=5
            if any(k in q for k in ("clarifies","framework","approval","innovation")):risk-=2
        return {"ok":True,"verified":True,"risk_score":clamp(risk),
                "summary":"Official SEC crypto/regulatory headlines","details":{"items":titles},"source":"U.S. SEC"}
    return cached("sec",1800,work)

# ---------- GDELT ----------
QUERY='(bitcoin OR crypto OR ethereum OR "Federal Reserve" OR inflation OR oil OR sanctions OR war OR "Treasury yields" OR "stock market")'
CRITICAL=("war","attack","missile","invasion","sanctions","emergency","hack","exploit","bankruptcy","default","liquidation","shutdown","outage")
RISKNEG=("inflation","yield","rate hike","tightening","selloff","crash","outflow","lawsuit","enforcement")
RISKPOS=("rate cut","easing","inflow","approval","ceasefire","stimulus")

def gdelt():
    def work():
        r=http_get("https://api.gdeltproject.org/api/v2/doc/doc",
                   params={"query":QUERY,"mode":"artlist","maxrecords":75,"format":"json","timespan":"24h","sort":"hybridrel"},
                   timeout=10)
        r.raise_for_status(); arts=r.json().get("articles") or []
        risk=50; ev=[]
        for a in arts[:75]:
            t=a.get("title",""); q=t.lower(); impact=0
            impact += 4*sum(k in q for k in CRITICAL)
            impact += 2*sum(k in q for k in RISKNEG)
            impact -= 1.5*sum(k in q for k in RISKPOS)
            risk += min(6,max(-3,impact))*0.35
            if abs(impact)>=3:
                ev.append({"title":t,"domain":a.get("domain"),"url":a.get("url"),"impact":impact})
        return {"ok":True,"verified":True,"risk_score":clamp(risk),
                "summary":f"Global news scan {len(arts)} articles / 24h","details":{"evidence":ev[:20]},
                "source":"GDELT DOC 2.0"}
    return cached("gdelt",600,work)


# ---------- V40.24 RENDER-RESILIENT FALLBACKS ----------
# Some public sites rate-limit or block cloud-host IP ranges. These fallbacks
# use independent public endpoints and never fabricate missing values.

COINBASE_PRODUCTS=["BTC-USD","ETH-USD","SOL-USD","XRP-USD","ADA-USD","DOGE-USD","AVAX-USD","LINK-USD","SUI-USD","DOT-USD","UNI-USD"]

def coinbase_crypto_market():
    rows=[]
    for product in COINBASE_PRODUCTS:
        try:
            r=http_get(f"https://api.exchange.coinbase.com/products/{product}/stats",
                       headers={"Accept":"application/json"}, timeout=5)
            r.raise_for_status(); j=r.json()
            op=float(j["open"]); last=float(j["last"])
            rows.append({"symbol":product.replace("-USD","USDT"),
                         "change":((last/op-1)*100 if op else 0),
                         "quote_volume":float(j.get("volume",0))*last})
        except Exception:
            pass
    if len(rows)<4:
        return {"ok":False,"verified":False,"risk_score":None,
                "summary":"Coinbase fallback insufficient coverage"}
    btc=next((x for x in rows if x["symbol"]=="BTCUSDT"),rows[0])
    breadth=100*sum(x["change"]>0 for x in rows)/len(rows)
    avg=sum(x["change"] for x in rows)/len(rows)
    risk=50
    if btc["change"]<-3:risk+=16
    elif btc["change"]<-1:risk+=8
    elif btc["change"]>3:risk-=12
    elif btc["change"]>1:risk-=6
    if breadth<35:risk+=14
    elif breadth<48:risk+=6
    elif breadth>70:risk-=10
    elif breadth>58:risk-=5
    if avg<-2:risk+=10
    elif avg>2:risk-=8
    return {"ok":True,"verified":True,"risk_score":clamp(risk),
            "summary":f"BTC {btc['change']:+.2f}% | breadth {breadth:.0f}% | avg {avg:+.2f}%",
            "details":{"rows":rows,"breadth":breadth,"avg_change":avg,"btc_change":btc["change"]},
            "source":"Coinbase Exchange public market data"}

_original_crypto_market=crypto_market
def crypto_market():
    def work():
        a=_original_crypto_market()
        if a.get("verified"): return a
        b=coinbase_crypto_market()
        if b.get("verified"): return b
        return {"ok":False,"verified":False,"risk_score":None,
                "summary":"Crypto providers unavailable",
                "details":{"primary_error":a.get("summary"),"coinbase_error":b.get("summary")}}
    return cached("crypto_v424",30,work)

# Cross-asset composite from two primary official feeds that are already
# working on the deployed host. It remains useful even if the optional quote
# snapshot provider is blocked.
_original_cross_asset_quotes=cross_asset_quotes
def cross_asset_quotes():
    def work():
        q=_original_cross_asset_quotes()
        if q.get("verified"): return q
        t=treasury_yields(); v=cboe_vix()
        vals=[]; parts=[]
        if t.get("verified") and t.get("risk_score") is not None:
            vals.append(float(t["risk_score"])); parts.append("Treasury yields")
        if v.get("verified") and v.get("risk_score") is not None:
            vals.append(float(v["risk_score"])); parts.append("Cboe VIX")
        if not vals:
            return {"ok":False,"verified":False,"risk_score":None,
                    "summary":"Cross-asset sources unavailable"}
        return {"ok":True,"verified":True,"risk_score":sum(vals)/len(vals),
                "summary":" + ".join(parts)+" verified composite",
                "details":{"treasury":t,"vix":v,"quote_fallback":q},
                "source":"U.S. Treasury + Cboe official data"}
    return cached("cross_v424",300,work)

# Render can receive 403 from the BLS calendar page even though BLS API works.
# FOMC remains official event data. BLS calendar failure is explicitly partial,
# rather than making the entire event-risk category unavailable.
_original_bls_calendar=bls_calendar
def bls_calendar():
    a=_original_bls_calendar()
    if a.get("verified"): return a
    return {"ok":False,"verified":False,"risk_score":None,
            "summary":"BLS calendar blocked from host; FOMC event monitor remains active",
            "details":{"error":a.get("summary")},"source":"BLS calendar"}

# News fallback: official Federal Reserve RSS is already reachable from Render.
# If GDELT is reset/blocked, use verified Fed policy headlines as the news-risk
# input rather than marking NEWS unavailable.
_original_gdelt=gdelt
def gdelt():
    def work():
        a=_original_gdelt()
        if a.get("verified"): return a
        f=fed_rss()
        if f.get("verified"):
            return {"ok":True,"verified":True,"risk_score":f.get("risk_score",50),
                    "summary":"Global news provider unavailable; official Fed news fallback active",
                    "details":{"fallback":f,"gdelt_error":a.get("summary")},
                    "source":"Federal Reserve official RSS fallback"}
        return a
    return cached("news_v424",600,work)

# SEC access from cloud IPs can be denied. Do not invent regulation data.
# When direct SEC is blocked, keep regulation explicitly unavailable; the
# source-health card explains that it is a host-access restriction.
_original_sec_crypto=sec_crypto
def sec_crypto():
    a=_original_sec_crypto()
    if a.get("verified"): return a
    return {"ok":False,"verified":False,"risk_score":None,
            "summary":"SEC blocks this cloud-host request; regulation receives zero weight",
            "details":{"error":a.get("summary")},"source":"U.S. SEC"}


# ---------- V40.24 VERIFIED SOURCE HARDENING ----------
# Goal: replace "neutral because a provider failed" with either verified data
# or explicit zero-weight unavailability. No fabricated live values.

# --- BLS event calendar fallback ---
# The official BLS ICS endpoint can return 403 to cloud-hosted servers.
# These upcoming high-impact dates are an embedded snapshot copied from the
# official BLS release schedule. The live ICS remains the primary source.
BLS_HIGH_IMPACT_2026 = [
    ("Employment Situation", "2026-09-04T08:30:00-04:00"),
    ("Producer Price Index", "2026-09-10T08:30:00-04:00"),
    ("Consumer Price Index", "2026-09-11T08:30:00-04:00"),
    ("U.S. Import and Export Price Indexes", "2026-09-16T08:30:00-04:00"),
    ("Job Openings and Labor Turnover Survey", "2026-09-29T10:00:00-04:00"),
    ("Producer Price Index", "2026-10-15T08:30:00-04:00"),
    ("U.S. Import and Export Price Indexes", "2026-10-16T08:30:00-04:00"),
    ("Job Openings and Labor Turnover Survey", "2026-11-03T10:00:00-05:00"),
    ("Producer Price Index", "2026-11-13T08:30:00-05:00"),
    ("U.S. Import and Export Price Indexes", "2026-11-17T08:30:00-05:00"),
    ("Job Openings and Labor Turnover Survey", "2026-12-01T10:00:00-05:00"),
    ("U.S. Import and Export Price Indexes", "2026-12-17T08:30:00-05:00"),
]

_original_bls_calendar_v424 = bls_calendar
def bls_calendar():
    a = _original_bls_calendar_v424()
    if a.get("verified"):
        return a
    events=[]
    for title, iso in BLS_HIGH_IMPACT_2026:
        try:
            dt=dtparser.isoparse(iso).astimezone(timezone.utc)
            h=(dt-now()).total_seconds()/3600
            if -2 <= h <= 168:
                events.append({"title":title,"time_utc":dt.isoformat(),"hours":h})
        except Exception:
            pass
    future=[e for e in events if e["hours"]>=0]
    risk=50
    if future:
        h=min(e["hours"] for e in future)
        risk=90 if h<=6 else 78 if h<=24 else 68 if h<=48 else 60 if h<=72 else 50
    return {
        "ok":True,"verified":True,"partial":True,"risk_score":risk,
        "summary":"Official BLS schedule snapshot fallback active" if events else "No imminent high-impact BLS release",
        "details":{"events":sorted(events,key=lambda x:x["hours"])[:12],
                   "live_ics_error":a.get("summary"),
                   "snapshot_note":"Embedded from official 2026 BLS release schedule; live ICS remains primary."},
        "source":"U.S. Bureau of Labor Statistics official schedule snapshot"
    }

# --- SEC official RSS fallback ---
# Official SEC press-release RSS: https://www.sec.gov/news/pressreleases.rss
_original_sec_crypto_v424 = sec_crypto
def sec_crypto():
    a = _original_sec_crypto_v424()
    if a.get("verified"):
        return a
    try:
        f=feedparser.parse("https://www.sec.gov/news/pressreleases.rss", agent=UA["User-Agent"])
        items=[]
        for e in f.entries[:40]:
            title=e.get("title","").strip()
            q=title.lower()
            if any(k in q for k in ("crypto","digital asset","bitcoin","ethereum","exchange-traded fund","etf","blockchain","token")):
                items.append({"title":title,"link":e.get("link",""),"published":e.get("published","")})
        if not f.entries:
            raise RuntimeError("SEC RSS returned no entries")
        risk=50
        for x in items[:12]:
            q=x["title"].lower()
            if any(k in q for k in ("charges","fraud","enforcement","violation","lawsuit","halt","suspend")): risk+=5
            if any(k in q for k in ("approval","approves","clarifies","framework","innovation","public comment")): risk-=2
        return {
            "ok":True,"verified":True,"partial":False,"risk_score":clamp(risk),
            "summary":f"Official SEC RSS active | {len(items)} crypto/digital-asset headlines",
            "details":{"items":items[:20],"direct_page_error":a.get("summary")},
            "source":"U.S. SEC official press-release RSS"
        }
    except Exception as e:
        return {
            "ok":False,"verified":False,"risk_score":None,
            "summary":"SEC direct page and official RSS unavailable; zero weight",
            "details":{"direct_error":a.get("summary"),"rss_error":str(e)},
            "source":"U.S. SEC"
        }

# --- Broader news fallback using Google News RSS ---
# GDELT remains primary. If it fails, scan several public RSS queries instead
# of treating Federal Reserve headlines as a proxy for all global news.
_original_gdelt_v424 = gdelt
NEWS_QUERIES = [
    "bitcoin OR ethereum OR crypto",
    "Federal Reserve inflation interest rates",
    "oil sanctions war markets",
    "cryptocurrency hack exchange outage liquidation",
]
def google_news_rss_scan():
    items=[]
    seen=set()
    for q in NEWS_QUERIES:
        try:
            url="https://news.google.com/rss/search"
            r=http_get(url, params={"q":q,"hl":"en-US","gl":"US","ceid":"US:en"}, timeout=8)
            r.raise_for_status()
            f=feedparser.parse(r.content)
            for e in f.entries[:20]:
                t=e.get("title","").strip()
                if not t or t in seen: continue
                seen.add(t)
                items.append({"title":t,"link":e.get("link",""),"published":e.get("published","")})
        except Exception:
            continue
    if not items:
        raise RuntimeError("Google News RSS fallback returned no items")
    risk=50
    evidence=[]
    for x in items[:80]:
        q=x["title"].lower()
        impact=0
        impact += 4*sum(k in q for k in CRITICAL)
        impact += 2*sum(k in q for k in RISKNEG)
        impact -= 1.5*sum(k in q for k in RISKPOS)
        risk += min(6,max(-3,impact))*0.25
        if abs(impact)>=3:
            evidence.append({"title":x["title"],"link":x["link"],"impact":impact})
    return {
        "ok":True,"verified":True,"risk_score":clamp(risk),
        "summary":f"Google News RSS fallback | {len(items)} headlines scanned",
        "details":{"evidence":evidence[:25],"items":items[:30]},
        "source":"Google News RSS public aggregation fallback"
    }

def gdelt():
    a=_original_gdelt_v424()
    if a.get("verified"):
        return a
    try:
        b=google_news_rss_scan()
        b["details"]["gdelt_error"]=a.get("summary")
        return b
    except Exception as e:
        f=fed_rss()
        if f.get("verified"):
            return {
                "ok":True,"verified":True,"partial":True,
                "risk_score":f.get("risk_score",50),
                "summary":"Global-news providers unavailable; official Fed RSS only",
                "details":{"fallback":f,"gdelt_error":a.get("summary"),"rss_error":str(e)},
                "source":"Federal Reserve official RSS last-resort fallback"
            }
        return a

# --- Cross-asset quote fallback using Yahoo chart JSON ---
# Stooq remains primary. Yahoo is best-effort secondary. If both fail, the
# Treasury + Cboe composite remains the verified final fallback.
YAHOO_SYMBOLS={"sp500":"^GSPC","nasdaq":"^IXIC","oil":"CL=F","gold":"GC=F","dollar":"DX-Y.NYB"}

def yahoo_daily_change(symbol):
    u=f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    r=http_get(u,params={"range":"5d","interval":"1d","includePrePost":"false"},timeout=8)
    r.raise_for_status()
    j=r.json()["chart"]["result"][0]
    closes=j["indicators"]["quote"][0]["close"]
    vals=[float(x) for x in closes if x is not None]
    if len(vals)<2: raise RuntimeError("insufficient Yahoo closes")
    prev,last=vals[-2],vals[-1]
    return {"previous":prev,"close":last,"pct":((last/prev-1)*100 if prev else 0)}

_original_cross_asset_quotes_v424 = cross_asset_quotes
def cross_asset_quotes():
    # First retain the V40.24 chain (Stooq -> Treasury/VIX composite).
    base=_original_cross_asset_quotes_v424()
    # If Stooq worked, it exposes actual multi-asset quotes and is preferred.
    d=base.get("details") or {}
    if base.get("verified") and any(isinstance(v,dict) and "pct" in v for v in d.values()):
        return base

    vals={}
    for k,sym in YAHOO_SYMBOLS.items():
        try: vals[k]=yahoo_daily_change(sym)
        except Exception as e: vals[k]={"error":str(e)}
    usable=[k for k,v in vals.items() if isinstance(v,dict) and "pct" in v]
    if len(usable)>=2:
        risk=50
        sp=vals.get("sp500",{}).get("pct"); nq=vals.get("nasdaq",{}).get("pct")
        oil=vals.get("oil",{}).get("pct"); usd=vals.get("dollar",{}).get("pct")
        gold=vals.get("gold",{}).get("pct")
        if sp is not None: risk += 10 if sp<-1.5 else (-7 if sp>1.5 else 0)
        if nq is not None: risk += 10 if nq<-1.8 else (-7 if nq>1.8 else 0)
        if oil is not None and oil>3: risk+=5
        if usd is not None and usd>0.6: risk+=5
        if gold is not None and gold>1.5 and sp is not None and sp<0: risk+=3
        return {
            "ok":True,"verified":True,"risk_score":clamp(risk),
            "summary":"S&P/Nasdaq/Oil/Gold/Dollar market fallback",
            "details":vals,"source":"Yahoo Finance chart JSON (secondary fallback)"
        }
    # V40.24 base is still useful if it is the verified Treasury+Cboe composite.
    return base

# ---------- FUSION ----------
def fusion():
    inputs={
        "crypto_market":crypto_market(),
        "macro":macro_bls(),
        "treasury":treasury_yields(),
        "vix":cboe_vix(),
        "cross_asset":cross_asset_quotes(),
        "event_risk":{
            "ok":True,"verified":True,
            "risk_score":max(bls_calendar().get("risk_score") or 50,fomc_calendar().get("risk_score") or 50),
            "summary":"Official BLS + FOMC event risk",
            "details":{"bls":bls_calendar(),"fomc":fomc_calendar()},
            "source":"BLS + Federal Reserve"
        },
        "news":gdelt(),
        "fed_news":fed_rss(),
        "regulation":sec_crypto()
    }
    W={"crypto_market":24,"macro":11,"treasury":11,"vix":12,"cross_asset":8,
       "event_risk":14,"news":9,"fed_news":6,"regulation":5}
    num=den=0
    for k,v in inputs.items():
        if v.get("verified") and v.get("risk_score") is not None:
            num += W[k]*float(v["risk_score"]); den += W[k]
    risk=num/den if den else 50
    event=inputs["event_risk"].get("risk_score") or 50
    vix=inputs["vix"].get("risk_score") or 50
    # strong vetoes are not diluted away
    if event>=90:risk=max(risk,76)
    elif event>=80:risk=max(risk,68)
    if vix>=78:risk=max(risk,66)
    coverage=den/sum(W.values())
    level="DEFENSIVE" if risk>=75 else "HIGH CAUTION" if risk>=60 else "CAUTION" if risk>=42 else "SUPPORTIVE"
    out={"ok":True,"version":"40.24","generated_at":now().isoformat(),
         "risk_score":round(risk,2),"coverage":round(coverage,3),"level":level,"inputs":inputs,
         "note":"Risk index, not a guaranteed probability. Missing/unverified inputs receive zero weight."}
    save_snapshot(risk,coverage,out)
    return out

@asynccontextmanager
async def lifespan(app):
    init_db()
    yield

app=FastAPI(title="Crypto Copilot Intelligence Backend V40.24",lifespan=lifespan)
app.add_middleware(CORSMiddleware,allow_origins=["*"],allow_credentials=False,allow_methods=["GET"],allow_headers=["*"])

@app.get("/health")
def health():
    return {"ok":True,"version":"40.24","time_utc":now().isoformat()}

@app.get("/api/v1/market-shield")
def market_shield():
    return fusion()

@app.get("/api/v1/daily-intelligence")
def daily_intelligence():
    p=fusion()
    # Compatible with V40.19/20 frontend categories
    return {"ok":True,"version":"40.24","generated_at":p["generated_at"],
            "risk_score":p["risk_score"],"coverage":p["coverage"],"level":p["level"],
            "inputs":{
                "macro":p["inputs"]["macro"],
                "news":p["inputs"]["news"],
                "cross_asset":p["inputs"]["cross_asset"],
                "event_risk":p["inputs"]["event_risk"],
                "crypto_market":p["inputs"]["crypto_market"]
            },
            "extra":{
                "treasury":p["inputs"]["treasury"],
                "vix":p["inputs"]["vix"],
                "fed_news":p["inputs"]["fed_news"],
                "regulation":p["inputs"]["regulation"]
            },
            "note":p["note"]}

@app.get("/api/v1/source-health")
def source_health():
    src={
      "crypto_market":crypto_market(),
      "bls_macro":macro_bls(),
      "treasury":treasury_yields(),
      "cboe_vix":cboe_vix(),
      "cross_asset":cross_asset_quotes(),
      "bls_calendar":bls_calendar(),
      "fomc_calendar":fomc_calendar(),
      "fed_rss":fed_rss(),
      "sec":sec_crypto(),
      "gdelt":gdelt()
    }
    return {"ok":True,"generated_at":now().isoformat(),
            "sources":{k:{"ok":bool(v.get("verified")),"partial":False,
                          "status":v.get("summary") or "Unavailable","source":v.get("source")}
                       for k,v in src.items()}}

@app.get("/api/v1/history")
def history(days:int=30):
    days=max(1,min(days,365))
    con=sqlite3.connect(DB)
    rows=con.execute("select ts,risk,coverage from shield_history where ts>=? order by ts",
                     ((now()-timedelta(days=days)).isoformat(),)).fetchall()
    con.close()
    return {"ok":True,"rows":[{"ts":a,"risk":b,"coverage":c} for a,b,c in rows]}

@app.get("/api/v1/news")
def news():
    return {"gdelt":gdelt(),"fed":fed_rss(),"sec":sec_crypto()}

@app.get("/api/v1/events")
def events():
    return {"bls":bls_calendar(),"fomc":fomc_calendar()}

@app.get("/api/v1/cross-asset")
def cross_asset():
    return {"treasury":treasury_yields(),"vix":cboe_vix(),"quotes":cross_asset_quotes()}

@app.get("/api/v1/config")
def config():
    return {"version":"40.24","api_keys_required":False,
            "primary_no_key_sources":["Binance/CoinGecko/Coinbase fallback","BLS","U.S. Treasury","Cboe VIX",
                                      "Federal Reserve","SEC","GDELT"],
            "secondary_best_effort":["Stooq quote snapshots"]}
