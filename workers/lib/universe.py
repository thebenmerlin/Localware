"""S&P 500 + S&P MidCap 400 universe loader.

Resolution strategy, in order:
  1. Scrape Wikipedia constituent tables (canonical, refreshed automatically).
  2. Fall back to a baked-in static list (last refreshed 2026-05; resilient when
     Wikipedia layout changes or there is no network).

Tickers are normalized to yfinance form (BRK.B → BRK-B, BF.B → BF-B).
"""
from __future__ import annotations

import datetime as dt
import io
import sys
from typing import Iterable

import requests

from . import db


SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
SP400_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies"
USER_AGENT = "Mozilla/5.0 (Localware-Capital research-bot; benmerlin1969@gmail.com)"

# Always-included instruments that aren't constituents themselves.
SUPPLEMENTARY = [
    ("SPY", "SPDR S&P 500 ETF",       "Index", "ETF",  "etf"),
    ("MDY", "SPDR S&P MidCap 400 ETF", "Index", "ETF",  "etf"),
]


# ---- normalization ---------------------------------------------------------

def _normalize_ticker(t: str) -> str:
    t = t.strip().upper()
    t = t.replace(".", "-")
    t = t.replace(" ", "")
    return t


# ---- scrape ----------------------------------------------------------------

def _wiki_table(url: str) -> list[tuple[str, str, str, str]]:
    """Return [(ticker, name, sector, industry)] from the first constituent table."""
    import pandas as pd  # local import — pandas is heavy

    headers = {"User-Agent": USER_AGENT}
    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()
    tables = pd.read_html(io.StringIO(r.text))
    # Pick the first table that has Symbol + GICS Sector columns
    for t in tables:
        cols = [str(c).lower() for c in t.columns]
        if any("symbol" in c for c in cols) and any("gics" in c for c in cols):
            df = t.copy()
            df.columns = [str(c) for c in df.columns]
            sym_col = next(c for c in df.columns if "symbol" in c.lower())
            name_col = next((c for c in df.columns if "security" in c.lower() or "company" in c.lower()), sym_col)
            sec_col = next(c for c in df.columns if "gics" in c.lower() and "sector" in c.lower())
            ind_col = next((c for c in df.columns if "gics" in c.lower() and ("sub" in c.lower() or "industry" in c.lower())), sec_col)
            out = []
            for _, row in df.iterrows():
                tk = _normalize_ticker(str(row[sym_col]))
                if not tk or tk == "NAN":
                    continue
                out.append((tk, str(row[name_col]).strip(), str(row[sec_col]).strip(), str(row[ind_col]).strip()))
            return out
    raise RuntimeError(f"Could not find constituent table at {url}")


def fetch_constituents() -> list[tuple[str, str, str, str, str]]:
    """[(ticker, name, sector, industry, asset_class)] for the full universe."""
    out: list[tuple[str, str, str, str, str]] = []
    seen: set[str] = set()
    sources_used: list[str] = []

    try:
        sp500 = _wiki_table(SP500_URL)
        sources_used.append(f"S&P 500 from Wikipedia ({len(sp500)})")
    except Exception as e:
        print(f"  S&P 500 wiki fetch failed: {e}", file=sys.stderr)
        sp500 = [(t, n, s, i) for t, n, s, i in STATIC_SP500]
        sources_used.append(f"S&P 500 from static fallback ({len(sp500)})")

    try:
        sp400 = _wiki_table(SP400_URL)
        sources_used.append(f"S&P 400 from Wikipedia ({len(sp400)})")
    except Exception as e:
        print(f"  S&P 400 wiki fetch failed: {e}", file=sys.stderr)
        sp400 = [(t, n, s, i) for t, n, s, i in STATIC_SP400]
        sources_used.append(f"S&P 400 from static fallback ({len(sp400)})")

    for t, n, s, i in sp500 + sp400:
        if t and t not in seen:
            seen.add(t)
            out.append((t, n, s, i, "equity"))

    for t, n, s, i, ac in SUPPLEMENTARY:
        if t not in seen:
            seen.add(t)
            out.append((t, n, s, i, ac))

    print("Universe sources: " + "; ".join(sources_used))
    return out


# ---- DB --------------------------------------------------------------------

UPSERT_SQL = """
INSERT INTO securities (ticker, name, sector, industry, asset_class, active)
VALUES (%s, %s, %s, %s, %s, TRUE)
ON CONFLICT (ticker) DO UPDATE
  SET name = EXCLUDED.name,
      sector = EXCLUDED.sector,
      industry = EXCLUDED.industry,
      asset_class = EXCLUDED.asset_class,
      active = TRUE
RETURNING id;
"""


def load_universe(constituents: Iterable[tuple[str, str, str, str, str]] | None = None) -> int:
    if constituents is None:
        constituents = fetch_constituents()
    constituents = list(constituents)
    today = dt.date.today()

    with db.conn() as c, c.cursor() as cur:
        for ticker, name, sector, industry, asset_class in constituents:
            cur.execute(UPSERT_SQL, (ticker, name, sector, industry, asset_class))
            sid = cur.fetchone()[0]
            cur.execute(
                """
                INSERT INTO universe (security_id, included_at)
                VALUES (%s, %s)
                ON CONFLICT (security_id, included_at) DO NOTHING;
                """,
                (sid, today),
            )
        cur.execute("SELECT COUNT(*) FROM securities WHERE active = TRUE;")
        n = cur.fetchone()[0]
    return n


def get_active(asset_class: str | None = "equity") -> list[dict]:
    sql = "SELECT id, ticker, name, sector, industry, asset_class FROM securities WHERE active = TRUE"
    params: list = []
    if asset_class:
        sql += " AND asset_class = %s"
        params.append(asset_class)
    sql += " ORDER BY ticker;"
    return db.query(sql, params)


def get_benchmark() -> dict | None:
    rows = db.query("SELECT id, ticker FROM securities WHERE ticker = 'SPY' LIMIT 1;")
    return rows[0] if rows else None


# ---- static fallback (last refreshed 2026-05) ------------------------------
# (ticker, name, sector, industry)
# Both lists are intentionally compact — only the columns we use downstream.

STATIC_SP500: list[tuple[str, str, str, str]] = [
    ("MMM","3M","Industrials","Industrial Conglomerates"),("AOS","A. O. Smith","Industrials","Building Products"),
    ("ABT","Abbott Laboratories","Health Care","Health Care Equipment"),("ABBV","AbbVie","Health Care","Pharmaceuticals"),
    ("ACN","Accenture","Information Technology","IT Consulting"),("ADBE","Adobe","Information Technology","Application Software"),
    ("AMD","AMD","Information Technology","Semiconductors"),("AES","AES","Utilities","Independent Power Producers"),
    ("AFL","Aflac","Financials","Insurance"),("A","Agilent Technologies","Health Care","Life Sciences Tools"),
    ("APD","Air Products","Materials","Industrial Gases"),("ABNB","Airbnb","Consumer Discretionary","Hotels Resorts & Cruise Lines"),
    ("AKAM","Akamai","Information Technology","Internet Services & Infrastructure"),("ALB","Albemarle","Materials","Specialty Chemicals"),
    ("ARE","Alexandria Real Estate","Real Estate","Office REITs"),("ALGN","Align Technology","Health Care","Health Care Supplies"),
    ("ALLE","Allegion","Industrials","Building Products"),("LNT","Alliant Energy","Utilities","Electric Utilities"),
    ("ALL","Allstate","Financials","Insurance"),("GOOGL","Alphabet (A)","Communication Services","Interactive Media"),
    ("GOOG","Alphabet (C)","Communication Services","Interactive Media"),("MO","Altria","Consumer Staples","Tobacco"),
    ("AMZN","Amazon","Consumer Discretionary","Internet Retail"),("AMCR","Amcor","Materials","Containers & Packaging"),
    ("AEE","Ameren","Utilities","Multi-Utilities"),("AEP","American Electric Power","Utilities","Electric Utilities"),
    ("AXP","American Express","Financials","Consumer Finance"),("AIG","American International","Financials","Insurance"),
    ("AMT","American Tower","Real Estate","Specialized REITs"),("AWK","American Water","Utilities","Water Utilities"),
    ("AMP","Ameriprise","Financials","Capital Markets"),("AME","Ametek","Industrials","Electrical Components"),
    ("AMGN","Amgen","Health Care","Biotechnology"),("APH","Amphenol","Information Technology","Electronic Components"),
    ("ADI","Analog Devices","Information Technology","Semiconductors"),("ANSS","Ansys","Information Technology","Application Software"),
    ("AON","Aon","Financials","Insurance Brokers"),("APA","APA","Energy","Oil & Gas E&P"),
    ("AAPL","Apple","Information Technology","Technology Hardware"),("AMAT","Applied Materials","Information Technology","Semiconductor Equipment"),
    ("APTV","Aptiv","Consumer Discretionary","Automotive Parts"),("ACGL","Arch Capital","Financials","Reinsurance"),
    ("ADM","ADM","Consumer Staples","Agricultural Products"),("ANET","Arista Networks","Information Technology","Communications Equipment"),
    ("AJG","Arthur J. Gallagher","Financials","Insurance Brokers"),("AIZ","Assurant","Financials","Insurance"),
    ("T","AT&T","Communication Services","Telecom"),("ATO","Atmos Energy","Utilities","Gas Utilities"),
    ("ADSK","Autodesk","Information Technology","Application Software"),("ADP","ADP","Industrials","Payroll Services"),
    ("AZO","AutoZone","Consumer Discretionary","Auto Retail"),("AVB","AvalonBay","Real Estate","Residential REITs"),
    ("AVY","Avery Dennison","Materials","Packaging"),("AXON","Axon Enterprise","Industrials","Aerospace & Defense"),
    ("BKR","Baker Hughes","Energy","Oil Services"),("BALL","Ball","Materials","Metal Containers"),
    ("BAC","Bank of America","Financials","Banks"),("BK","BNY Mellon","Financials","Asset Management"),
    ("BBWI","Bath & Body Works","Consumer Discretionary","Specialty Retail"),("BAX","Baxter","Health Care","Health Care Equipment"),
    ("BDX","BD","Health Care","Health Care Equipment"),("WRB","W. R. Berkley","Financials","Insurance"),
    ("BRK-B","Berkshire Hathaway","Financials","Multi-Sector Holdings"),("BBY","Best Buy","Consumer Discretionary","Computer & Electronics Retail"),
    ("TECH","Bio-Techne","Health Care","Life Sciences Tools"),("BIIB","Biogen","Health Care","Biotechnology"),
    ("BLK","BlackRock","Financials","Asset Management"),("BX","Blackstone","Financials","Capital Markets"),
    ("BA","Boeing","Industrials","Aerospace & Defense"),("BKNG","Booking Holdings","Consumer Discretionary","Hotels Resorts & Cruise Lines"),
    ("BSX","Boston Scientific","Health Care","Health Care Equipment"),("BMY","Bristol-Myers Squibb","Health Care","Pharmaceuticals"),
    ("AVGO","Broadcom","Information Technology","Semiconductors"),("BR","Broadridge","Industrials","Data Processing"),
    ("BRO","Brown & Brown","Financials","Insurance Brokers"),("BF-B","Brown-Forman","Consumer Staples","Distillers"),
    ("BLDR","Builders FirstSource","Industrials","Building Products"),("BG","Bunge","Consumer Staples","Agricultural Products"),
    ("BXP","BXP","Real Estate","Office REITs"),("CHRW","C.H. Robinson","Industrials","Air Freight & Logistics"),
    ("CDNS","Cadence Design","Information Technology","Application Software"),("CZR","Caesars","Consumer Discretionary","Casinos"),
    ("CPT","Camden Property","Real Estate","Residential REITs"),("CPB","Campbell's","Consumer Staples","Packaged Foods"),
    ("COF","Capital One","Financials","Consumer Finance"),("CAH","Cardinal Health","Health Care","Health Care Distributors"),
    ("KMX","CarMax","Consumer Discretionary","Automotive Retail"),("CCL","Carnival","Consumer Discretionary","Hotels Resorts & Cruise Lines"),
    ("CARR","Carrier Global","Industrials","Building Products"),("CAT","Caterpillar","Industrials","Construction Machinery"),
    ("CBOE","Cboe Global Markets","Financials","Financial Exchanges"),("CBRE","CBRE Group","Real Estate","Real Estate Services"),
    ("CDW","CDW","Information Technology","Technology Distributors"),("CE","Celanese","Materials","Specialty Chemicals"),
    ("COR","Cencora","Health Care","Health Care Distributors"),("CNC","Centene","Health Care","Managed Health Care"),
    ("CNP","CenterPoint Energy","Utilities","Multi-Utilities"),("CF","CF Industries","Materials","Fertilizers"),
    ("CRL","Charles River Labs","Health Care","Life Sciences Tools"),("SCHW","Charles Schwab","Financials","Capital Markets"),
    ("CHTR","Charter Communications","Communication Services","Cable & Satellite"),("CVX","Chevron","Energy","Integrated Oil"),
    ("CMG","Chipotle","Consumer Discretionary","Restaurants"),("CB","Chubb","Financials","Insurance"),
    ("CHD","Church & Dwight","Consumer Staples","Household Products"),("CI","Cigna","Health Care","Managed Health Care"),
    ("CINF","Cincinnati Financial","Financials","Insurance"),("CTAS","Cintas","Industrials","Diversified Support Services"),
    ("CSCO","Cisco","Information Technology","Networking"),("C","Citigroup","Financials","Banks"),
    ("CFG","Citizens Financial","Financials","Banks"),("CLX","Clorox","Consumer Staples","Household Products"),
    ("CME","CME Group","Financials","Financial Exchanges"),("CMS","CMS Energy","Utilities","Multi-Utilities"),
    ("KO","Coca-Cola","Consumer Staples","Beverages"),("CTSH","Cognizant","Information Technology","IT Services"),
    ("CL","Colgate-Palmolive","Consumer Staples","Household Products"),("CMCSA","Comcast","Communication Services","Cable & Satellite"),
    ("CAG","Conagra Brands","Consumer Staples","Packaged Foods"),("COP","ConocoPhillips","Energy","Oil & Gas E&P"),
    ("ED","Consolidated Edison","Utilities","Multi-Utilities"),("STZ","Constellation Brands","Consumer Staples","Distillers"),
    ("CEG","Constellation Energy","Utilities","Independent Power"),("COO","Cooper Companies","Health Care","Health Care Supplies"),
    ("CPRT","Copart","Industrials","Diversified Support Services"),("GLW","Corning","Information Technology","Electronic Components"),
    ("CPAY","Corpay","Financials","Data Processing"),("CTVA","Corteva","Materials","Fertilizers"),
    ("CSGP","CoStar Group","Real Estate","Real Estate Services"),("COST","Costco","Consumer Staples","Hypermarkets"),
    ("CTRA","Coterra","Energy","Oil & Gas E&P"),("CRWD","CrowdStrike","Information Technology","Application Software"),
    ("CCI","Crown Castle","Real Estate","Specialized REITs"),("CSX","CSX","Industrials","Railroads"),
    ("CMI","Cummins","Industrials","Construction Machinery"),("CVS","CVS Health","Health Care","Health Care Services"),
    ("DHR","Danaher","Health Care","Life Sciences Tools"),("DRI","Darden Restaurants","Consumer Discretionary","Restaurants"),
    ("DVA","DaVita","Health Care","Health Care Services"),("DAY","Dayforce","Information Technology","Application Software"),
    ("DECK","Deckers","Consumer Discretionary","Footwear"),("DE","Deere","Industrials","Farm Machinery"),
    ("DELL","Dell Technologies","Information Technology","Technology Hardware"),("DAL","Delta Air Lines","Industrials","Airlines"),
    ("DVN","Devon Energy","Energy","Oil & Gas E&P"),("DXCM","Dexcom","Health Care","Health Care Equipment"),
    ("FANG","Diamondback Energy","Energy","Oil & Gas E&P"),("DLR","Digital Realty","Real Estate","Specialized REITs"),
    ("DFS","Discover","Financials","Consumer Finance"),("DG","Dollar General","Consumer Staples","Drug Retail"),
    ("DLTR","Dollar Tree","Consumer Discretionary","Variety Stores"),("D","Dominion Energy","Utilities","Electric Utilities"),
    ("DPZ","Domino's","Consumer Discretionary","Restaurants"),("DASH","DoorDash","Consumer Discretionary","Internet Retail"),
    ("DOV","Dover","Industrials","Industrial Machinery"),("DOW","Dow","Materials","Commodity Chemicals"),
    ("DHI","D.R. Horton","Consumer Discretionary","Homebuilding"),("DTE","DTE Energy","Utilities","Multi-Utilities"),
    ("DUK","Duke Energy","Utilities","Electric Utilities"),("DD","DuPont","Materials","Specialty Chemicals"),
    ("EMN","Eastman Chemical","Materials","Specialty Chemicals"),("ETN","Eaton","Industrials","Electrical Components"),
    ("EBAY","eBay","Consumer Discretionary","Internet Retail"),("ECL","Ecolab","Materials","Specialty Chemicals"),
    ("EIX","Edison International","Utilities","Electric Utilities"),("EW","Edwards Lifesciences","Health Care","Health Care Equipment"),
    ("EA","Electronic Arts","Communication Services","Interactive Entertainment"),("ELV","Elevance Health","Health Care","Managed Health Care"),
    ("EMR","Emerson Electric","Industrials","Electrical Components"),("ENPH","Enphase Energy","Information Technology","Solar Equipment"),
    ("ETR","Entergy","Utilities","Electric Utilities"),("EOG","EOG Resources","Energy","Oil & Gas E&P"),
    ("EPAM","EPAM Systems","Information Technology","IT Consulting"),("EQT","EQT","Energy","Oil & Gas E&P"),
    ("EFX","Equifax","Industrials","Research & Consulting"),("EQIX","Equinix","Real Estate","Specialized REITs"),
    ("EQR","Equity Residential","Real Estate","Residential REITs"),("ERIE","Erie Indemnity","Financials","Insurance"),
    ("ESS","Essex Property","Real Estate","Residential REITs"),("EL","Estée Lauder","Consumer Staples","Personal Products"),
    ("EG","Everest Group","Financials","Reinsurance"),("EVRG","Evergy","Utilities","Electric Utilities"),
    ("ES","Eversource Energy","Utilities","Electric Utilities"),("EXC","Exelon","Utilities","Electric Utilities"),
    ("EXE","Expand Energy","Energy","Oil & Gas E&P"),("EXPE","Expedia","Consumer Discretionary","Hotels Resorts & Cruise Lines"),
    ("EXPD","Expeditors","Industrials","Air Freight & Logistics"),("EXR","Extra Space Storage","Real Estate","Specialized REITs"),
    ("XOM","ExxonMobil","Energy","Integrated Oil"),("FFIV","F5","Information Technology","Communications Equipment"),
    ("FDS","FactSet","Financials","Financial Data"),("FICO","Fair Isaac","Information Technology","Application Software"),
    ("FAST","Fastenal","Industrials","Trading Companies & Distributors"),("FRT","Federal Realty","Real Estate","Retail REITs"),
    ("FDX","FedEx","Industrials","Air Freight & Logistics"),("FIS","Fidelity National","Financials","Data Processing"),
    ("FITB","Fifth Third","Financials","Banks"),("FSLR","First Solar","Information Technology","Solar Equipment"),
    ("FE","FirstEnergy","Utilities","Electric Utilities"),("FI","Fiserv","Financials","Data Processing"),
    ("F","Ford","Consumer Discretionary","Automobile Manufacturers"),("FTNT","Fortinet","Information Technology","Systems Software"),
    ("FTV","Fortive","Industrials","Industrial Machinery"),("FOXA","Fox (A)","Communication Services","Broadcasting"),
    ("FOX","Fox (B)","Communication Services","Broadcasting"),("BEN","Franklin Resources","Financials","Asset Management"),
    ("FCX","Freeport-McMoRan","Materials","Copper"),("GRMN","Garmin","Consumer Discretionary","Consumer Electronics"),
    ("IT","Gartner","Information Technology","IT Consulting"),("GE","GE Aerospace","Industrials","Aerospace & Defense"),
    ("GEHC","GE HealthCare","Health Care","Health Care Equipment"),("GEV","GE Vernova","Industrials","Heavy Electrical"),
    ("GEN","Gen Digital","Information Technology","Systems Software"),("GNRC","Generac","Industrials","Electrical Components"),
    ("GD","General Dynamics","Industrials","Aerospace & Defense"),("GIS","General Mills","Consumer Staples","Packaged Foods"),
    ("GM","General Motors","Consumer Discretionary","Automobile Manufacturers"),("GPC","Genuine Parts","Consumer Discretionary","Automotive Retail"),
    ("GILD","Gilead Sciences","Health Care","Biotechnology"),("GPN","Global Payments","Financials","Data Processing"),
    ("GL","Globe Life","Financials","Insurance"),("GDDY","GoDaddy","Information Technology","Internet Services"),
    ("GS","Goldman Sachs","Financials","Capital Markets"),("HAL","Halliburton","Energy","Oil Services"),
    ("HIG","Hartford","Financials","Insurance"),("HAS","Hasbro","Consumer Discretionary","Leisure Products"),
    ("HCA","HCA Healthcare","Health Care","Health Care Facilities"),("DOC","Healthpeak","Real Estate","Health Care REITs"),
    ("HSIC","Henry Schein","Health Care","Health Care Distributors"),("HSY","Hershey","Consumer Staples","Packaged Foods"),
    ("HES","Hess","Energy","Oil & Gas E&P"),("HPE","HP Enterprise","Information Technology","Technology Hardware"),
    ("HLT","Hilton","Consumer Discretionary","Hotels Resorts & Cruise Lines"),("HOLX","Hologic","Health Care","Health Care Equipment"),
    ("HD","Home Depot","Consumer Discretionary","Home Improvement Retail"),("HON","Honeywell","Industrials","Industrial Conglomerates"),
    ("HRL","Hormel","Consumer Staples","Packaged Foods"),("HST","Host Hotels","Real Estate","Hotel & Resort REITs"),
    ("HWM","Howmet Aerospace","Industrials","Aerospace & Defense"),("HPQ","HP","Information Technology","Technology Hardware"),
    ("HUBB","Hubbell","Industrials","Electrical Components"),("HUM","Humana","Health Care","Managed Health Care"),
    ("HBAN","Huntington Bancshares","Financials","Banks"),("HII","Huntington Ingalls","Industrials","Aerospace & Defense"),
    ("IBM","IBM","Information Technology","IT Services"),("IEX","IDEX","Industrials","Industrial Machinery"),
    ("IDXX","Idexx","Health Care","Health Care Equipment"),("ITW","Illinois Tool Works","Industrials","Industrial Machinery"),
    ("INCY","Incyte","Health Care","Biotechnology"),("IR","Ingersoll Rand","Industrials","Industrial Machinery"),
    ("PODD","Insulet","Health Care","Health Care Equipment"),("INTC","Intel","Information Technology","Semiconductors"),
    ("ICE","Intercontinental Exchange","Financials","Financial Exchanges"),("IFF","IFF","Materials","Specialty Chemicals"),
    ("IP","International Paper","Materials","Paper Packaging"),("INTU","Intuit","Information Technology","Application Software"),
    ("ISRG","Intuitive Surgical","Health Care","Health Care Equipment"),("IVZ","Invesco","Financials","Asset Management"),
    ("INVH","Invitation Homes","Real Estate","Residential REITs"),("IQV","IQVIA","Health Care","Life Sciences Tools"),
    ("IRM","Iron Mountain","Real Estate","Specialized REITs"),("JBHT","J.B. Hunt","Industrials","Trucking"),
    ("JBL","Jabil","Information Technology","Electronic Manufacturing"),("JKHY","Jack Henry","Financials","Data Processing"),
    ("J","Jacobs","Industrials","Construction & Engineering"),("JNJ","Johnson & Johnson","Health Care","Pharmaceuticals"),
    ("JCI","Johnson Controls","Industrials","Building Products"),("JPM","JPMorgan Chase","Financials","Banks"),
    ("JNPR","Juniper Networks","Information Technology","Communications Equipment"),("K","Kellanova","Consumer Staples","Packaged Foods"),
    ("KVUE","Kenvue","Consumer Staples","Personal Products"),("KDP","Keurig Dr Pepper","Consumer Staples","Beverages"),
    ("KEY","KeyCorp","Financials","Banks"),("KEYS","Keysight","Information Technology","Electronic Equipment"),
    ("KMB","Kimberly-Clark","Consumer Staples","Household Products"),("KIM","Kimco Realty","Real Estate","Retail REITs"),
    ("KMI","Kinder Morgan","Energy","Oil & Gas Storage"),("KKR","KKR","Financials","Capital Markets"),
    ("KLAC","KLA","Information Technology","Semiconductor Equipment"),("KHC","Kraft Heinz","Consumer Staples","Packaged Foods"),
    ("KR","Kroger","Consumer Staples","Food Retail"),("LHX","L3Harris","Industrials","Aerospace & Defense"),
    ("LH","LabCorp","Health Care","Health Care Services"),("LRCX","Lam Research","Information Technology","Semiconductor Equipment"),
    ("LW","Lamb Weston","Consumer Staples","Packaged Foods"),("LVS","Las Vegas Sands","Consumer Discretionary","Casinos"),
    ("LDOS","Leidos","Industrials","IT Services"),("LEN","Lennar","Consumer Discretionary","Homebuilding"),
    ("LII","Lennox","Industrials","Building Products"),("LLY","Eli Lilly","Health Care","Pharmaceuticals"),
    ("LIN","Linde","Materials","Industrial Gases"),("LYV","Live Nation","Communication Services","Movies & Entertainment"),
    ("LKQ","LKQ","Consumer Discretionary","Distributors"),("LMT","Lockheed Martin","Industrials","Aerospace & Defense"),
    ("L","Loews","Financials","Insurance"),("LOW","Lowe's","Consumer Discretionary","Home Improvement Retail"),
    ("LULU","Lululemon","Consumer Discretionary","Apparel"),("LYB","LyondellBasell","Materials","Commodity Chemicals"),
    ("MTB","M&T Bank","Financials","Banks"),("MPC","Marathon Petroleum","Energy","Oil & Gas Refining"),
    ("MKTX","MarketAxess","Financials","Financial Exchanges"),("MAR","Marriott","Consumer Discretionary","Hotels Resorts & Cruise Lines"),
    ("MMC","Marsh & McLennan","Financials","Insurance Brokers"),("MLM","Martin Marietta","Materials","Construction Materials"),
    ("MAS","Masco","Industrials","Building Products"),("MA","Mastercard","Financials","Transaction Processing"),
    ("MTCH","Match Group","Communication Services","Interactive Media"),("MKC","McCormick","Consumer Staples","Packaged Foods"),
    ("MCD","McDonald's","Consumer Discretionary","Restaurants"),("MCK","McKesson","Health Care","Health Care Distributors"),
    ("MDT","Medtronic","Health Care","Health Care Equipment"),("MRK","Merck","Health Care","Pharmaceuticals"),
    ("META","Meta Platforms","Communication Services","Interactive Media"),("MET","MetLife","Financials","Insurance"),
    ("MTD","Mettler-Toledo","Health Care","Life Sciences Tools"),("MGM","MGM Resorts","Consumer Discretionary","Casinos"),
    ("MCHP","Microchip","Information Technology","Semiconductors"),("MU","Micron","Information Technology","Semiconductors"),
    ("MSFT","Microsoft","Information Technology","Systems Software"),("MAA","Mid-America Apartment","Real Estate","Residential REITs"),
    ("MRNA","Moderna","Health Care","Biotechnology"),("MHK","Mohawk","Consumer Discretionary","Home Furnishings"),
    ("MOH","Molina Healthcare","Health Care","Managed Health Care"),("TAP","Molson Coors","Consumer Staples","Brewers"),
    ("MDLZ","Mondelez","Consumer Staples","Packaged Foods"),("MPWR","Monolithic Power","Information Technology","Semiconductors"),
    ("MNST","Monster","Consumer Staples","Beverages"),("MCO","Moody's","Financials","Financial Data"),
    ("MS","Morgan Stanley","Financials","Capital Markets"),("MOS","Mosaic","Materials","Fertilizers"),
    ("MSI","Motorola Solutions","Information Technology","Communications Equipment"),("MSCI","MSCI","Financials","Financial Data"),
    ("NDAQ","Nasdaq","Financials","Financial Exchanges"),("NTAP","NetApp","Information Technology","Technology Hardware"),
    ("NFLX","Netflix","Communication Services","Movies & Entertainment"),("NEM","Newmont","Materials","Gold"),
    ("NWSA","News Corp (A)","Communication Services","Publishing"),("NWS","News Corp (B)","Communication Services","Publishing"),
    ("NEE","NextEra Energy","Utilities","Electric Utilities"),("NKE","Nike","Consumer Discretionary","Footwear"),
    ("NI","NiSource","Utilities","Multi-Utilities"),("NDSN","Nordson","Industrials","Industrial Machinery"),
    ("NSC","Norfolk Southern","Industrials","Railroads"),("NTRS","Northern Trust","Financials","Asset Management"),
    ("NOC","Northrop Grumman","Industrials","Aerospace & Defense"),("NCLH","Norwegian Cruise","Consumer Discretionary","Hotels Resorts & Cruise Lines"),
    ("NRG","NRG Energy","Utilities","Independent Power"),("NUE","Nucor","Materials","Steel"),
    ("NVDA","NVIDIA","Information Technology","Semiconductors"),("NVR","NVR","Consumer Discretionary","Homebuilding"),
    ("NXPI","NXP","Information Technology","Semiconductors"),("ORLY","O'Reilly Auto","Consumer Discretionary","Automotive Retail"),
    ("OXY","Occidental","Energy","Oil & Gas E&P"),("ODFL","Old Dominion","Industrials","Trucking"),
    ("OMC","Omnicom","Communication Services","Advertising"),("ON","ON Semi","Information Technology","Semiconductors"),
    ("OKE","ONEOK","Energy","Oil & Gas Storage"),("ORCL","Oracle","Information Technology","Application Software"),
    ("OTIS","Otis","Industrials","Building Products"),("PCAR","Paccar","Industrials","Construction Machinery"),
    ("PKG","Packaging Corp","Materials","Paper Packaging"),("PLTR","Palantir","Information Technology","Application Software"),
    ("PANW","Palo Alto Networks","Information Technology","Systems Software"),("PARA","Paramount","Communication Services","Movies & Entertainment"),
    ("PH","Parker Hannifin","Industrials","Industrial Machinery"),("PAYX","Paychex","Industrials","Data Processing"),
    ("PAYC","Paycom","Information Technology","Application Software"),("PYPL","PayPal","Financials","Transaction Processing"),
    ("PNR","Pentair","Industrials","Industrial Machinery"),("PEP","PepsiCo","Consumer Staples","Beverages"),
    ("PFE","Pfizer","Health Care","Pharmaceuticals"),("PCG","PG&E","Utilities","Electric Utilities"),
    ("PM","Philip Morris","Consumer Staples","Tobacco"),("PSX","Phillips 66","Energy","Oil & Gas Refining"),
    ("PNW","Pinnacle West","Utilities","Electric Utilities"),("PNC","PNC","Financials","Banks"),
    ("POOL","Pool","Consumer Discretionary","Distributors"),("PPG","PPG","Materials","Specialty Chemicals"),
    ("PPL","PPL","Utilities","Electric Utilities"),("PFG","Principal Financial","Financials","Insurance"),
    ("PG","Procter & Gamble","Consumer Staples","Household Products"),("PGR","Progressive","Financials","Insurance"),
    ("PLD","Prologis","Real Estate","Industrial REITs"),("PRU","Prudential","Financials","Insurance"),
    ("PEG","PSEG","Utilities","Electric Utilities"),("PTC","PTC","Information Technology","Application Software"),
    ("PSA","Public Storage","Real Estate","Specialized REITs"),("PHM","PulteGroup","Consumer Discretionary","Homebuilding"),
    ("PWR","Quanta Services","Industrials","Construction & Engineering"),("QCOM","Qualcomm","Information Technology","Semiconductors"),
    ("DGX","Quest Diagnostics","Health Care","Health Care Services"),("RL","Ralph Lauren","Consumer Discretionary","Apparel"),
    ("RJF","Raymond James","Financials","Capital Markets"),("RTX","RTX","Industrials","Aerospace & Defense"),
    ("O","Realty Income","Real Estate","Retail REITs"),("REG","Regency Centers","Real Estate","Retail REITs"),
    ("REGN","Regeneron","Health Care","Biotechnology"),("RF","Regions Financial","Financials","Banks"),
    ("RSG","Republic Services","Industrials","Environmental Services"),("RMD","ResMed","Health Care","Health Care Equipment"),
    ("RVTY","Revvity","Health Care","Life Sciences Tools"),("ROK","Rockwell","Industrials","Industrial Machinery"),
    ("ROL","Rollins","Industrials","Diversified Support Services"),("ROP","Roper","Information Technology","Application Software"),
    ("ROST","Ross Stores","Consumer Discretionary","Apparel Retail"),("RCL","Royal Caribbean","Consumer Discretionary","Hotels Resorts & Cruise Lines"),
    ("SPGI","S&P Global","Financials","Financial Data"),("CRM","Salesforce","Information Technology","Application Software"),
    ("SBAC","SBA Communications","Real Estate","Specialized REITs"),("SLB","Schlumberger","Energy","Oil Services"),
    ("STX","Seagate","Information Technology","Technology Hardware"),("SRE","Sempra","Utilities","Multi-Utilities"),
    ("NOW","ServiceNow","Information Technology","Application Software"),("SHW","Sherwin-Williams","Materials","Specialty Chemicals"),
    ("SPG","Simon Property","Real Estate","Retail REITs"),("SWKS","Skyworks","Information Technology","Semiconductors"),
    ("SJM","J.M. Smucker","Consumer Staples","Packaged Foods"),("SW","Smurfit Westrock","Materials","Paper Packaging"),
    ("SNA","Snap-on","Industrials","Industrial Machinery"),("SOLV","Solventum","Health Care","Health Care Equipment"),
    ("SO","Southern Co","Utilities","Electric Utilities"),("LUV","Southwest Airlines","Industrials","Airlines"),
    ("SWK","Stanley Black & Decker","Industrials","Industrial Machinery"),("SBUX","Starbucks","Consumer Discretionary","Restaurants"),
    ("STT","State Street","Financials","Asset Management"),("STLD","Steel Dynamics","Materials","Steel"),
    ("STE","Steris","Health Care","Health Care Equipment"),("SYK","Stryker","Health Care","Health Care Equipment"),
    ("SMCI","Super Micro","Information Technology","Technology Hardware"),("SYF","Synchrony","Financials","Consumer Finance"),
    ("SNPS","Synopsys","Information Technology","Application Software"),("SYY","Sysco","Consumer Staples","Food Distributors"),
    ("TMUS","T-Mobile","Communication Services","Wireless Telecom"),("TROW","T. Rowe Price","Financials","Asset Management"),
    ("TTWO","Take-Two","Communication Services","Interactive Entertainment"),("TPR","Tapestry","Consumer Discretionary","Apparel"),
    ("TRGP","Targa Resources","Energy","Oil & Gas Storage"),("TGT","Target","Consumer Staples","Hypermarkets"),
    ("TEL","TE Connectivity","Information Technology","Electronic Components"),("TDY","Teledyne","Industrials","Aerospace & Defense"),
    ("TFX","Teleflex","Health Care","Health Care Equipment"),("TER","Teradyne","Information Technology","Semiconductor Equipment"),
    ("TSLA","Tesla","Consumer Discretionary","Automobile Manufacturers"),("TXN","Texas Instruments","Information Technology","Semiconductors"),
    ("TXT","Textron","Industrials","Aerospace & Defense"),("TMO","Thermo Fisher","Health Care","Life Sciences Tools"),
    ("TJX","TJX","Consumer Discretionary","Apparel Retail"),("TKO","TKO Group","Communication Services","Movies & Entertainment"),
    ("TSCO","Tractor Supply","Consumer Discretionary","Specialty Retail"),("TT","Trane Technologies","Industrials","Building Products"),
    ("TDG","TransDigm","Industrials","Aerospace & Defense"),("TRV","Travelers","Financials","Insurance"),
    ("TRMB","Trimble","Information Technology","Electronic Equipment"),("TFC","Truist","Financials","Banks"),
    ("TYL","Tyler Technologies","Information Technology","Application Software"),("TSN","Tyson","Consumer Staples","Packaged Foods"),
    ("USB","U.S. Bancorp","Financials","Banks"),("UBER","Uber","Industrials","Ground Transportation"),
    ("UDR","UDR","Real Estate","Residential REITs"),("ULTA","Ulta Beauty","Consumer Discretionary","Specialty Retail"),
    ("UNP","Union Pacific","Industrials","Railroads"),("UAL","United Airlines","Industrials","Airlines"),
    ("UPS","UPS","Industrials","Air Freight & Logistics"),("URI","United Rentals","Industrials","Trading & Distribution"),
    ("UNH","UnitedHealth","Health Care","Managed Health Care"),("UHS","Universal Health","Health Care","Health Care Facilities"),
    ("VLO","Valero","Energy","Oil & Gas Refining"),("VTR","Ventas","Real Estate","Health Care REITs"),
    ("VLTO","Veralto","Industrials","Industrial Machinery"),("VRSN","Verisign","Information Technology","Internet Services"),
    ("VRSK","Verisk","Industrials","Research & Consulting"),("VZ","Verizon","Communication Services","Telecom"),
    ("VRTX","Vertex","Health Care","Biotechnology"),("VTRS","Viatris","Health Care","Pharmaceuticals"),
    ("VICI","VICI Properties","Real Estate","Specialized REITs"),("V","Visa","Financials","Transaction Processing"),
    ("VST","Vistra","Utilities","Independent Power"),("VMC","Vulcan Materials","Materials","Construction Materials"),
    ("WAB","Wabtec","Industrials","Construction Machinery"),("WBA","Walgreens","Consumer Staples","Drug Retail"),
    ("WMT","Walmart","Consumer Staples","Hypermarkets"),("DIS","Walt Disney","Communication Services","Movies & Entertainment"),
    ("WBD","Warner Bros Discovery","Communication Services","Movies & Entertainment"),("WM","Waste Management","Industrials","Environmental Services"),
    ("WAT","Waters","Health Care","Life Sciences Tools"),("WEC","WEC Energy","Utilities","Multi-Utilities"),
    ("WFC","Wells Fargo","Financials","Banks"),("WELL","Welltower","Real Estate","Health Care REITs"),
    ("WST","West Pharma","Health Care","Health Care Equipment"),("WDC","Western Digital","Information Technology","Technology Hardware"),
    ("WY","Weyerhaeuser","Real Estate","Specialized REITs"),("WSM","Williams-Sonoma","Consumer Discretionary","Home Furnishings"),
    ("WMB","Williams","Energy","Oil & Gas Storage"),("WTW","WTW","Financials","Insurance Brokers"),
    ("WDAY","Workday","Information Technology","Application Software"),("WYNN","Wynn Resorts","Consumer Discretionary","Casinos"),
    ("XEL","Xcel Energy","Utilities","Electric Utilities"),("XYL","Xylem","Industrials","Industrial Machinery"),
    ("YUM","Yum! Brands","Consumer Discretionary","Restaurants"),("ZBRA","Zebra","Information Technology","Electronic Equipment"),
    ("ZBH","Zimmer Biomet","Health Care","Health Care Equipment"),("ZTS","Zoetis","Health Care","Pharmaceuticals"),
]

STATIC_SP400: list[tuple[str, str, str, str]] = [
    ("AAP","Advance Auto Parts","Consumer Discretionary","Automotive Retail"),
    ("AGCO","AGCO","Industrials","Farm Machinery"),
    ("ALSN","Allison Transmission","Industrials","Construction Machinery"),
    ("ALK","Alaska Air","Industrials","Airlines"),
    ("ALV","Autoliv","Consumer Discretionary","Automotive Parts"),
    ("AM","Antero Midstream","Energy","Oil & Gas Storage"),
    ("AMG","Affiliated Managers","Financials","Asset Management"),
    ("APG","APi Group","Industrials","Construction & Engineering"),
    ("AR","Antero Resources","Energy","Oil & Gas E&P"),
    ("ARW","Arrow Electronics","Information Technology","Technology Distributors"),
    ("ASH","Ashland","Materials","Specialty Chemicals"),
    ("ATR","AptarGroup","Materials","Containers & Packaging"),
    ("AYI","Acuity Brands","Industrials","Electrical Components"),
    ("BC","Brunswick","Consumer Discretionary","Leisure Products"),
    ("BCO","Brink's","Industrials","Diversified Support Services"),
    ("BERY","Berry Global","Materials","Containers & Packaging"),
    ("BFAM","Bright Horizons","Consumer Discretionary","Education Services"),
    ("BJ","BJ's Wholesale","Consumer Staples","Hypermarkets"),
    ("BLD","TopBuild","Industrials","Building Products"),
    ("BMI","Badger Meter","Information Technology","Electronic Equipment"),
    ("BWXT","BWX Technologies","Industrials","Aerospace & Defense"),
    ("BYD","Boyd Gaming","Consumer Discretionary","Casinos"),
    ("CACI","CACI International","Industrials","IT Services"),
    ("CASY","Casey's","Consumer Staples","Food Retail"),
    ("CBT","Cabot","Materials","Specialty Chemicals"),
    ("CC","Chemours","Materials","Specialty Chemicals"),
    ("CFR","Cullen/Frost","Financials","Banks"),
    ("CGNX","Cognex","Information Technology","Electronic Equipment"),
    ("CHE","Chemed","Health Care","Health Care Services"),
    ("CHX","ChampionX","Energy","Oil Services"),
    ("CIEN","Ciena","Information Technology","Communications Equipment"),
    ("CLF","Cleveland-Cliffs","Materials","Steel"),
    ("CMC","Commercial Metals","Materials","Steel"),
    ("CNH","CNH Industrial","Industrials","Farm Machinery"),
    ("CNX","CNX Resources","Energy","Oil & Gas E&P"),
    ("COKE","Coca-Cola Consolidated","Consumer Staples","Beverages"),
    ("COLM","Columbia Sportswear","Consumer Discretionary","Apparel"),
    ("CR","Crane","Industrials","Industrial Machinery"),
    ("CROX","Crocs","Consumer Discretionary","Footwear"),
    ("CRUS","Cirrus Logic","Information Technology","Semiconductors"),
    ("CUZ","Cousins Properties","Real Estate","Office REITs"),
    ("CW","Curtiss-Wright","Industrials","Aerospace & Defense"),
    ("DAR","Darling Ingredients","Consumer Staples","Agricultural Products"),
    ("DCI","Donaldson","Industrials","Industrial Machinery"),
    ("DBX","Dropbox","Information Technology","Application Software"),
    ("DKS","Dick's Sporting Goods","Consumer Discretionary","Specialty Retail"),
    ("DLB","Dolby","Information Technology","Electronic Equipment"),
    ("DTM","DT Midstream","Energy","Oil & Gas Storage"),
    ("DUOL","Duolingo","Communication Services","Interactive Media"),
    ("DV","DoubleVerify","Information Technology","Application Software"),
    ("EEFT","Euronet","Financials","Data Processing"),
    ("EGP","EastGroup Properties","Real Estate","Industrial REITs"),
    ("EHC","Encompass Health","Health Care","Health Care Facilities"),
    ("ELS","Equity LifeStyle","Real Estate","Residential REITs"),
    ("EME","EMCOR","Industrials","Construction & Engineering"),
    ("ENS","EnerSys","Industrials","Electrical Components"),
    ("ENV","Envestnet","Information Technology","Application Software"),
    ("EPR","EPR Properties","Real Estate","Specialized REITs"),
    ("EXEL","Exelixis","Health Care","Biotechnology"),
    ("EXP","Eagle Materials","Materials","Construction Materials"),
    ("FAF","First American Financial","Financials","Insurance"),
    ("FCN","FTI Consulting","Industrials","Research & Consulting"),
    ("FFIN","First Financial Bankshares","Financials","Banks"),
    ("FIVE","Five Below","Consumer Discretionary","Specialty Retail"),
    ("FIX","Comfort Systems","Industrials","Construction & Engineering"),
    ("FLO","Flowers Foods","Consumer Staples","Packaged Foods"),
    ("FLR","Fluor","Industrials","Construction & Engineering"),
    ("FN","Fabrinet","Information Technology","Electronic Manufacturing"),
    ("FNB","F.N.B. Corp","Financials","Banks"),
    ("FR","First Industrial","Real Estate","Industrial REITs"),
    ("FYBR","Frontier Communications","Communication Services","Telecom"),
    ("GATX","GATX","Industrials","Trading & Distribution"),
    ("GBCI","Glacier Bancorp","Financials","Banks"),
    ("GGG","Graco","Industrials","Industrial Machinery"),
    ("GLPI","Gaming and Leisure","Real Estate","Specialized REITs"),
    ("GME","GameStop","Consumer Discretionary","Specialty Retail"),
    ("GNTX","Gentex","Consumer Discretionary","Automotive Parts"),
    ("GPK","Graphic Packaging","Materials","Paper Packaging"),
    ("GPS","Gap","Consumer Discretionary","Apparel Retail"),
    ("GT","Goodyear","Consumer Discretionary","Automotive Parts"),
    ("GTLS","Chart Industries","Industrials","Industrial Machinery"),
    ("HALO","Halozyme","Health Care","Biotechnology"),
    ("HOG","Harley-Davidson","Consumer Discretionary","Motorcycle"),
    ("HOMB","Home BancShares","Financials","Banks"),
    ("HRB","H&R Block","Industrials","Diversified Support Services"),
    ("HSY","Hershey","Consumer Staples","Packaged Foods"),
    ("ICUI","ICU Medical","Health Care","Health Care Equipment"),
    ("IDCC","InterDigital","Information Technology","Electronic Equipment"),
    ("INGR","Ingredion","Consumer Staples","Agricultural Products"),
    ("ITT","ITT","Industrials","Industrial Machinery"),
    ("JEF","Jefferies","Financials","Capital Markets"),
    ("JLL","Jones Lang LaSalle","Real Estate","Real Estate Services"),
    ("JWN","Nordstrom","Consumer Discretionary","Apparel Retail"),
    ("KBR","KBR","Industrials","Construction & Engineering"),
    ("KD","Kyndryl","Information Technology","IT Services"),
    ("KMPR","Kemper","Financials","Insurance"),
    ("KNX","Knight-Swift","Industrials","Trucking"),
    ("KRG","Kite Realty","Real Estate","Retail REITs"),
    ("KSS","Kohl's","Consumer Discretionary","Apparel Retail"),
    ("LAMR","Lamar Advertising","Real Estate","Specialized REITs"),
    ("LAZ","Lazard","Financials","Capital Markets"),
    ("LECO","Lincoln Electric","Industrials","Industrial Machinery"),
    ("LITE","Lumentum","Information Technology","Communications Equipment"),
    ("LIVN","LivaNova","Health Care","Health Care Equipment"),
    ("LNC","Lincoln National","Financials","Insurance"),
    ("LNW","Light & Wonder","Consumer Discretionary","Casinos"),
    ("LOPE","Grand Canyon Education","Consumer Discretionary","Education Services"),
    ("LSTR","Landstar","Industrials","Trucking"),
    ("M","Macy's","Consumer Discretionary","Apparel Retail"),
    ("MAN","ManpowerGroup","Industrials","Human Resources"),
    ("MASI","Masimo","Health Care","Health Care Equipment"),
    ("MAT","Mattel","Consumer Discretionary","Leisure Products"),
    ("MIDD","Middleby","Industrials","Industrial Machinery"),
    ("MKSI","MKS Instruments","Information Technology","Semiconductor Equipment"),
    ("MLI","Mueller Industries","Industrials","Industrial Machinery"),
    ("MORN","Morningstar","Financials","Financial Data"),
    ("MSA","MSA Safety","Industrials","Industrial Machinery"),
    ("MSM","MSC Industrial","Industrials","Trading & Distribution"),
    ("MTG","MGIC Investment","Financials","Insurance"),
    ("MTN","Vail Resorts","Consumer Discretionary","Hotels Resorts & Cruise Lines"),
    ("MTSI","MACOM","Information Technology","Semiconductors"),
    ("MTZ","MasTec","Industrials","Construction & Engineering"),
    ("MUR","Murphy Oil","Energy","Oil & Gas E&P"),
    ("NBIX","Neurocrine","Health Care","Biotechnology"),
    ("NEU","NewMarket","Materials","Specialty Chemicals"),
    ("NFG","National Fuel Gas","Utilities","Gas Utilities"),
    ("NJR","New Jersey Resources","Utilities","Gas Utilities"),
    ("NLY","Annaly Capital","Financials","Mortgage REITs"),
    ("NOG","Northern Oil","Energy","Oil & Gas E&P"),
    ("NOV","NOV","Energy","Oil Services"),
    ("NSP","Insperity","Industrials","Human Resources"),
    ("NWE","NorthWestern Energy","Utilities","Electric Utilities"),
    ("NXST","Nexstar","Communication Services","Broadcasting"),
    ("NYT","New York Times","Communication Services","Publishing"),
    ("OC","Owens Corning","Industrials","Building Products"),
    ("OGE","OGE Energy","Utilities","Electric Utilities"),
    ("OGS","ONE Gas","Utilities","Gas Utilities"),
    ("OHI","Omega Healthcare","Real Estate","Health Care REITs"),
    ("OLED","Universal Display","Information Technology","Semiconductors"),
    ("OLN","Olin","Materials","Commodity Chemicals"),
    ("OLLI","Ollie's Bargain","Consumer Discretionary","Variety Stores"),
    ("ONB","Old National","Financials","Banks"),
    ("ORI","Old Republic","Financials","Insurance"),
    ("OSK","Oshkosh","Industrials","Construction Machinery"),
    ("OZK","Bank OZK","Financials","Banks"),
    ("PB","Prosperity Bancshares","Financials","Banks"),
    ("PBF","PBF Energy","Energy","Oil & Gas Refining"),
    ("PBH","Prestige Consumer","Consumer Staples","Personal Products"),
    ("PEN","Penumbra","Health Care","Health Care Equipment"),
    ("PII","Polaris","Consumer Discretionary","Leisure Products"),
    ("PIPR","Piper Sandler","Financials","Capital Markets"),
    ("PNFP","Pinnacle Financial","Financials","Banks"),
    ("POR","Portland General","Utilities","Electric Utilities"),
    ("POST","Post Holdings","Consumer Staples","Packaged Foods"),
    ("PPC","Pilgrim's Pride","Consumer Staples","Packaged Foods"),
    ("PRGO","Perrigo","Consumer Staples","Personal Products"),
    ("PRI","Primerica","Financials","Insurance"),
    ("PSN","Parsons","Industrials","Construction & Engineering"),
    ("PVH","PVH","Consumer Discretionary","Apparel"),
    ("R","Ryder","Industrials","Trucking"),
    ("RBA","RB Global","Industrials","Diversified Support Services"),
    ("RBC","RBC Bearings","Industrials","Industrial Machinery"),
    ("RDN","Radian","Financials","Insurance"),
    ("RGA","Reinsurance Group","Financials","Reinsurance"),
    ("RGEN","Repligen","Health Care","Life Sciences Tools"),
    ("RGLD","Royal Gold","Materials","Gold"),
    ("RH","RH","Consumer Discretionary","Specialty Retail"),
    ("RHI","Robert Half","Industrials","Human Resources"),
    ("RLI","RLI","Financials","Insurance"),
    ("RNR","RenaissanceRe","Financials","Reinsurance"),
    ("RPM","RPM","Materials","Specialty Chemicals"),
    ("RRX","Regal Rexnord","Industrials","Electrical Components"),
    ("RS","Reliance Steel","Materials","Steel"),
    ("RYAN","Ryan Specialty","Financials","Insurance Brokers"),
    ("SAIA","Saia","Industrials","Trucking"),
    ("SAIC","SAIC","Industrials","IT Services"),
    ("SCI","Service Corp","Consumer Discretionary","Specialized Consumer Services"),
    ("SCL","Stepan","Materials","Specialty Chemicals"),
    ("SEE","Sealed Air","Materials","Containers & Packaging"),
    ("SEIC","SEI Investments","Financials","Asset Management"),
    ("SF","Stifel","Financials","Capital Markets"),
    ("SFM","Sprouts Farmers","Consumer Staples","Food Retail"),
    ("SIG","Signet Jewelers","Consumer Discretionary","Specialty Retail"),
    ("SIGI","Selective Insurance","Financials","Insurance"),
    ("SLAB","Silicon Labs","Information Technology","Semiconductors"),
    ("SLGN","Silgan","Materials","Containers & Packaging"),
    ("SLM","SLM","Financials","Consumer Finance"),
    ("SM","SM Energy","Energy","Oil & Gas E&P"),
    ("SNX","TD SYNNEX","Information Technology","Technology Distributors"),
    ("SON","Sonoco","Materials","Containers & Packaging"),
    ("SR","Spire","Utilities","Gas Utilities"),
    ("SRPT","Sarepta","Health Care","Biotechnology"),
    ("SSB","SouthState","Financials","Banks"),
    ("SSD","Simpson","Industrials","Building Products"),
    ("STAG","STAG Industrial","Real Estate","Industrial REITs"),
    ("STAA","STAAR Surgical","Health Care","Health Care Equipment"),
    ("STN","Stantec","Industrials","Construction & Engineering"),
    ("STWD","Starwood Property","Financials","Mortgage REITs"),
    ("SUM","Summit Materials","Materials","Construction Materials"),
    ("SWX","Southwest Gas","Utilities","Gas Utilities"),
    ("SXT","Sensient","Materials","Specialty Chemicals"),
    ("TCBI","Texas Capital","Financials","Banks"),
    ("TDC","Teradata","Information Technology","Application Software"),
    ("TEX","Terex","Industrials","Construction Machinery"),
    ("TFX","Teleflex","Health Care","Health Care Equipment"),
    ("THC","Tenet Healthcare","Health Care","Health Care Facilities"),
    ("THG","Hanover Insurance","Financials","Insurance"),
    ("THO","Thor Industries","Consumer Discretionary","Leisure Products"),
    ("TKR","Timken","Industrials","Industrial Machinery"),
    ("TMHC","Taylor Morrison","Consumer Discretionary","Homebuilding"),
    ("TNL","Travel + Leisure","Consumer Discretionary","Hotels Resorts & Cruise Lines"),
    ("TOL","Toll Brothers","Consumer Discretionary","Homebuilding"),
    ("TPL","Texas Pacific Land","Energy","Oil & Gas E&P"),
    ("TPX","Tempur Sealy","Consumer Discretionary","Home Furnishings"),
    ("TREX","Trex","Industrials","Building Products"),
    ("TRN","Trinity Industries","Industrials","Construction Machinery"),
    ("TTC","Toro","Industrials","Industrial Machinery"),
    ("UFPI","UFP Industries","Industrials","Building Products"),
    ("UGI","UGI","Utilities","Gas Utilities"),
    ("UNM","Unum","Financials","Insurance"),
    ("USFD","US Foods","Consumer Staples","Food Distributors"),
    ("UTHR","United Therapeutics","Health Care","Biotechnology"),
    ("VFC","V.F.","Consumer Discretionary","Apparel"),
    ("VLY","Valley National","Financials","Banks"),
    ("VMI","Valmont","Industrials","Construction & Engineering"),
    ("VNO","Vornado","Real Estate","Office REITs"),
    ("VNT","Vontier","Information Technology","Electronic Equipment"),
    ("VVV","Valvoline","Consumer Discretionary","Specialty Retail"),
    ("WAL","Western Alliance","Financials","Banks"),
    ("WCC","WESCO","Industrials","Trading & Distribution"),
    ("WEN","Wendy's","Consumer Discretionary","Restaurants"),
    ("WERN","Werner","Industrials","Trucking"),
    ("WEX","WEX","Financials","Data Processing"),
    ("WH","Wyndham","Consumer Discretionary","Hotels Resorts & Cruise Lines"),
    ("WHR","Whirlpool","Consumer Discretionary","Household Appliances"),
    ("WMG","Warner Music","Communication Services","Movies & Entertainment"),
    ("WMS","Advanced Drainage","Industrials","Building Products"),
    ("WOLF","Wolfspeed","Information Technology","Semiconductors"),
    ("WPC","W. P. Carey","Real Estate","Diversified REITs"),
    ("WSC","WillScot","Industrials","Trading & Distribution"),
    ("WTRG","Essential Utilities","Utilities","Water Utilities"),
    ("WTS","Watts Water","Industrials","Building Products"),
    ("X","US Steel","Materials","Steel"),
    ("XPO","XPO","Industrials","Trucking"),
    ("XRAY","Dentsply Sirona","Health Care","Health Care Equipment"),
    ("YETI","YETI","Consumer Discretionary","Leisure Products"),
    ("ZD","Ziff Davis","Communication Services","Interactive Media"),
    ("ZION","Zions","Financials","Banks"),
]


if __name__ == "__main__":
    n = load_universe()
    print(f"Universe loaded: {n} securities")
