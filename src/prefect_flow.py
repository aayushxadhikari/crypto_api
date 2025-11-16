from prefect import flow, task
import json
from pathlib import Path
import pandas as pd
from datetime import datetime, timezone
import requests

BINANCE_URL = "https://api.binance.com/api/v3/ticker/price"
QUOTE_CURRENCY = "USDT" 


def _mock_rows(symbols: list[str], now: str) -> list[dict]:
    """Fallback data if API is unreachable."""
    default_prices = {
        "BTC": 68000.0,
        "ETH": 3550.0,
        "SOL": 180.0,
    }

    rows: list[dict] = []
    for sym in symbols:
        s = sym.upper()
        rows.append(
            {
                "symbol": s,
                "name": s,
                "price_usd": float(default_prices.get(s, 100.0)),
                "market_cap_usd": None,
                "volume_24h": None,
                "rank": None,
                "source_ts": now,
            }
        )
    return rows


@task(retries=0)  # handled fallback on own
def extract(symbols: list[str], use_mock_on_failure: bool = True) -> list[dict]:
    """
    Extract crypto prices from Binance public API.

    For each symbol like 'BTC', we query the pair 'BTCUSDT'.
    If the API is unreachable, we fall back to mock data so the pipeline still runs.
    """
    now = datetime.now(timezone.utc).isoformat()
    rows: list[dict] = []

    try:
        for sym in symbols:
            pair = f"{sym.upper()}{QUOTE_CURRENCY}" 
            params = {"symbol": pair}

            resp = requests.get(BINANCE_URL, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()  # {"symbol": "BTCUSDT", "price": "xxxxx.xx"}

            rows.append(
                {
                    "symbol": sym.upper(),
                    "name": sym.upper(),          
                    "price_usd": float(data["price"]),
                    "market_cap_usd": None,      
                    "volume_24h": None,           
                    "rank": None,                 
                    "source_ts": now,
                }
            )

        return rows

    except Exception as e:
        print(f"[extract] ERROR calling Binance: {e}")
        if not use_mock_on_failure:
            raise
        print("[extract] Falling back to mock data so the pipeline can continue.")
        return _mock_rows(symbols, now)


@task
def land(raw_rows: list[dict]) -> str:
    ts = datetime.now(timezone.utc).isoformat()
    landing = Path("landing_raw") / f"prefect_{ts.replace(':', '_')}.ndjson"
    landing.parent.mkdir(parents=True, exist_ok=True)

    with open(landing, "w") as f:
        for r in raw_rows:
            f.write(json.dumps(r) + "\n")

    return ts


@task
def transform(_ts: str) -> pd.DataFrame:
    latest = sorted(Path("landing_raw").glob("prefect_*.ndjson"))[-1]
    with open(latest) as f:
        rows = [json.loads(line) for line in f]
    return pd.DataFrame(rows)


@task
def load(df: pd.DataFrame):
    print(f"Loaded {len(df)} rows.")
    print(df)


@flow(name="crypto-api-warehouse")
def main(symbols: list[str] = ["BTC", "ETH"]):
    rows = extract(symbols)
    ts = land(rows)
    df = transform(ts)
    load(df)


if __name__ == "__main__":
    main()
