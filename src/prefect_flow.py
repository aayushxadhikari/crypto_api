from prefect import flow, task
import json
from pathlib import Path
import pandas as pd
from datetime import datetime, timezone


@task(retries=3, retry_delay_seconds=5)
def extract(symbols: list[str]) -> list[dict]:
    now = datetime.now(timezone.utc).isoformat()
    # Mock data for now – in real life you'd call an API here
    return [
        {
            "symbol": "BTC",
            "name": "Bitcoin",
            "price_usd": 68000.0,
            "market_cap_usd": 1.34e12,
            "volume_24h": 2.3e10,
            "rank": 1,
            "source_ts": now,
        },
        {
            "symbol": "ETH",
            "name": "Ethereum",
            "price_usd": 3550.0,
            "market_cap_usd": 4.2e11,
            "volume_24h": 1.1e10,
            "rank": 2,
            "source_ts": now,
        },
    ]


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
    # placeholder to call db loaders
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
