from __future__ import annotations

import datetime as dt
import json
import math
import os
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any

NOTION_VERSION = "2022-06-28"
NOTION_API_BASE = "https://api.notion.com/v1"
KIWOOM_BASE_URL = "https://api.kiwoom.com"

KST = dt.timezone(dt.timedelta(hours=9))


class ApiError(RuntimeError):
    pass


@dataclass
class StockCandidate:
    code: str
    name: str
    change_pct: float
    trade_value: int
    market_cap: int


@dataclass
class ConditionConfig:
    min_change_pct: float
    max_change_pct: float
    min_trade_value: int
    min_market_cap: int
    exclude_keywords: tuple[str, ...]
    max_results: int


def env(name: str, default: str | None = None) -> str | None:
    return os.getenv(name, default)


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def env_float(name: str, default: float) -> float:
    value = env(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a number") from exc


def env_int(name: str, default: int) -> int:
    value = env(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer") from exc


def parse_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return default
    try:
        return int(float(text))
    except ValueError:
        return default


def parse_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return default
    try:
        return float(text)
    except ValueError:
        return default


def load_condition_config() -> ConditionConfig:
    raw_keywords = env("EXCLUDE_KEYWORDS", "ETF,ETN,스팩,SPAC")
    exclude_keywords = tuple(
        keyword.strip().lower() for keyword in raw_keywords.split(",") if keyword.strip()
    )
    return ConditionConfig(
        min_change_pct=env_float("MIN_CHANGE_PCT", 2.0),
        max_change_pct=env_float("MAX_CHANGE_PCT", 25.0),
        min_trade_value=env_int("MIN_TRADE_VALUE", 10_000_000_000),
        min_market_cap=env_int("MIN_MARKET_CAP", 300_000_000_000),
        max_results=env_int("MAX_RESULTS", 20),
        exclude_keywords=exclude_keywords,
    )


def build_notion_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def http_json_request(
    method: str,
    url: str,
    headers: dict[str, str] | None = None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    data = None
    request_headers = headers.copy() if headers else {}
    request_headers.setdefault(
        'User-Agent',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    )
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        request_headers.setdefault("Content-Type", "application/json")

    request = urllib.request.Request(
        url=url,
        data=data,
        headers=request_headers,
        method=method,
    )

    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise ApiError(f"HTTP {exc.code} {exc.reason} @ {url}\n{body}") from exc
    except urllib.error.URLError as exc:
        raise ApiError(f"Network error @ {url}: {exc}") from exc


def request_with_retry(
    method: str,
    url: str,
    headers: dict[str, str],
    payload: dict[str, Any],
    retries: int = 3,
) -> dict[str, Any]:
    delay = 1.0
    for attempt in range(1, retries + 1):
        try:
            return http_json_request(
                method=method,
                url=url,
                headers=headers,
                payload=payload,
            )
        except ApiError as exc:
            if attempt == retries:
                raise
            if "HTTP 5" not in str(exc) and "Network error" not in str(exc):
                raise
            time.sleep(delay)
            delay *= 2
    raise RuntimeError("Unreachable")


def query_by_date_and_code(
    headers: dict[str, str],
    database_id: str,
    date_value: str,
    code_value: str,
) -> str | None:
    url = f"{NOTION_API_BASE}/databases/{database_id}/query"
    payload: dict[str, Any] = {
        "filter": {
            "and": [
                {"property": "Date", "date": {"equals": date_value}},
                {"property": "Code", "rich_text": {"equals": code_value}},
            ]
        },
        "page_size": 1,
    }
    data = request_with_retry("POST", url, headers, payload)
    results = data.get("results", [])
    if not results:
        return None
    return results[0]["id"]


def build_notion_properties(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "Name": {"title": [{"text": {"content": record["name"]}}]},
        "Date": {"date": {"start": record["date"]}},
        "Code": {"rich_text": [{"text": {"content": record["code"]}}]},
        "Score": {"number": record["score"]},
        "Reason": {"rich_text": [{"text": {"content": record["reason"]}}]},
        "Source": {"select": {"name": record["source"]}},
        "UpdatedAt": {"date": {"start": record["updated_at"]}},
    }


def upsert_page(
    headers: dict[str, str],
    database_id: str,
    record: dict[str, Any],
) -> None:
    page_id = query_by_date_and_code(
        headers=headers,
        database_id=database_id,
        date_value=record["date"],
        code_value=record["code"],
    )

    properties = build_notion_properties(record)

    if page_id:
        url = f"{NOTION_API_BASE}/pages/{page_id}"
        payload = {"properties": properties}
        request_with_retry("PATCH", url, headers, payload)
        print(f"Updated existing row: {page_id}")
    else:
        url = f"{NOTION_API_BASE}/pages"
        payload = {
            "parent": {"database_id": database_id},
            "properties": properties,
        }
        request_with_retry("POST", url, headers, payload)
        print(f"Created new row: {record['code']}")


def build_kiwoom_headers(access_token: str, api_id: str | None = None) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json;charset=UTF-8",
    }
    if api_id:
        headers["api-id"] = api_id
    return headers


def issue_kiwoom_access_token(app_key: str, app_secret: str, base_url: str) -> str:
    data = http_json_request(
        method="POST",
        url=f"{base_url}/oauth2/token",
        headers={"Content-Type": "application/json;charset=UTF-8"},
        payload={
            "grant_type": "client_credentials",
            "appkey": app_key,
            "secretkey": app_secret,
        },
    )
    token = data.get("token") or data.get("access_token")
    if not isinstance(token, str) or not token.strip():
        data_dump = json.dumps(data, ensure_ascii=False)
        raise RuntimeError(f"Kiwoom token response does not include token: {data_dump}")
    return token.strip()


def extract_kiwoom_items(data: dict[str, Any]) -> list[dict[str, Any]]:
    for key in (
        "items",
        "bid_req_upper",
        "trde_amt_upper",
        "trde_qty_upper",
        "flu_rt_upper",
    ):
        value = data.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]

    for value in data.values():
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]

    keys = ", ".join(sorted(data.keys()))
    raise RuntimeError(f"Kiwoom response must include a list payload. keys={keys}")


def to_candidate_from_custom_item(item: dict[str, Any]) -> StockCandidate | None:
    code = str(item.get("code", "")).strip()
    name = str(item.get("name", "")).strip()
    if not code or not name:
        return None
    return StockCandidate(
        code=code,
        name=name,
        change_pct=parse_float(item.get("change_pct"), 0.0),
        trade_value=parse_int(item.get("trade_value"), 0),
        market_cap=parse_int(item.get("market_cap"), 0),
    )


def to_candidate_from_rank_item(item: dict[str, Any], fallback_market_cap: int) -> StockCandidate | None:
    code = str(item.get("stk_cd") or item.get("code") or "").strip()
    if code.startswith("A") and code[1:].isdigit():
        code = code[1:]
    name = str(item.get("stk_nm") or item.get("name") or "").strip()
    if not code or not name:
        return None

    current_price = abs(parse_int(item.get("cur_prc"), 0))
    prev_change = parse_int(item.get("pred_pre"), 0)
    sign = str(item.get("pred_pre_sig") or "").strip()
    if sign in {"2", "+", "up", "UP"}:
        prev_change = abs(prev_change)
    elif sign in {"5", "-", "down", "DOWN"}:
        prev_change = -abs(prev_change)

    change_pct = parse_float(item.get("flu_rt"), math.nan)
    if math.isnan(change_pct):
        prev_close = current_price - prev_change
        change_pct = (prev_change / prev_close * 100) if prev_close > 0 else 0.0

    trade_value = parse_int(item.get("trde_amt"), 0)
    if trade_value <= 0:
        trade_qty = abs(parse_int(item.get("trde_qty"), 0))
        trade_value = current_price * trade_qty

    market_cap = parse_int(item.get("mket_tot_amt") or item.get("mrkt_tot_amt"), 0)
    if market_cap <= 0:
        market_cap = fallback_market_cap

    return StockCandidate(
        code=code,
        name=name,
        change_pct=change_pct,
        trade_value=trade_value,
        market_cap=market_cap,
    )


def fetch_kiwoom_candidates_from_custom_url(url: str, access_token: str) -> list[StockCandidate]:
    data = http_json_request(
        method="GET",
        url=url,
        headers=build_kiwoom_headers(access_token),
    )
    raw_items = data.get("items")
    if not isinstance(raw_items, list):
        raise RuntimeError("Kiwoom custom response must include items list")

    candidates: list[StockCandidate] = []
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        candidate = to_candidate_from_custom_item(item)
        if candidate:
            candidates.append(candidate)
    return candidates


def fetch_kiwoom_candidates_from_rank_api(base_url: str, access_token: str) -> list[StockCandidate]:
    url = f"{base_url}/api/dostk/rkinfo"
    api_id = env("KIWOOM_RANK_API_ID", "ka10020")
    market_types = [
        value.strip() for value in env("KIWOOM_MRKT_TP_LIST", "001,101").split(",") if value.strip()
    ]
    if not market_types:
        market_types = ["001"]

    fallback_market_cap = env_int(
        "KIWOOM_FALLBACK_MARKET_CAP",
        env_int("MIN_MARKET_CAP", 300_000_000_000),
    )

    candidates: list[StockCandidate] = []
    for market_type in market_types:
        payload = {
            "mrkt_tp": market_type,
            "sort_tp": env("KIWOOM_SORT_TP", "3"),
            "trde_qty_tp": env("KIWOOM_TRDE_QTY_TP", "00100"),
            "stk_cnd": env("KIWOOM_STK_CND", "1"),
            "crd_cnd": env("KIWOOM_CRD_CND", "0"),
            "stex_tp": env("KIWOOM_STEX_TP", "1"),
        }
        data = http_json_request(
            method="POST",
            url=url,
            headers=build_kiwoom_headers(access_token, api_id=api_id),
            payload=payload,
        )
        for item in extract_kiwoom_items(data):
            candidate = to_candidate_from_rank_item(item, fallback_market_cap)
            if candidate:
                candidates.append(candidate)

    if not candidates:
        raise RuntimeError("Kiwoom rank API returned no candidate rows")

    by_code: dict[str, StockCandidate] = {}
    for candidate in candidates:
        current = by_code.get(candidate.code)
        if current is None or candidate.trade_value > current.trade_value:
            by_code[candidate.code] = candidate
    return list(by_code.values())


def fetch_kiwoom_candidates() -> list[StockCandidate]:
    app_key = require_env("KIWOOM_APP_KEY")
    app_secret = require_env("KIWOOM_APP_SECRET")
    base_url = env("KIWOOM_BASE_URL", KIWOOM_BASE_URL).rstrip("/")

    access_token = env("KIWOOM_ACCESS_TOKEN")
    if not access_token:
        access_token = issue_kiwoom_access_token(app_key, app_secret, base_url)

    custom_url = env("KIWOOM_CANDIDATES_URL")
    if custom_url:
        return fetch_kiwoom_candidates_from_custom_url(custom_url, access_token)
    return fetch_kiwoom_candidates_from_rank_api(base_url, access_token)


def fetch_mock_candidates() -> list[StockCandidate]:
    return [
        StockCandidate("005930", "삼성전자", 2.5, 160_000_000_000, 430_000_000_000_000),
        StockCandidate("000660", "SK하이닉스", 3.4, 120_000_000_000, 120_000_000_000_000),
        StockCandidate("122630", "KODEX 레버리지 ETF", 4.1, 40_000_000_000, 5_000_000_000_000),
        StockCandidate("035720", "카카오", -1.0, 80_000_000_000, 25_000_000_000_000),
        StockCandidate("095570", "AJ네트웍스", 7.9, 8_000_000_000, 400_000_000),
    ]


def fetch_candidates() -> list[StockCandidate]:
    data_source = env("DATA_SOURCE", "mock").lower()
    if data_source == "kiwoom":
        return fetch_kiwoom_candidates()
    if data_source == "mock":
        return fetch_mock_candidates()
    raise RuntimeError("DATA_SOURCE must be one of: mock, kiwoom")


def pass_filters(stock: StockCandidate, cfg: ConditionConfig) -> bool:
    if not stock.code or not stock.name:
        return False
    if stock.change_pct < cfg.min_change_pct or stock.change_pct > cfg.max_change_pct:
        return False
    if stock.trade_value < cfg.min_trade_value:
        return False
    if stock.market_cap < cfg.min_market_cap:
        return False
    name_lower = stock.name.lower()
    return not any(keyword in name_lower for keyword in cfg.exclude_keywords)


def score_stock(stock: StockCandidate, cfg: ConditionConfig) -> int:
    trade_score = min(stock.trade_value / cfg.min_trade_value, 3.0) * 30
    change_score = min(max(stock.change_pct, 0.0), cfg.max_change_pct) / cfg.max_change_pct * 40
    cap_score = min(stock.market_cap / cfg.min_market_cap, 4.0) * 30
    return round(trade_score + change_score + cap_score)


def build_records(candidates: list[StockCandidate], cfg: ConditionConfig) -> list[dict[str, Any]]:
    now = dt.datetime.now(dt.timezone.utc).astimezone(KST)
    day = now.date().isoformat()
    timestamp = now.isoformat(timespec="seconds")

    filtered = [stock for stock in candidates if pass_filters(stock, cfg)]
    ranked = sorted(filtered, key=lambda item: score_stock(item, cfg), reverse=True)
    selected = ranked[: cfg.max_results]

    records: list[dict[str, Any]] = []
    for stock in selected:
        score = score_stock(stock, cfg)
        records.append(
            {
                "date": day,
                "code": stock.code,
                "name": stock.name,
                "score": score,
                "reason": (
                    f"등락률 {stock.change_pct:.2f}% / 거래대금 {stock.trade_value:,} / "
                    f"시총 {stock.market_cap:,}"
                ),
                "source": "kiwoom",
                "updated_at": timestamp,
            }
        )
    return records


def main() -> int:
    dry_run = env("DRY_RUN", "false").lower() == "true"
    cfg = load_condition_config()

    candidates = fetch_candidates()
    records = build_records(candidates, cfg)

    print(f"Candidates={len(candidates)} Filtered={len(records)}")
    for record in records:
        print(f"- {record['code']} {record['name']} score={record['score']}")

    if dry_run:
        print("DRY_RUN enabled: skip Notion updates")
        return 0

    notion_token = require_env("NOTION_TOKEN")
    notion_database_id = require_env("NOTION_DATABASE_ID")
    headers = build_notion_headers(notion_token)

    try:
        if not records:
            print("No stock matched conditions. Notion update skipped.")
            return 0
        for record in records:
            upsert_page(
                headers=headers,
                database_id=notion_database_id,
                record=record,
            )
    except ApiError as exc:
        print(f"API error: {exc}", file=sys.stderr)
        return 1

    print("Done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
