# src/news_collector.py

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
import time
import re
import os
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
import threading
from typing import Dict, List, Tuple, Optional, Any, Union
from urllib.parse import quote_plus
import difflib

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv, find_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

# ───────────────── 공통 유틸 (KST 로깅/경로) ─────────────────
from utils import (
    setup_logging,
    OUTPUT_DIR,
    find_latest_file,
    is_us_market,
    norm_ticker,
    parse_pipeline_artifact_stem,
    format_pipeline_artifact,
    load_config,
)

# ───────────────── 로깅 설정 ─────────────────
setup_logging()
logger = logging.getLogger("news_collector")

# ───────────────── .env 로딩 (고정 경로 + 폴백) ─────────────────
def load_env_with_fallback() -> str:
    candidates = [
        Path("/app/config/.env"),
        Path(__file__).resolve().parents[1] / "config" / ".env",
        Path(__file__).resolve().parent / "config" / ".env",
        Path(__file__).resolve().parent / ".env",
        Path.cwd() / "config" / ".env",
        Path.cwd() / ".env",
    ]
    loaded = ""
    for p in candidates:
        try:
            if p.is_file():
                if load_dotenv(dotenv_path=p, override=False):
                    loaded = str(p)
                    break
        except Exception:
            continue
    if not loaded:
        try:
            found = find_dotenv(usecwd=True)
            if found:
                load_dotenv(found, override=False)
                loaded = found
        except Exception:
            pass
    logger.info(f".env loaded from: {loaded if loaded else 'None'}")
    return loaded

_ = load_env_with_fallback()
NAVER_ID = os.getenv("NAVER_CLIENT_ID", "").strip()
NAVER_SECRET = os.getenv("NAVER_CLIENT_SECRET", "").strip()
DEDUPE_THRESHOLD = float(os.getenv("NEWS_TITLE_SIM_THRESHOLD", "0.85"))

_SCRAPE_LOCK = threading.Lock()
_LAST_SCRAPE_TS = 0.0

_DEFAULT_NEWS_PARAMS: Dict[str, Any] = {
    "days": 14,
    "articles_per_stock": 5,
    "scrape_body_us": True,
    "max_body_chars_per_article": 800,
    "max_gpt_text_chars": 1500,
    "dedupe_title_threshold": 0.85,
    "us_query_mode": "ticker_and_name",
    "scrape_rps": 2.0,
    "include_links_in_gpt_text": False,
}


def _load_news_params() -> Dict[str, Any]:
    """config.json → news_params (런타임 reload)."""
    out = dict(_DEFAULT_NEWS_PARAMS)
    try:
        cfg = load_config() or {}
        raw = cfg.get("news_params") or {}
        if isinstance(raw, dict):
            for k in out:
                if k in raw and raw[k] is not None:
                    out[k] = raw[k]
    except Exception as e:
        logger.debug("news_params 로드 실패, 기본값 사용: %s", e)
    out["days"] = int(out["days"])
    out["articles_per_stock"] = int(out["articles_per_stock"])
    out["max_body_chars_per_article"] = int(out["max_body_chars_per_article"])
    out["max_gpt_text_chars"] = int(out["max_gpt_text_chars"])
    out["dedupe_title_threshold"] = float(out["dedupe_title_threshold"])
    out["scrape_rps"] = float(out["scrape_rps"])
    out["scrape_body_us"] = bool(out["scrape_body_us"])
    out["include_links_in_gpt_text"] = bool(out["include_links_in_gpt_text"])
    out["us_query_mode"] = str(out["us_query_mode"] or "ticker_and_name")
    return out


def _rate_limit_scrape(rps: float) -> None:
    global _LAST_SCRAPE_TS
    if rps <= 0:
        return
    interval = 1.0 / rps
    with _SCRAPE_LOCK:
        now = time.monotonic()
        wait = interval - (now - _LAST_SCRAPE_TS)
        if wait > 0:
            time.sleep(wait)
        _LAST_SCRAPE_TS = time.monotonic()


if not is_us_market(os.getenv("MARKET", "SP500")):
    if not (NAVER_ID and NAVER_SECRET):
        logger.warning(
            "NAVER API 키가 비었습니다. (/app/config/.env 확인) "
            "예: NAVER_CLIENT_ID=xxx, NAVER_CLIENT_SECRET=yyy"
        )

# ─────────────────────── 유틸 함수 ───────────────────────
def _clean_text(text: str) -> str:
    text = re.sub(r"<.*?>", " ", text)
    text = text.replace("&quot;", '"').replace("&apos;", "'").replace("&amp;", "&")
    return " ".join(text.split())

def _normalize_title(text: str) -> str:
    if not text:
        return ""
    t = _clean_text(text)
    t = re.sub(r"[\(\[\{（\[｛].*?[\)\]\}）\]｝]", " ", t)
    t = re.sub(r"\s*[-–—]\s*[^-–—]{0,20}$", " ", t)
    t = " ".join(t.split())
    return t

def _title_similarity(a: str, b: str) -> float:
    return difflib.SequenceMatcher(None, a, b).ratio()

def _dedupe_items_by_title(items: List[Dict], threshold: float = DEDUPE_THRESHOLD) -> List[Dict]:
    selected: List[Dict] = []
    norm_titles: List[str] = []
    for it in items:
        title = _normalize_title(it.get("title", "") or "")
        if not title:
            selected.append(it)
            norm_titles.append(title)
            continue
        dup = any(prev and _title_similarity(title, prev) >= threshold for prev in norm_titles)
        if not dup:
            selected.append(it)
            norm_titles.append(title)
    return selected

# ───────────────── NAVER API / 스크레이핑 ─────────────────
def _fetch_naver_news_api(keyword: str, num_articles: int) -> List[Dict]:
    if not (NAVER_ID and NAVER_SECRET):
        return []
    url = "https://openapi.naver.com/v1/search/news.json"
    headers = {"X-Naver-Client-Id": NAVER_ID, "X-Naver-Client-Secret": NAVER_SECRET}
    params = {"query": keyword, "display": num_articles, "sort": "date"}
    r = requests.get(url, headers=headers, params=params, timeout=10)
    r.raise_for_status()
    data = r.json()
    return data.get("items", []) if isinstance(data, dict) else []

def _scrape_article_content(url: str) -> str:
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        r.encoding = r.apparent_encoding
        soup = BeautifulSoup(r.text, "html.parser")

        selectors = [
            "#articleBodyContents",
            "div.news_end",
            "#articletxt",
            ".article_body",
            "[itemprop='articleBody']",
            "#article-view-content-div",
            "article",
        ]
        container = next((soup.select_one(s) for s in selectors if soup.select_one(s)), None)
        if not container:
            return _clean_text(soup.get_text())

        for unwanted_tag in container.select("script, style, .ad, .aside, .related-news, figure, .promotion"):
            unwanted_tag.decompose()

        return _clean_text(container.get_text())
    except Exception as e:
        logger.warning(f"본문 스크레이핑 실패({url}): {e}")
        return "본문을 가져오지 못했습니다."


def _extract_publisher_from_title(title: str) -> str:
    if not title:
        return ""
    parts = [p.strip() for p in re.split(r"\s[-–—]\s+", title) if p.strip()]
    if len(parts) >= 2:
        return parts[-1]
    return ""


def _us_rss_search_query(name: str, ticker: str, mode: str) -> str:
    mode = (mode or "ticker_and_name").lower().strip()
    if mode == "ticker_and_name" and name:
        short = re.sub(
            r"\s+(INC\.?|CORP\.?|CORPORATION|LTD\.?|PLC\.?|CO\.?)\s*$",
            "",
            name,
            flags=re.I,
        ).strip()
        if short:
            return quote_plus(f'"{short}" {ticker} stock')
    return quote_plus(f"{ticker} stock")


def _resolve_publisher_url(link: str) -> str:
    """Google News RSS redirect → publisher URL (가능한 경우)."""
    if not link:
        return ""
    if "news.google.com" not in link:
        return link
    try:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; NewsBot/1.0)"}
        r = requests.get(link, headers=headers, timeout=12, allow_redirects=True)
        final = (r.url or "").strip()
        if final and "news.google.com" not in final:
            return final
        if r.text:
            soup = BeautifulSoup(r.text, "html.parser")
            for tag_name, attrs in (
                ("meta", {"property": "og:url"}),
                ("link", {"rel": "canonical"}),
            ):
                tag = soup.find(tag_name, attrs)
                if tag:
                    href = (tag.get("content") or tag.get("href") or "").strip()
                    if href and "news.google.com" not in href:
                        return href
        return final or link
    except Exception as e:
        logger.debug("URL resolve 실패(%s): %s", link[:80], e)
        return link


_US_BODY_SELECTORS = [
    "[itemprop='articleBody']",
    "article .article-body",
    "article .article-content",
    ".article-body",
    ".article-content",
    ".story-body",
    ".entry-content",
    ".post-content",
    "article",
    "main",
]


def _scrape_us_article_content(url: str, max_chars: int, scrape_rps: float) -> Tuple[str, int]:
    if not url or not url.startswith("http"):
        return "", 0
    _rate_limit_scrape(scrape_rps)
    try:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; NewsBot/1.0)"}
        r = requests.get(url, headers=headers, timeout=12, allow_redirects=True)
        r.raise_for_status()
        r.encoding = r.apparent_encoding or r.encoding
        soup = BeautifulSoup(r.text, "html.parser")
        container = None
        for sel in _US_BODY_SELECTORS:
            container = soup.select_one(sel)
            if container:
                break
        if not container:
            container = soup.body
        if not container:
            return "", 0
        for unwanted in container.select(
            "script, style, nav, footer, aside, .ad, .advertisement, "
            ".related, .newsletter, figure, iframe"
        ):
            unwanted.decompose()
        text = _clean_text(container.get_text())
        if max_chars > 0 and len(text) > max_chars:
            text = text[:max_chars].rsplit(" ", 1)[0] + "…"
        return text, len(text)
    except Exception as e:
        logger.debug("US 본문 스크래핑 실패(%s): %s", url[:80], e)
        return "", 0


def _build_gpt_news_text(
    articles: List[Dict[str, Any]],
    max_chars: int,
    include_links: bool,
) -> str:
    blocks: List[str] = []
    for art in articles:
        parts = [f"Title: {art.get('title', '')}"]
        summary = (art.get("summary") or "").strip()
        body = (art.get("body") or "").strip()
        if summary and summary.lower() != (art.get("title") or "").lower():
            parts.append(f"Summary: {summary}")
        if body:
            parts.append(f"Body: {body}")
        if include_links and art.get("url"):
            parts.append(f"Link: {art['url']}")
        blocks.append("\n".join(parts))
    combined = "\n\n---\n\n".join(blocks)
    if max_chars > 0 and len(combined) > max_chars:
        combined = combined[:max_chars].rsplit("\n", 1)[0] + "\n…"
    return combined


def _news_payload(
    status: str,
    text: str,
    articles: Optional[List[Dict[str, Any]]] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return {
        "status": status,
        "text": text or "",
        "articles": articles or [],
        "meta": meta or {},
    }


def _parse_pubdate(pub: str) -> datetime:
    dt = datetime.strptime(pub, "%a, %d %b %Y %H:%M:%S %z")
    return dt.astimezone(timezone.utc)

def _market_tag() -> str:
    return os.getenv("MARKET", "SP500")


def _normalize_stock_dict(d: Dict) -> Dict:
    out = dict(d)
    mkt = _market_tag()
    if not out.get("Ticker"):
        if out.get("Code"):
            out["Ticker"] = norm_ticker(out["Code"], mkt)
        elif out.get("ticker"):
            out["Ticker"] = norm_ticker(out["ticker"], mkt)
    else:
        out["Ticker"] = norm_ticker(out["Ticker"], mkt)
    if not out.get("Name"):
        for k in ["Name", "종목명", "name"]:
            if d.get(k):
                out["Name"] = str(d[k])
                break
    return out

def _fetch_google_news_rss(
    ticker: str,
    limit: int = 20,
    *,
    name: str = "",
    query_mode: str = "ticker_and_name",
) -> List[Dict]:
    """Google News RSS (US)."""
    q = _us_rss_search_query(name, ticker, query_mode)
    url = f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
    try:
        r = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200:
            return []
        root = ET.fromstring(r.content)
        items = []
        for item in root.findall(".//item")[:limit]:
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            pub = (item.findtext("pubDate") or "").strip()
            desc = (item.findtext("description") or "").strip()
            items.append({"title": title, "link": link, "pubDate": pub, "description": desc})
        return items
    except Exception as e:
        logger.warning("Google RSS 실패(%s): %s", ticker, e)
        return []


def _parse_rss_pubdate(pub: str) -> Optional[datetime]:
    if not pub:
        return None
    try:
        return parsedate_to_datetime(pub).astimezone(timezone.utc)
    except Exception:
        return None


def _fetch_news_for_single_stock_us(
    stock: Dict,
    cutoff_utc: datetime,
    num_articles: int,
    params: Optional[Dict[str, Any]] = None,
) -> Tuple[str, Dict[str, Any]]:
    stock = _normalize_stock_dict(stock)
    name, ticker = stock.get("Name"), stock.get("Ticker")
    p = params or _load_news_params()
    if not ticker:
        return "", _news_payload("NO_NEWS", "[NO_NEWS] 종목 정보 누락")

    t0 = time.perf_counter()
    try:
        items = _fetch_google_news_rss(
            str(ticker),
            limit=100,
            name=str(name or ""),
            query_mode=str(p.get("us_query_mode", "ticker_and_name")),
        )
        if not items:
            return str(ticker), _news_payload("NO_NEWS", "[NO_NEWS] Google RSS 결과 없음")

        recent: List[Dict] = []
        skipped_no_date = 0
        for it in items:
            pub = _parse_rss_pubdate(it.get("pubDate", ""))
            if pub is None:
                skipped_no_date += 1
                continue
            if pub >= cutoff_utc:
                recent.append(it)

        if not recent:
            return str(ticker), _news_payload("NO_NEWS", "[NO_NEWS] 최근 기간 내 뉴스 없음")

        dedupe_thr = float(p.get("dedupe_title_threshold", DEDUPE_THRESHOLD))
        recent = _dedupe_items_by_title(recent, threshold=dedupe_thr)

        scrape_body = bool(p.get("scrape_body_us", True))
        max_body = int(p.get("max_body_chars_per_article", 800))
        scrape_rps = float(p.get("scrape_rps", 2.0))
        max_gpt = int(p.get("max_gpt_text_chars", 1500))
        include_links = bool(p.get("include_links_in_gpt_text", False))

        article_records: List[Dict[str, Any]] = []
        bodies_ok = 0
        for it in recent[:num_articles]:
            title = _clean_text(it.get("title", ""))
            desc = _clean_text(it.get("description", ""))
            rss_link = it.get("link", "")
            pub_dt = _parse_rss_pubdate(it.get("pubDate", ""))
            published = pub_dt.isoformat() if pub_dt else ""

            publisher_url = _resolve_publisher_url(rss_link) if rss_link else ""
            body, body_chars = "", 0
            if scrape_body and publisher_url:
                body, body_chars = _scrape_us_article_content(
                    publisher_url, max_body, scrape_rps
                )
                if body_chars > 0:
                    bodies_ok += 1

            article_records.append({
                "title": title,
                "summary": desc,
                "body": body,
                "url": publisher_url or rss_link,
                "rss_url": rss_link,
                "published": published,
                "source": _extract_publisher_from_title(title),
                "body_chars": body_chars,
            })

        if not article_records:
            return str(ticker), _news_payload("NO_NEWS", "[NO_NEWS] 기사 수집 실패")

        gpt_text = _build_gpt_news_text(article_records, max_gpt, include_links)
        if not gpt_text.strip():
            return str(ticker), _news_payload("NO_NEWS", "[NO_NEWS] GPT용 텍스트 생성 실패")

        if bodies_ok == 0:
            status = "PARTIAL"
        elif bodies_ok < len(article_records):
            status = "PARTIAL"
        else:
            status = "OK"

        meta = {
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "article_count": len(article_records),
            "bodies_scraped": bodies_ok,
            "text_chars": len(gpt_text),
            "skipped_no_pubdate": skipped_no_date,
            "dedupe_threshold": dedupe_thr,
            "scrape_body_us": scrape_body,
            "duration_sec": round(time.perf_counter() - t0, 2),
        }
        logger.info(
            "[%s] 뉴스 %s: articles=%d bodies=%d text_chars=%d (%.2fs)",
            ticker,
            status,
            len(article_records),
            bodies_ok,
            len(gpt_text),
            meta["duration_sec"],
        )
        return str(ticker), _news_payload(status, gpt_text, article_records, meta)

    except Exception as e:
        logger.error("US 뉴스 수집 오류 %s: %s", ticker, e)
        return str(ticker), _news_payload("ERROR", "[ERROR] 뉴스 수집 중 오류 발생")


# ───────────────── 뉴스 수집 코어 ─────────────────
def _fetch_news_for_single_stock(
    stock: Dict, cutoff_utc: datetime, num_articles: int
) -> Tuple[str, str]:
    """
    반환: (ticker, text)
    - 데이터 부족 시 text는 '[NO_NEWS] ...' 형식으로 반환
    - 오류 시 text는 '[ERROR] ...' 형식
    """
    stock = _normalize_stock_dict(stock)
    name, ticker = stock.get("Name"), stock.get("Ticker")
    if not (name and ticker):
        return (str(ticker) if ticker else ""), "[NO_NEWS] 종목 정보 누락"

    try:
        # 1) API 호출
        items = _fetch_naver_news_api(f'"{name}"', 100)
        if not items:
            return str(ticker), "[NO_NEWS] 뉴스 API 호출 결과 없음"

        # 2) 기간 필터
        recent: List[Dict] = []
        for it in items:
            try:
                pub = _parse_pubdate(it["pubDate"])
                if pub >= cutoff_utc:
                    recent.append(it)
            except Exception:
                continue
        if not recent:
            return str(ticker), "[NO_NEWS] 최근 기간 내 뉴스 없음"

        # 3) 중복 제거 (제목 유사도)
        recent = _dedupe_items_by_title(recent, threshold=DEDUPE_THRESHOLD)

        # 4) 스크래핑 및 포맷
        articles = []
        for it in recent[:num_articles]:
            raw_link = it.get("link") or ""
            link = raw_link if "naver.com" in raw_link else it.get("originallink") or raw_link
            title = _clean_text((it.get("title") or "").strip())
            desc = it.get("description")

            content = _scrape_article_content(link) if link else "원문 링크 없음"
            parts = [f"제목: {title}"]
            if desc:
                parts.append(f"요약: {_clean_text(desc)}")
            parts.extend([f"링크: {link or 'N/A'}", f"본문: {content}"])
            articles.append("\n".join(parts))

        if not articles:
            return str(ticker), "[NO_NEWS] 기사 본문 수집 실패"

        return str(ticker), "\n\n---\n\n".join(articles)

    except Exception as e:
        logger.error(f"'{name}'({ticker}) 뉴스 수집 오류: {e}")
        return str(ticker), "[ERROR] 뉴스 수집 중 오류 발생"

def fetch_news_for_stocks(
    stocks: List[Dict],
    num_articles_per_stock: Optional[int] = None,
    days: Optional[int] = None,
    max_workers: Optional[int] = None,
    news_params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Union[str, Dict[str, Any]]]:
    """
    항상 각 티커 키를 포함한 dict를 반환.
    US: 구조화 dict(status,text,articles,meta) / KR: legacy str (저장 시 구조화)
    """
    if not stocks:
        return {}

    p = dict(news_params or _load_news_params())
    if num_articles_per_stock is not None:
        p["articles_per_stock"] = int(num_articles_per_stock)
    if days is not None:
        p["days"] = int(days)

    cutoff_utc = datetime.now(timezone.utc) - timedelta(days=int(p["days"]))
    n_articles = int(p["articles_per_stock"])
    if max_workers is None:
        max_workers = max(1, min(10, len(stocks)))

    base_map: Dict[str, str] = {}
    norm_list: List[Dict] = []
    mkt = _market_tag()
    for stock in stocks:
        s = _normalize_stock_dict(stock)
        t = norm_ticker(s.get("Ticker", ""), mkt)
        if t and s.get("Name"):
            base_map[t] = "[NO_NEWS] 데이터 부족(수집 0건)"
            norm_list.append({"Name": s["Name"], "Ticker": t})

    news_cache: Dict[str, Union[str, Dict[str, Any]]] = dict(base_map)

    logger.info(
        f"{len(norm_list)}개 종목 뉴스 병렬 수집 시작... "
        f"(최근 {p['days']}일, articles={n_articles}, scrape_us={p.get('scrape_body_us')})"
    )
    if not norm_list:
        return news_cache

    us_mode = is_us_market(mkt)

    def _fetch_one(stock: Dict) -> Tuple[str, Union[str, Dict[str, Any]]]:
        if us_mode:
            return _fetch_news_for_single_stock_us(stock, cutoff_utc, n_articles, p)
        return _fetch_news_for_single_stock(stock, cutoff_utc, n_articles)

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {
            ex.submit(_fetch_one, stock): str(stock.get("Name", ""))
            for stock in norm_list
        }
        for fut in as_completed(futures):
            stock_name = futures[fut]
            try:
                ticker, payload = fut.result()
                if ticker:
                    if isinstance(payload, dict):
                        news_cache[ticker] = payload
                    else:
                        news_cache[ticker] = payload or "[NO_NEWS] 빈 본문"
            except Exception as e:
                logger.error(f"'{stock_name}' 처리 중 예외: {e}", exc_info=True)

    logger.info(f"✅ 뉴스 수집 완료 (총 {len(news_cache)}종목)")
    return news_cache

# ───────────────── 저장 전 포맷 통일 유틸 ─────────────────
def _to_structured_news_map(news_data: Dict[str, object], all_tickers: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    다양한 형태의 news_data 값을 저장 전 통일:
      - dict(status,text[,articles,meta]) → 보존
      - str 시작이 "[NO_NEWS]" → {"status":"NO_NEWS","text":...}
      - 그 외 str → {"status":"OK","text":...}
    """
    out: Dict[str, Dict[str, Any]] = {}
    for t in all_tickers:
        v = news_data.get(t, None)
        if isinstance(v, dict) and "status" in v and "text" in v:
            entry: Dict[str, Any] = {
                "status": str(v.get("status")),
                "text": str(v.get("text") or ""),
            }
            if v.get("articles") is not None:
                entry["articles"] = v.get("articles")
            if v.get("meta") is not None:
                entry["meta"] = v.get("meta")
            out[t] = entry
        elif isinstance(v, str):
            s = v.strip()
            if s.startswith("[NO_NEWS]"):
                out[t] = {"status": "NO_NEWS", "text": s, "articles": [], "meta": {}}
            elif s.startswith("[ERROR]"):
                out[t] = {"status": "ERROR", "text": s, "articles": [], "meta": {}}
            else:
                out[t] = {"status": "OK", "text": s, "articles": [], "meta": {}}
        else:
            out[t] = {"status": "NO_NEWS", "text": "", "articles": [], "meta": {}}
    return out

def run_news_collection_from_results_file(
    results_file: Path,
    num_articles_per_stock: Optional[int] = None,
    days: Optional[int] = None,
) -> None:
    t0 = time.perf_counter()
    news_params = _load_news_params()

    if not results_file.exists():
        logger.error(f"결과 파일({results_file})이 존재하지 않습니다.")
        return

    meta = parse_pipeline_artifact_stem(results_file.stem)
    fixed_date = meta.get("date") or "unknown"
    market = meta.get("market") or "UNKNOWN"
    session = meta.get("session")

    logger.info(f"결과 파일 로드 → {results_file}")
    try:
        with open(results_file, "r", encoding="utf-8") as f:
            screened_stocks = json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logger.error(f"결과 파일 읽기 오류: {e}")
        return

    if not isinstance(screened_stocks, list) or not screened_stocks:
        logger.info("종목이 없어 뉴스 수집 종료.")
        return

    stocks_for_news: List[Dict] = []
    for s in screened_stocks:
        norm = _normalize_stock_dict(s)
        if norm.get("Ticker") and norm.get("Name"):
            stocks_for_news.append({
                "Name": norm["Name"],
                "Ticker": norm_ticker(norm["Ticker"], market),
            })

    if not stocks_for_news:
        logger.warning("Ticker/Name이 유효한 종목이 없습니다.")
        return

    news_data = fetch_news_for_stocks(
        stocks_for_news,
        num_articles_per_stock=num_articles_per_stock,
        days=days,
        news_params=news_params,
    )

    # 파일 저장: 비어 있어도 NO_NEWS 기본값으로 저장되도록 보장
    if not news_data:
        news_data = {s["Ticker"]: "[NO_NEWS] 데이터 부족(수집 0건)" for s in stocks_for_news}

    out_file = results_file.parent / format_pipeline_artifact(
        "collected_news", fixed_date, market, session
    )
    logger.info(f"저장 → {out_file}")

    # ✅ 저장 직전: 모든 티커에 대해 구조화 포맷 보장 + 빈 값은 NO_NEWS 채움
    all_tickers = [s['Ticker'] for s in stocks_for_news]
    news_data_struct = _to_structured_news_map(news_data, all_tickers)

    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(news_data_struct, f, ensure_ascii=False, indent=2)

    logger.info(f"완료 (소요 {time.perf_counter() - t0:.2f}초)")

# ───────────────────────────── CLI ─────────────────────────────
if __name__ == "__main__":
    import argparse

    np = _load_news_params()
    parser = argparse.ArgumentParser(description="News Collector (compat with screener outputs)")
    parser.add_argument("--file", type=str, help="스크리너 결과 JSON 경로")
    parser.add_argument(
        "--articles",
        type=int,
        default=None,
        help=f"종목별 기사 수 (config 기본 {np['articles_per_stock']})",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help=f"최근 N일만 수집 (config 기본 {np['days']})",
    )
    args = parser.parse_args()

    if args.file:
        run_news_collection_from_results_file(
            Path(args.file), num_articles_per_stock=args.articles, days=args.days
        )
    else:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        latest = (
            find_latest_file("screener_candidates_*.json", market=os.getenv("MARKET"))
            or find_latest_file("screener_candidates_full_*.json", market=os.getenv("MARKET"))
            or find_latest_file("screener_rank_*.json", market=os.getenv("MARKET"))
            or find_latest_file("screener_rank_full_*.json", market=os.getenv("MARKET"))
        )
        if latest is None:
            logger.error("output/ 폴더에 screener_candidates_*.json 또는 screener_rank_*.json 파일이 없습니다.")
        else:
            logger.info(f"자동 선택: {latest.name}")
            run_news_collection_from_results_file(
                latest, num_articles_per_stock=args.articles, days=args.days
            )
