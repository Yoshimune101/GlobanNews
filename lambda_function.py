# lambda_function.py
import os
import json
import re
import html
import hashlib
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Tuple, Optional
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

import boto3
import feedparser
import requests


# ---------- Config ----------
APP_PREFIX = "global-news-"

S3_BUCKET = os.environ["S3_BUCKET"]

MAX_ITEMS_PER_CATEGORY = int(os.environ.get("MAX_ITEMS_PER_CATEGORY", "30"))
MAX_ITEMS_PER_FEED = int(os.environ.get("MAX_ITEMS_PER_FEED", "20"))
FETCH_TIMEOUT_SEC = int(os.environ.get("FETCH_TIMEOUT_SEC", "10"))
HTTP_USER_AGENT = os.environ.get("HTTP_USER_AGENT", f"{APP_PREFIX}bot/0.1")

BEDROCK_REGION = os.environ.get("BEDROCK_REGION", os.environ.get("AWS_REGION", "us-west-2"))
BEDROCK_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "amazon.nova-pro-v1:0")

# RSS feeds (comma-separated). If not provided, default to the curated set below.
DEFAULT_RSS_POLITICS = [
    "http://feeds.feedburner.com/prachataienglish",
    "https://api.gdeltproject.org/api/v2/doc/doc?query=thailand%20(politics%20OR%20government%20OR%20election)&mode=ArtList&format=rss&maxrecords=50&sort=HybridRel",
]
DEFAULT_RSS_ECONOMY = [
    "https://api.gdeltproject.org/api/v2/doc/doc?query=thailand%20(economy%20OR%20gdp%20OR%20inflation%20OR%20bank%20OR%20baht%20OR%20trade%20OR%20tourism)&mode=ArtList&format=rss&maxrecords=50&sort=HybridRel",
]
DEFAULT_RSS_TECH = [
    "https://api.gdeltproject.org/api/v2/doc/doc?query=thailand%20(technology%20OR%20ai%20OR%20cyber%20OR%20software%20OR%20startup%20OR%20digital)&mode=ArtList&format=rss&maxrecords=50&sort=HybridRel",
]

RSS_POLITICS = [u.strip() for u in os.environ.get("RSS_POLITICS", "").split(",") if u.strip()] or DEFAULT_RSS_POLITICS
RSS_ECONOMY = [u.strip() for u in os.environ.get("RSS_ECONOMY", "").split(",") if u.strip()] or DEFAULT_RSS_ECONOMY
RSS_TECH = [u.strip() for u in os.environ.get("RSS_TECH", "").split(",") if u.strip()] or DEFAULT_RSS_TECH

# Date boundary: Thailand time is natural for Thailand news daily cut
TH_TZ = ZoneInfo("Asia/Bangkok")


# ---------- Clients ----------
s3 = boto3.client("s3")
brt = boto3.client("bedrock-runtime", region_name=BEDROCK_REGION)


# ---------- Helpers ----------
def _clean_text(s: str) -> str:
    if not s:
        return ""
    s = html.unescape(s)
    s = re.sub(r"<[^>]+>", " ", s)  # strip HTML tags
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _hash(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


def _normalize_url(url: str) -> str:
    """
    Normalize URL to improve dedup:
    - strip common tracking query params (utm_*, fbclid, etc.)
    - keep stable parts
    """
    if not url:
        return url
    try:
        p = urlparse(url)
        q = []
        for k, v in parse_qsl(p.query, keep_blank_values=True):
            lk = k.lower()
            if lk.startswith("utm_"):
                continue
            if lk in ("fbclid", "gclid", "igshid", "mc_cid", "mc_eid"):
                continue
            q.append((k, v))
        new_query = urlencode(q, doseq=True)
        return urlunparse((p.scheme, p.netloc, p.path, p.params, new_query, p.fragment))
    except Exception:
        return url


def _fetch_url(url: str) -> Optional[bytes]:
    try:
        r = requests.get(
            url,
            headers={
                "User-Agent": HTTP_USER_AGENT,
                "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, */*",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "close",
            },
            timeout=(3, FETCH_TIMEOUT_SEC),
            allow_redirects=True,
        )

        ct = (r.headers.get("Content-Type") or "").lower()
        b = r.content or b""
        head = b[:200].decode("utf-8", errors="replace").replace("\n", " ").replace("\r", " ")

        print(
            f"[INFO] fetch url={url} status={r.status_code} bytes={len(b)} ct={ct} final_url={r.url}"
        )

        # 200だけどHTMLが返ってる（= botブロック/エラーページ）を検知
        if "text/html" in ct or head.lstrip().startswith("<!doctype html") or head.lstrip().startswith("<html"):
            print(f"[WARN] non-rss response (looks like HTML). url={url} head={head[:200]}")
            return None

        if 200 <= r.status_code < 300 and b:
            return b

        print(f"[WARN] fetch failed status={r.status_code} url={url} head={head[:200]}")
        return None

    except Exception as ex:
        print(f"[WARN] fetch exception url={url} ex={ex}")
        return None

def fetch_rss_items(feed_urls: List[str], max_items_per_feed: int, max_items_total: int) -> List[Dict[str, Any]]:
    """
    - Parse RSS/Atom robustly
    - Dedup by normalized URL
    - Return up to max_items_total
    """
    items: List[Dict[str, Any]] = []

    for url in feed_urls:
        raw = _fetch_url(url)
        if not raw:
            continue

        try:
            fp = feedparser.parse(raw)
            entries = getattr(fp, "entries", []) or []

            print(f"[INFO] parsed feed url={url} bozo={getattr(fp, 'bozo', None)} entries={len(entries)}")
            if getattr(fp, "bozo", 0):
                print(f"[WARN] feed bozo url={url} ex={getattr(fp, 'bozo_exception', None)}")

            for e in entries[: max_items_per_feed]:
                title = _clean_text(getattr(e, "title", ""))
                link = getattr(e, "link", "") or ""
                link = _normalize_url(link)
                summary = _clean_text(getattr(e, "summary", "") or getattr(e, "description", ""))
                published = getattr(e, "published", "") or getattr(e, "updated", "") or ""

                if not title or not link:
                    continue

                items.append(
                    {
                        "source_feed": url,
                        "title": title,
                        "link": link,
                        "summary": summary,
                        "published": published,
                        "id": _hash(link),
                    }
                )
        except Exception as ex:
            print(f"[WARN] RSS parse failed url={url} ex={ex}")

    # Dedup by normalized link
    seen = set()
    dedup = []
    for it in items:
        if it["link"] in seen:
            continue
        seen.add(it["link"])
        dedup.append(it)

    # Prefer items that have some summary
    dedup.sort(key=lambda x: (0 if x.get("summary") else 1, x.get("published", "")), reverse=False)

    return dedup[:max_items_total]


def bedrock_summarize_and_translate(category_name_ja: str, items: List[Dict[str, Any]]) -> str:
    """
    PoC: summarize based on title/summary only.
    Assumes Nova-like Bedrock message schema.
    """
    compact = []
    for i, it in enumerate(items, start=1):
        compact.append(
            {
                "no": i,
                "title": it["title"],
                "summary": (it["summary"] or "")[:800],
                "published": it["published"],
                "url": it["link"],
                "source_feed": it["source_feed"],
            }
        )

    prompt = f"""
あなたは国際ニュース編集者です。以下はタイのニュース（カテゴリ: {category_name_ja}）のRSS抜粋です。

要件:
- 出力は日本語
- Markdown形式
- 最初に「今日の要点（3〜6点）」を箇条書き
- 次に「記事一覧」として、各記事を見出し付きで要約（2〜4行）し、最後に必ずURLを記載
- 不確かな推測は禁止。与えられた情報（title/summary）から言える範囲で書く
- 誇張せず、事実ベースで簡潔に
- 同じ話題が複数記事にある場合は、記事一覧は残しつつ「同一トピック」と分かるように表現を揃える

入力データ(JSON):
{json.dumps(compact, ensure_ascii=False)}
""".strip()

    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 1800,
        "temperature": 0.2,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt}
                ]
            }
        ]
    }

    resp = brt.invoke_model(
        modelId=BEDROCK_MODEL_ID,
        body=json.dumps(body).encode("utf-8"),
        accept="application/json",
        contentType="application/json",
    )

    payload = json.loads(resp["body"].read().decode("utf-8"))

    # ✅ Claude(Anthropic)の返却は payload["content"] が block配列になりがち
    # 例: [{"type":"text","text":"...markdown..."}, ...]
    out = ""

    # 1) Claude形式（content blocks）
    content_blocks = payload.get("content")
    if isinstance(content_blocks, list):
        parts = []
        for b in content_blocks:
            if isinstance(b, dict) and b.get("type") == "text":
                t = b.get("text", "")
                if isinstance(t, str) and t:
                    parts.append(t)
        out = "\n".join(parts).strip()

    # 2) Converse/Nova系フォールバック（output.message.content）
    if not out:
        parts = []
        for b in payload.get("output", {}).get("message", {}).get("content", []):
            if isinstance(b, dict) and "text" in b and isinstance(b["text"], str):
                parts.append(b["text"])
        out = "\n".join(parts).strip()

    # 3) 最終フォールバック（completion等）
    if not out:
        if isinstance(payload.get("completion"), str):
            out = payload["completion"].strip()
        elif isinstance(payload.get("output_text"), str):
            out = payload["output_text"].strip()

    # 4) それでも空なら、payloadをダンプ（デバッグ用）※本番では消してOK
    if not out:
        out = json.dumps(payload, ensure_ascii=False)

    return out



def build_daily_markdown(date_str: str, sections: List[Tuple[str, str]]) -> str:
    header = f"# Thailand Daily News ({date_str})\n\n"
    toc = "## 目次\n" + "\n".join([f"- [{title}](#{title})" for title, _ in sections]) + "\n\n"
    body = ""
    for title, md in sections:
        body += f"## {title}\n\n{md}\n\n---\n\n"
    return header + toc + body


def put_to_s3(markdown: str, date_str: str) -> str:
    # Requirement: Thailand/yyyy_mm_dd.md
    key = f"Thailand/{date_str}.md"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=markdown.encode("utf-8"),
        ContentType="text/markdown; charset=utf-8",
        CacheControl="no-cache",
    )
    return key


# ---------- Lambda handler ----------
def lambda_handler(event, context):
    print(f"[INFO] timeout={FETCH_TIMEOUT_SEC} max_items_per_feed={MAX_ITEMS_PER_FEED} max_items_per_category={MAX_ITEMS_PER_CATEGORY}")

    today = datetime.now(TH_TZ)
    date_str = today.strftime("%Y_%m_%d")

    print(f"[INFO] start date={date_str} bucket={S3_BUCKET} model={BEDROCK_MODEL_ID}")
    print(f"[INFO] feeds politics={len(RSS_POLITICS)} economy={len(RSS_ECONOMY)} tech={len(RSS_TECH)}")

    # 1) Fetch
    politics_items = fetch_rss_items(RSS_POLITICS, MAX_ITEMS_PER_FEED, MAX_ITEMS_PER_CATEGORY)
    economy_items = fetch_rss_items(RSS_ECONOMY, MAX_ITEMS_PER_FEED, MAX_ITEMS_PER_CATEGORY)
    tech_items = fetch_rss_items(RSS_TECH, MAX_ITEMS_PER_FEED, MAX_ITEMS_PER_CATEGORY)

    # 2) Summarize/Translate per category
    sections: List[Tuple[str, str]] = []

    if politics_items:
        sections.append(("政治", bedrock_summarize_and_translate("政治", politics_items)))
    else:
        sections.append(("政治", "_（取得0件：RSSが落ちている/フィード形式変更の可能性）_"))

    if economy_items:
        sections.append(("経済", bedrock_summarize_and_translate("経済", economy_items)))
    else:
        sections.append(("経済", "_（取得0件：RSSが落ちている/検索条件が強すぎる可能性）_"))

    if tech_items:
        sections.append(("テック", bedrock_summarize_and_translate("テック", tech_items)))
    else:
        sections.append(("テック", "_（取得0件：RSSが落ちている/フィード形式変更の可能性）_"))

    # 3) Build Markdown & Save to S3
    md = build_daily_markdown(date_str, sections)
    key = put_to_s3(md, date_str)

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "ok",
                "date": date_str,
                "s3_bucket": S3_BUCKET,
                "s3_key": key,
                "counts": {
                    "politics": len(politics_items),
                    "economy": len(economy_items),
                    "tech": len(tech_items),
                },
                "feeds": {
                    "politics": RSS_POLITICS,
                    "economy": RSS_ECONOMY,
                    "tech": RSS_TECH,
                },
            },
            ensure_ascii=False,
        ),
    }
