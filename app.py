# app.py
from dataclasses import dataclass
from datetime import date, datetime
from zoneinfo import ZoneInfo

import boto3, os, calendar
import streamlit as st
from botocore.exceptions import ClientError
from dotenv import load_dotenv


# ---------- Constants ----------
APP_PREFIX = "global-news-"
TH_TZ = ZoneInfo("Asia/Bangkok")  # Daily cut aligns with Thailand time


# ---------- Env Loader ----------
# ローカル実行時のみ .env を読む（Cloud では通常 .env は無い）
load_dotenv(override=False)


def get_env(key: str, default: str | None = None) -> str | None:
    """
    Priority:
      1. OS env (.env 포함)
      2. st.secrets (Streamlit Cloud)
      3. default
    """
    if key in os.environ:
        return os.environ.get(key)

    if hasattr(st, "secrets") and key in st.secrets:
        return st.secrets.get(key)

    return default


# ---------- Config ----------
AWS_REGION = get_env("AWS_REGION", "us-west-2")
S3_BUCKET = get_env("S3_BUCKET")

AWS_ACCESS_KEY_ID = get_env("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = get_env("AWS_SECRET_ACCESS_KEY")


# ---------- S3 Client ----------
_s3_kwargs = {
    "region_name": AWS_REGION,
}

if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
    _s3_kwargs.update(
        {
            "aws_access_key_id": AWS_ACCESS_KEY_ID,
            "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
        }
    )

s3 = boto3.client("s3", **_s3_kwargs)

# ---------- Helpers ----------
def md_key_for(d: date) -> str:
    return f"Thailand/{d.strftime('%Y_%m_%d')}.md"


def load_md_from_s3(key: str) -> str:
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return obj["Body"].read().decode("utf-8")


def list_month_objects(prefix: str) -> set[str]:
    """
    Optional UX improvement:
    - Prefetch list of existing md files in the month to show markers.
    """
    keys = set()
    token = None
    while True:
        kwargs = {"Bucket": S3_BUCKET, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        for it in resp.get("Contents", []):
            keys.add(it["Key"])
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return keys


@dataclass(frozen=True)
class MonthView:
    year: int
    month: int


def month_grid(year: int, month: int):
    cal = calendar.Calendar(firstweekday=0)  # Monday start
    return list(cal.monthdatescalendar(year, month))


# ---------- Page ----------
st.set_page_config(
    page_title="Thailand Daily News",
    page_icon="📰",
    layout="wide",
)

st.title("Thailand Daily News")

now_th = datetime.now(TH_TZ).date()

# Session state
if "selected_date" not in st.session_state:
    st.session_state["selected_date"] = now_th
if "month_view" not in st.session_state:
    st.session_state["month_view"] = MonthView(now_th.year, now_th.month)

selected_date: date = st.session_state["selected_date"]
month_view: MonthView = st.session_state["month_view"]

# CSS: highlight today only (bright background + border)
st.markdown(
    """
<style>
/* Make buttons more compact */
div.stButton > button {
  width: 100%;
  padding: 0.35rem 0.25rem;
  border-radius: 0.75rem;
}

/* Today highlight: we mark via data-testid wrapper class */
.today-btn div.stButton > button {
  border: 2px solid rgba(255, 255, 255, 0.6) !important;
  background: rgba(255, 255, 255, 0.20) !important;
  font-weight: 700 !important;
}

/* Dim out non-current-month buttons (disabled look is already there, but keep subtle) */
.dim-btn div.stButton > button {
  opacity: 0.55;
}
</style>
""",
    unsafe_allow_html=True,
)


# ---------- Layout ----------
col_left, col_right = st.columns([1, 2], gap="large")

with col_left:
    st.subheader("カレンダー")

    # Month navigation
    nav1, nav2, nav3 = st.columns([1, 2, 1])
    with nav1:
        if st.button("◀", key="prev-month"):
            y, m = month_view.year, month_view.month
            if m == 1:
                month_view = MonthView(y - 1, 12)
            else:
                month_view = MonthView(y, m - 1)
            st.session_state["month_view"] = month_view
            st.rerun()

    with nav2:
        st.markdown(f"### {month_view.year}-{month_view.month:02d}")

    with nav3:
        if st.button("▶", key="next-month"):
            y, m = month_view.year, month_view.month
            if m == 12:
                month_view = MonthView(y + 1, 1)
            else:
                month_view = MonthView(y, m + 1)
            st.session_state["month_view"] = month_view
            st.rerun()

    # Optional: prefetch existing objects for the month to show indicator
    # Prefix example: Thailand/2026_02_
    month_prefix = f"Thailand/{month_view.year}_{month_view.month:02d}_"
    try:
        existing_keys = list_month_objects(prefix=month_prefix)
    except Exception:
        existing_keys = set()

    # Weekday header
    dow = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    hdr = st.columns(7)
    for i, d in enumerate(dow):
        hdr[i].markdown(f"**{d}**")

    weeks = month_grid(month_view.year, month_view.month)

    for w in weeks:
        cols = st.columns(7)
        for i, d in enumerate(w):
            is_current_month = (d.month == month_view.month)
            is_today = (d == now_th)

            # Show an indicator if file exists
            key = md_key_for(d)
            has_file = key in existing_keys

            label = str(d.day)
            if has_file and is_current_month:
                label = f"{label} •"  # dot marker

            # Use containers with class to control CSS per cell
            wrapper_class = []
            if is_today:
                wrapper_class.append("today-btn")
            if not is_current_month:
                wrapper_class.append("dim-btn")

            # Streamlit doesn't allow per-button class directly, so wrap in HTML
            # that scopes via CSS selectors above.
            with cols[i]:
                st.markdown(f'<div class="{" ".join(wrapper_class)}">', unsafe_allow_html=True)
                clicked = st.button(
                    label,
                    key=f"day-{month_view.year}-{month_view.month}-{d.isoformat()}",
                    disabled=not is_current_month,
                )
                st.markdown("</div>", unsafe_allow_html=True)

                if clicked:
                    st.session_state["selected_date"] = d
                    st.rerun()

    st.divider()
    st.caption("• が付いている日はMarkdownがS3に存在します（推定）。")

with col_right:
    st.subheader(f"記事: {selected_date.strftime('%Y-%m-%d')}")
    key = md_key_for(selected_date)

    try:
        md = load_md_from_s3(key)
        st.markdown(md)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404", "NotFound"):
            st.info(f"この日のファイルがまだありません: `{key}`")
        else:
            st.error(f"S3取得エラー: {e}")
    except Exception as e:
        st.error(f"予期しないエラー: {e}")
