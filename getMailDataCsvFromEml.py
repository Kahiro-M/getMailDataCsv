# -*- coding: utf-8 -*-
"""
複数の .eml を読み込み、getMailDataCsv.py と同じ列構成の CSV（UTF-8 BOM）に出力するツール。
- TextBody は text/plain を優先、無ければ HTML -> テキスト化
- HtmlBody は <body> 内を抽出、script/style 除去、1x1ピクセル除去など軽整形
- Date は INI [MODE] date_format / timezone で出力制御
- フィルタ式: SUBJECT:/.../, FROM:/.../, TO:/.../, CC:/.../, DATE>=NOW-7d など（既存互換）

実運用のポイント:
- UIDL は POP3 固有のため、EML では Message-Id or 内容ハッシュで代替
- 文字化け対策で charset -> utf-8 のフォールバックを実装
"""

import csv
import sys
import re
import ssl
import hashlib
import pathlib
import configparser
from datetime import datetime, timedelta, timezone
from email import policy
from email.parser import BytesParser
from email.header import decode_header, make_header
from email.utils import parsedate_to_datetime, getaddresses
from html import unescape

# --- 任意：HTML整形（無ければフォールバック） ---
try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None

# ====== 実行ファイル / 設定ロード ======
if getattr(sys, "frozen", False):
    BASE_DIR = pathlib.Path(sys.executable).parent
    CONFIG_PATH = pathlib.Path(sys.executable).with_name("config_eml.ini")
else:
    BASE_DIR = pathlib.Path(__file__).parent
    CONFIG_PATH = pathlib.Path(__file__).with_name("config_eml.ini")

def resolve_path(path_str: str) -> pathlib.Path:
    """INI の相対パスは .py/.exe のあるフォルダ基準に解決"""
    p = pathlib.Path(path_str).expanduser()
    return p if p.is_absolute() else (BASE_DIR / p)

# RawConfigParser で % 展開を無効化（strftime の %Y 等を安全に扱う）
config = configparser.RawConfigParser(inline_comment_prefixes=('#', ';'))
config.read(CONFIG_PATH, encoding="utf-8")

# ====== 設定 ======
EML_DIR = resolve_path(config.get("INPUT", "eml_dir", fallback="./eml_in"))

OUT_DIR = resolve_path(config.get("OUTPUT", "out_dir", fallback="./out_csv"))
FILENAME_PREFIX = config.get("OUTPUT", "filename_prefix", fallback="mails_")
# 以下2つは互換のために読むが、本ツールでは未使用
LOCAL_DEDUPE = config.getboolean("OUTPUT", "local_dedupe", fallback=False)
STATE_FILE = resolve_path(config.get("OUTPUT", "state_file", fallback="./processed_uidl.txt"))

FIELD_UIDL = config.get("FIELD", "uidl", fallback="Uidl")
FIELD_SUBJECT = config.get("FIELD", "subject", fallback="Subject")
FIELD_FROM = config.get("FIELD", "from", fallback="From")
FIELD_DATE = config.get("FIELD", "date", fallback="Date")
FIELD_TEXT = config.get("FIELD", "text", fallback="TextBody")
FIELD_HTML = config.get("FIELD", "html", fallback="HtmlBody")
CSV_HEADERS = [FIELD_UIDL, FIELD_SUBJECT, FIELD_FROM, FIELD_DATE, FIELD_TEXT, FIELD_HTML]

DATE_FORMAT = config.get("MODE", "date_format", fallback="%Y-%m-%d %H:%M")
TIMEZONE_MODE = config.get("MODE", "timezone", fallback="local").lower()

FILTER_RULE = config.get("FILTER", "rule", fallback="").strip()

# JST（固定TZの後方互換用）
JST = timezone(timedelta(hours=9), name="JST")

# ====== ユーティリティ ======
def get_output_timezone():
    """INIのtimezone指定に従い出力タイムゾーンを返す。None ならローカル。"""
    if TIMEZONE_MODE == "utc":
        return timezone.utc
    if TIMEZONE_MODE in ("jst", "asia/tokyo"):
        try:
            import zoneinfo
            return zoneinfo.ZoneInfo("Asia/Tokyo")
        except Exception:
            return JST
    return None  # local

def decode_mime_header(raw) -> str:
    """MIMEエンコードヘッダを安全にデコード"""
    if raw is None:
        return ""
    try:
        return str(make_header(decode_header(raw)))
    except Exception:
        try:
            return raw.decode("utf-8", errors="replace") if isinstance(raw, (bytes, bytearray)) else str(raw)
        except Exception:
            return str(raw)

def extract_addresses_only(header_value: str) -> list[str]:
    """From/To/Cc からアドレスのみ小文字で抽出"""
    if not header_value:
        return []
    decoded = decode_mime_header(header_value)
    pairs = getaddresses([decoded])
    return [addr.strip().lower() for _, addr in pairs if addr and "@" in addr]

def html_to_text(html: str) -> str:
    """HTML -> 素朴なテキスト"""
    if not html:
        return ""
    if BeautifulSoup:
        soup = BeautifulSoup(html, "html.parser")
        # 1x1トラッキング画像除去
        for img in soup.find_all("img"):
            try:
                w = int(img.get("width", "0"))
                h = int(img.get("height", "0"))
                if (w and w <= 1) or (h and h <= 1):
                    img.decompose()
            except Exception:
                pass
        text = soup.get_text("\n")
        return unescape(text).strip()
    # フォールバック
    text = html
    text = re.sub(r"<(script|style)[\s\S]*?</\1>", "", text, flags=re.I)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"</p\s*>", "\n\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    return unescape(text).strip()

def sanitize_html(html: str) -> str:
    """最低限のHTML整形（追跡ピクセル除去）"""
    if not html:
        return ""
    if BeautifulSoup:
        soup = BeautifulSoup(html, "html.parser")
        for img in soup.find_all("img"):
            try:
                w = int(img.get("width", "0"))
                h = int(img.get("height", "0"))
                if (w and w <= 1) or (h and h <= 1):
                    img.decompose()
            except Exception:
                pass
        return str(soup)
    return html

def extract_body_content(html: str, *, strip_script_style: bool = True) -> str:
    """<body> 内を抽出し、script/style を除去"""
    if not html:
        return ""
    cleaned = html.replace("\ufeff", "").replace("\x00", "")
    m = re.search(r"<body\b[^>]*>(.*?)</body\s*>", cleaned, re.IGNORECASE | re.DOTALL)
    body_inner = (m.group(1) if m else cleaned).strip()
    if strip_script_style:
        body_inner = re.sub(r"<script\b[^>]*>.*?</script\s*>", "", body_inner, flags=re.I | re.DOTALL)
        body_inner = re.sub(r"<style\b[^>]*>.*?</style\s*>", "", body_inner, flags=re.I | re.DOTALL)
    return body_inner.strip()

# ====== フィルタ式（既存互換） ======
TOKEN_REGEX = re.compile(
    r"""
    SUBJECT:/.*?/         |
    FROM:/.*?/            |
    TO:/.*?/              |
    CC:/.*?/              |
    DATE[<>]=?\S+         |
    AND                   |
    OR                    |
    \(|\)
    """,
    re.VERBOSE | re.IGNORECASE,
)
DATE_REL_REGEX = re.compile(r"^NOW-(\d+)([dhm])$", re.IGNORECASE)

def tokenize(rule: str):
    if not rule:
        return []
    tokens = TOKEN_REGEX.findall(rule)
    return [t.upper() if t in ("AND", "OR") else t for t in tokens]

def eval_date_token(token: str, mail_dt: datetime | None) -> bool:
    m = re.match(r"^DATE([<>]=?)(\S+)$", token, flags=re.IGNORECASE)
    if not m:
        return False
    op = m.group(1)
    rhs = m.group(2)
    if mail_dt is None:
        return False

    # 比較対象の作成
    rel = DATE_REL_REGEX.match(rhs)
    if rel:
        amount = int(rel.group(1))
        unit = rel.group(2).lower()
        delta = timedelta(days=amount) if unit == "d" else (
            timedelta(hours=amount) if unit == "h" else timedelta(minutes=amount)
        )
        base_tz = mail_dt.tzinfo or JST
        cmp_dt = datetime.now(base_tz) - delta
    else:
        try:
            if re.match(r"^\d{4}-\d{2}-\d{2}$", rhs):
                y, mth, d = map(int, rhs.split("-"))
                cmp_dt = datetime(y, mth, d, 0, 0, 0, tzinfo=(mail_dt.tzinfo or JST))
            else:
                cmp_dt = datetime.fromisoformat(rhs)
                if cmp_dt.tzinfo is None:
                    cmp_dt = cmp_dt.replace(tzinfo=(mail_dt.tzinfo or JST))
        except Exception:
            return False

    if op == ">=":
        return mail_dt >= cmp_dt
    if op == "<=":
        return mail_dt <= cmp_dt
    if op == ">":
        return mail_dt > cmp_dt
    if op == "<":
        return mail_dt < cmp_dt
    return False

def parse_and_eval(rule: str, subject: str, from_addrs: list[str],
                   to_addrs: list[str], cc_addrs: list[str],
                   mail_dt: datetime | None) -> bool:
    tokens = tokenize(rule)
    if not tokens:
        return True
    idx = 0
    def parse_expr():
        nonlocal idx
        val = parse_term()
        while idx < len(tokens) and tokens[idx] == "OR":
            idx += 1
            val = val or parse_term()
        return val
    def parse_term():
        nonlocal idx
        val = parse_factor()
        while idx < len(tokens) and tokens[idx] == "AND":
            idx += 1
            val = val and parse_factor()
        return val
    def parse_factor():
        nonlocal idx
        if idx >= len(tokens):
            raise ValueError("Unexpected end of filter rule")
        tok = tokens[idx]
        if tok == "(":
            idx += 1
            val = parse_expr()
            if idx >= len(tokens) or tokens[idx] != ")":
                raise ValueError("Missing closing parenthesis in filter rule")
            idx += 1
            return val
        if tok.upper().startswith("SUBJECT:/") and tok.endswith("/"):
            m = re.match(r"(?i)SUBJECT:/(.*)/$", tok, flags=re.DOTALL)
            pattern = "" if not m else m.group(1)
            idx += 1
            try:
                return re.search(pattern, subject or "", flags=re.I) is not None
            except re.error:
                return False
        if tok.upper().startswith("FROM:/") and tok.endswith("/"):
            m = re.match(r"(?i)FROM:/(.*)/$", tok, flags=re.DOTALL)
            pattern = "" if not m else m.group(1)
            idx += 1
            try:
                rgx = re.compile(pattern, flags=re.I)
            except re.error:
                return False
            return any(rgx.search(addr) for addr in (from_addrs or []))
        if tok.upper().startswith("TO:/") and tok.endswith("/"):
            m = re.match(r"(?i)TO:/(.*)/$", tok, flags=re.DOTALL)
            pattern = "" if not m else m.group(1)
            idx += 1
            try:
                rgx = re.compile(pattern, flags=re.I)
            except re.error:
                return False
            return any(rgx.search(addr) for addr in (to_addrs or []))
        if tok.upper().startswith("CC:/") and tok.endswith("/"):
            m = re.match(r"(?i)CC:/(.*)/$", tok, flags=re.DOTALL)
            pattern = "" if not m else m.group(1)
            idx += 1
            try:
                rgx = re.compile(pattern, flags=re.I)
            except re.error:
                return False
            return any(rgx.search(addr) for addr in (cc_addrs or []))
        if tok.upper().startswith("DATE"):
            ok = eval_date_token(tok, mail_dt)
            idx += 1
            return ok
        raise ValueError(f"Unexpected token: {tok}")
    return parse_expr()

# ====== メイン処理 ======
def find_eml_files(root: pathlib.Path) -> list[pathlib.Path]:
    """eml_dir 配下から再帰的に .eml を収集"""
    if not root.exists():
        return []
    return [p for p in root.rglob("*.eml") if p.is_file()]

def make_uidl_for_eml(msg_bytes: bytes, msg) -> str:
    """EML用の一意キー：Message-Id があればそれ、無ければ SHA1"""
    mid = (msg.get("Message-Id") or msg.get("Message-ID") or "").strip()
    if mid:
        return mid
    return "SHA1-" + hashlib.sha1(msg_bytes).hexdigest()

def main():
    print("====== EML → CSV 出力ツール ======")
    print("                                    v.1.0.0")

    emls = find_eml_files(EML_DIR)
    if not emls:
        print(f"EMLが見つかりませんでした: {EML_DIR}")
        return

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = OUT_DIR / f"{FILENAME_PREFIX}{ts}.csv"

    tz_out = get_output_timezone()

    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writeheader()

        for eml_path in sorted(emls):
            try:
                raw = eml_path.read_bytes()
                msg = BytesParser(policy=policy.default).parsebytes(raw)

                # ヘッダ
                subject = decode_mime_header(msg.get("Subject"))
                from_ = decode_mime_header(msg.get("From", ""))
                to_ = decode_mime_header(msg.get("To", ""))
                cc_ = decode_mime_header(msg.get("Cc", ""))

                # 日付
                date_str = ""
                mail_dt = None
                if msg.get("Date"):
                    try:
                        mail_dt = parsedate_to_datetime(msg.get("Date"))
                        if mail_dt.tzinfo is None:
                            mail_dt = mail_dt.replace(tzinfo=JST)
                        dt_out = mail_dt.astimezone(tz_out) if tz_out else mail_dt.astimezone()
                        date_str = dt_out.strftime(DATE_FORMAT)
                    except Exception:
                        mail_dt = None
                        date_str = ""

                # フィルタ（任意）
                if FILTER_RULE:
                    try:
                        ok = parse_and_eval(
                            rule=FILTER_RULE,
                            subject=subject,
                            from_addrs=extract_addresses_only(from_),
                            to_addrs=extract_addresses_only(to_),
                            cc_addrs=extract_addresses_only(cc_),
                            mail_dt=mail_dt
                        )
                        if not ok:
                            continue
                    except Exception as e:
                        print(f"[WARN] フィルタ評価失敗({eml_path.name}): {e}", file=sys.stderr)
                        continue

                # 本文抽出
                text_body, html_raw = None, None
                if msg.is_multipart():
                    for part in msg.walk():
                        if part.get_content_disposition() == "attachment":
                            continue
                        ctype = part.get_content_type()
                        payload = part.get_payload(decode=True) or b""
                        charset = part.get_content_charset() or "utf-8"
                        try:
                            content = payload.decode(charset, errors="replace")
                        except Exception:
                            content = payload.decode("utf-8", errors="replace")
                        if ctype == "text/plain" and text_body is None:
                            text_body = content.strip()
                        elif ctype == "text/html" and html_raw is None:
                            html_raw = content
                else:
                    ctype = msg.get_content_type()
                    payload = msg.get_payload(decode=True) or b""
                    charset = msg.get_content_charset() or "utf-8"
                    try:
                        content = payload.decode(charset, errors="replace")
                    except Exception:
                        content = payload.decode("utf-8", errors="replace")
                    if ctype == "text/plain":
                        text_body = content.strip()
                    elif ctype == "text/html":
                        html_raw = content

                if not text_body and html_raw:
                    text_body = html_to_text(html_raw)

                html_clean = extract_body_content(sanitize_html(html_raw or ""), strip_script_style=True)

                # UIDL 代替キー
                uidl = make_uidl_for_eml(raw, msg)

                # CSV 書き込み
                row = {
                    FIELD_UIDL: uidl,
                    FIELD_SUBJECT: subject,
                    FIELD_FROM: from_,
                    FIELD_DATE: date_str,
                    FIELD_TEXT: text_body or "",
                    FIELD_HTML: html_clean or "",
                }
                writer.writerow(row)

            except Exception as e:
                # 1件の失敗はスキップ（ログ出力）
                print(f"[WARN] {eml_path} の処理に失敗: {e}", file=sys.stderr)
                continue

    print(f"CSV出力: {out_path}")

if __name__ == "__main__":
    main()
