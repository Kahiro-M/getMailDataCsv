# pop3_to_csv.py
# -*- coding: utf-8 -*-

# 独自ドメインのPOP3サーバからメールを取得し、kintone取り込み用のCSVを出力する。
# - TextBody: text/plain を最優先。無ければ HTML→テキスト化。
# - HtmlBody: 軽く整形（1x1トラッキングピクセル除去など）。
# - CSVはUTF-8(BOM)で出力。列名はkintone側のフィールドコードと一致させる。
# - 重複管理はkintone側のUidl一意制約＋cli-kintoneのupsertに委ねる想定。
#   （必要なら local_dedupe=True でUIDLのローカル重複も避けられる）

import os
import ssl
import csv
import sys
import poplib
import pathlib
from datetime import datetime, timedelta, timezone
import re
from email import policy
from email.parser import BytesParser
from email.header import decode_header, make_header
from email.utils import parsedate_to_datetime
from email.utils import getaddresses
from html import unescape
import configparser
from mkdir_datetime import get_today_date, get_now_time

# ------- 任意：HTML整形用（無ければフォールバック） -------
try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None

# ====== 設定読み込み ======
if getattr(sys, 'frozen', False):
    # exe 実行時：exe のあるフォルダ
    CONFIG_PATH = pathlib.Path(sys.executable).with_name("config.ini")
else:
    # 通常の .py 実行時：このファイルのあるフォルダ
    CONFIG_PATH = pathlib.Path(__file__).with_name("config.ini")

# コメントは別行に書く運用ですが、念のため #/; のインラインコメントも許容
config = configparser.ConfigParser(inline_comment_prefixes=('#', ';'))
config.read(CONFIG_PATH, encoding="utf-8")


# 実行ファイルのあるディレクトリを取得
if getattr(sys, "frozen", False):  # exe 実行時
    BASE_DIR = pathlib.Path(sys.executable).parent
else:                              # .py 実行時
    BASE_DIR = pathlib.Path(__file__).parent

def resolve_path(path_str: str) -> pathlib.Path:
    """
    INIに書かれたパス文字列を解決する。
    - 絶対パスならそのまま
    - 相対パスなら exe/.py のあるフォルダ基準に変換
    """
    p = pathlib.Path(path_str).expanduser()
    return p if p.is_absolute() else (BASE_DIR / p)

# POP3設定
POP3_HOST = config.get("POP3", "host", fallback="")
POP3_PORT = config.getint("POP3", "port", fallback=995)
POP3_USER = config.get("POP3", "user", fallback="")
POP3_PASS = config.get("POP3", "password", fallback="")
POP3_USE_SSL = config.getboolean("POP3", "use_ssl", fallback=True)
POP3_MAX_FETCH = config.getint("POP3", "max_fetch", fallback=200)

# 出力
OUT_DIR = resolve_path(config.get("OUTPUT", "out_dir", fallback="./out_csv"))
FILENAME_PREFIX = config.get("OUTPUT", "filename_prefix", fallback="mails_")
LOCAL_DEDUPE = config.getboolean("OUTPUT", "local_dedupe", fallback=False)
STATE_FILE = resolve_path(config.get("OUTPUT", "state_file", fallback="./processed_uidl.txt"))

# kintone設定
KINTONE_SUBDOMAIN = config.get("KINTONE", "subdomain", fallback="")
KINTONE_DOMAIN = config.get("KINTONE", "domain", fallback="cybozu.com")
KINTONE_APP_ID = config.get("KINTONE", "app_id", fallback="")
KINTONE_API_TOKEN = config.get("KINTONE", "api_token", fallback="")

# フィールドコード
FIELD_UIDL = config.get("FIELD", "uidl", fallback="Uidl")
FIELD_SUBJECT = config.get("FIELD", "subject", fallback="Subject")
FIELD_FROM = config.get("FIELD", "from", fallback="From")
FIELD_DATE = config.get("FIELD", "date", fallback="Date")
FIELD_TEXT = config.get("FIELD", "text", fallback="TextBody")
FIELD_HTML = config.get("FIELD", "html", fallback="HtmlBody")

CSV_HEADERS = [FIELD_UIDL, FIELD_SUBJECT, FIELD_FROM, FIELD_DATE, FIELD_TEXT, FIELD_HTML]

# [FILTER] ・・・フィルタ式（空なら全件対象）
FILTER_RULE = config.get("FILTER", "rule", fallback="").strip()

# ---- タイムゾーン（メールのDateにTZが無い場合の補完用）：Asia/Tokyoを既定に ----
# Python 3.9+ なら zoneinfo が使えるが、依存を増やさないため JST固定を自作
JST = timezone(timedelta(hours=9), name="JST")


# ========== ユーティリティ ==========
def decode_mime_header(raw):
    """MIMEヘッダ（=?UTF-8?B?...?=）をデコードして文字列化"""
    if raw is None:
        return ""
    try:
        return str(make_header(decode_header(raw)))
    except Exception:
        try:
            return raw.decode("utf-8", errors="replace") if isinstance(raw, (bytes, bytearray)) else str(raw)
        except Exception:
            return str(raw)

def html_to_text(html):
    """HTML→テキスト簡易変換。BeautifulSoupがあれば使う。"""
    if not html:
        return ""
    if BeautifulSoup:
        soup = BeautifulSoup(html, "html.parser")
        # トラッキングピクセル（1x1等）を除去
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
    # フォールバック（簡易タグ除去）
    import re
    text = html
    text = re.sub(r"<(script|style)[\s\S]*?</\1>", "", text, flags=re.I)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"</p\s*>", "\n\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    return unescape(text).strip()

def sanitize_html(html):
    """最低限のHTML整形（追跡ピクセル除去など）。"""
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
    return html  # BeautifulSoup未導入時はそのまま

def extract_body_content(html: str, *, strip_script_style: bool = True) -> str:
    """
    HTML文字列から <body>～</body> の中身だけを抽出して返す共通関数。
    - <body>が無い／壊れたHTMLでも例外にならないようにフォールバック。
    - script/styleを除去したい場合は strip_script_style=True を指定。

    戻り値: <body>の内側のHTML（前後空白を削った文字列）
    """
    if not html:
        return ""

    # BOM/ヌル文字などの混入対策（CSV出力時の不正文字対策）
    cleaned = html.replace("\ufeff", "").replace("\x00", "")

    # 改行ありの全文検索。<BODY>など大文字にも対応
    m = re.search(r"<body\b[^>]*>(.*?)</body\s*>", cleaned, re.IGNORECASE | re.DOTALL)
    body_inner = (m.group(1) if m else cleaned).strip()

    if strip_script_style:
        # <script>…</script> と <style>…</style> を丸ごと除去（改行も跨ぐ）
        body_inner = re.sub(r"<script\b[^>]*>.*?</script\s*>", "", body_inner, flags=re.IGNORECASE | re.DOTALL)
        body_inner = re.sub(r"<style\b[^>]*>.*?</style\s*>", "", body_inner, flags=re.IGNORECASE | re.DOTALL)

    # 余計な前後空白を最終整形
    return body_inner.strip()

def extract_addresses_only(header_value: str) -> list[str]:
    """
    From/Toなどのヘッダ文字列から、メールアドレスだけを抜き出して小文字で返す。
    表示名は無視。壊れた/空のヘッダは空リスト。
    """
    if not header_value:
        return []
    # 既存のデコーダでMIMEエンコード解除
    decoded = decode_mime_header(header_value)
    # getaddressesで (display_name, addr) の配列に展開
    pairs = getaddresses([decoded])
    # アドレスのみを小文字で返す
    return [addr.strip().lower() for _, addr in pairs if addr and "@" in addr]

def load_state():
    """既処理UIDLを読み込む（ローカル重複回避用）"""
    if not LOCAL_DEDUPE:
        return set()
    p = pathlib.Path(STATE_FILE)
    if not p.exists():
        return set()
    return set(x.strip() for x in p.read_text(encoding="utf-8").splitlines() if x.strip())

def save_state(uidls):
    """既処理UIDLを保存（ローカル重複回避用）"""
    if not LOCAL_DEDUPE or not uidls:
        return
    p = pathlib.Path(STATE_FILE)
    old = load_state()
    all_u = sorted(old.union(uidls))
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("\n".join(all_u) + "\n", encoding="utf-8")

# ========== フィルタ式（AND/OR/()、SUBJECT、DATE>=NOW-◯d/h/m etc.） ==========
TOKEN_REGEX = re.compile(
    r"""
    SUBJECT:/.*?/         |   # SUBJECT:/.../
    FROM:/.*?/            |   # FROM:/.../
    TO:/.*?/              |   # TO:/.../
    CC:/.*?/              |   # CC:/.../
    DATE[<>]=?\S+         |   # DATE>=..., DATE<NOW-7d など
    AND                   |   # AND
    OR                    |   # OR
    \(|\)                     # 括弧
    """,
    re.VERBOSE | re.IGNORECASE,
)
DATE_REL_REGEX = re.compile(r"^NOW-(\d+)([dhm])$", re.IGNORECASE)
def tokenize(rule: str):
    """ルール文字列をトークン列に分割（未知の断片は無視せずエラーにしたいので厳格に）"""
    if not rule:
        return []
    tokens = TOKEN_REGEX.findall(rule)
    # スペースや改行で分割された不要部分が残っていないか簡易チェック
    compact = "".join(tokens)
    # 括弧と英字・スラッシュ以外はスペースが入り得るので、厳格チェックはしない
    return [t.upper() if t in ("AND", "OR") else t for t in tokens]
def parse_and_eval(
        rule: str,
        subject: str,
        from_addrs: list[str],
        to_addrs: list[str],
        cc_addrs: list[str],
        mail_dt: datetime | None
    ) -> bool:
    """
    フィルタ式を構文解析しつつ評価する
      - SUBJECT:/regex/
      - DATE(>=|<=|>|<)(YYYY-MM-DD | NOW-◯d/h/m)
      - AND / OR / ()
    """
    tokens = tokenize(rule)
    if not tokens:
        return True  # ルール未指定なら全件OK
    idx = 0
    def parse_expr():
        nonlocal idx
        value = parse_term()
        while idx < len(tokens) and tokens[idx] == "OR":
            idx += 1
            rhs = parse_term()
            value = value or rhs
        return value
    def parse_term():
        nonlocal idx
        value = parse_factor()
        while idx < len(tokens) and tokens[idx] == "AND":
            idx += 1
            rhs = parse_factor()
            value = value and rhs
        return value
    def parse_factor():
        nonlocal idx
        if idx >= len(tokens):
            raise ValueError("Unexpected end of filter rule")
        tok = tokens[idx]

        # 括弧
        if tok == "(":
            idx += 1
            val = parse_expr()
            if idx >= len(tokens) or tokens[idx] != ")":
                raise ValueError("Missing closing parenthesis in filter rule")
            idx += 1
            return val

        # SUBJECT:/.../
        if tok.startswith("SUBJECT:/") and tok.endswith("/"):
            # 非貪欲に中身を取る
            m = re.match(r"SUBJECT:/(.*)/$", tok, flags=re.IGNORECASE | re.DOTALL)
            if not m:
                raise ValueError(f"Invalid SUBJECT token: {tok}")
            pattern = m.group(1)
            idx += 1
            try:
                return re.search(pattern, subject or "", flags=re.IGNORECASE) is not None
            except re.error:
                # 正規表現エラー → 不一致扱い
                return False

        # FROM:/.../
        if tok.upper().startswith("FROM:/") and tok.endswith("/"):
            m = re.match(r"(?i)FROM:/(.*)/$", tok, flags=re.DOTALL)
            if not m:
                raise ValueError(f"Invalid FROM token: {tok}")
            pattern = m.group(1)
            idx += 1
            try:
                rgx = re.compile(pattern, flags=re.IGNORECASE)
            except re.error:
                return False
            # いずれかのアドレスがマッチしたらTrue
            return any(rgx.search(addr) for addr in (from_addrs or []))

        # TO:/.../
        if tok.upper().startswith("TO:/") and tok.endswith("/"):
            m = re.match(r"(?i)TO:/(.*)/$", tok, flags=re.DOTALL)
            if not m:
                raise ValueError(f"Invalid TO token: {tok}")
            pattern = m.group(1)
            idx += 1
            try:
                rgx = re.compile(pattern, flags=re.IGNORECASE)
            except re.error:
                return False
            # いずれかのアドレスがマッチしたらTrue
            return any(rgx.search(addr) for addr in (to_addrs or []))

        # CC:/.../
        if tok.upper().startswith("CC:/") and tok.endswith("/"):
            m = re.match(r"(?i)CC:/(.*)/$", tok, flags=re.DOTALL)
            if not m:
                raise ValueError(f"Invalid CC token: {tok}")
            pattern = m.group(1)
            idx += 1
            try:
                rgx = re.compile(pattern, flags=re.IGNORECASE)
            except re.error:
                return False
            # いずれかのアドレスがマッチしたらTrue
            return any(rgx.search(addr) for addr in (cc_addrs or []))

        # DATE...
        if tok.upper().startswith("DATE"):
            ok = eval_date_token(tok, mail_dt)
            idx += 1
            return ok
        raise ValueError(f"Unexpected token: {tok}")
    return parse_expr()
def eval_date_token(token: str, mail_dt: datetime | None) -> bool:
    """
    DATE比較を評価する。
    サポート:
      DATE>=YYYY-MM-DD
      DATE<=YYYY-MM-DD
      DATE>YYYY-MM-DD
      DATE<YYYY-MM-DD
      DATE>=NOW-7d / NOW-24h / NOW-30m（相対）
    """
    m = re.match(r"^DATE([<>]=?)(\S+)$", token, flags=re.IGNORECASE)
    if not m:
        return False
    op = m.group(1)
    rhs = m.group(2)
    # メールの日時が無ければ false（DATE条件は満たせない）
    if mail_dt is None:
        return False
    # 比較対象時刻 cmp_dt を作る
    cmp_dt: datetime
    rel = DATE_REL_REGEX.match(rhs)
    if rel:
        amount = int(rel.group(1))
        unit = rel.group(2).lower()
        delta = timedelta(days=amount) if unit == "d" else (
            timedelta(hours=amount) if unit == "h" else timedelta(minutes=amount)
        )
        # mail_dt のタイムゾーンに合わせて NOW を生成（無い場合は JST）
        base_tz = mail_dt.tzinfo or JST
        cmp_dt = datetime.now(base_tz) - delta
    else:
        # 絶対日付（YYYY-MM-DD または ISO日時想定）
        # 日付だけなら一日の始まりで比較、TZ無しならJST扱い
        try:
            if re.match(r"^\d{4}-\d{2}-\d{2}$", rhs):
                y, mth, d = map(int, rhs.split("-"))
                cmp_dt = datetime(y, mth, d, 0, 0, 0, tzinfo=(mail_dt.tzinfo or JST))
            else:
                # ISO8601想定
                cmp_dt = datetime.fromisoformat(rhs)
                if cmp_dt.tzinfo is None:
                    cmp_dt = cmp_dt.replace(tzinfo=(mail_dt.tzinfo or JST))
        except Exception:
            return False
    # 比較（時刻比較）
    if op == ">=":
        return mail_dt >= cmp_dt
    if op == "<=":
        return mail_dt <= cmp_dt
    if op == ">":
        return mail_dt > cmp_dt
    if op == "<":
        return mail_dt < cmp_dt
    return False
# ========== メイン処理 ==========
def main():
    
    print('====== POP3メールデータcsv取得アプリ ======')
    print('                             v.1.0.0')
    print(f"------ 処理開始 {get_today_date('/')+' '+get_now_time(':')} ------")

    # 入力チェック
    missing = [k for k, v in {
        "POP3_HOST": POP3_HOST,
        "POP3_USER": POP3_USER,
        "POP3_PASS": POP3_PASS
    }.items() if not v]
    if missing:
        print(f"環境変数が不足しています: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)

    # 出力ファイル
    pathlib.Path(OUT_DIR).mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = pathlib.Path(OUT_DIR) / f"{FILENAME_PREFIX}{ts}.csv"

    # POP3接続
    ctx = ssl.create_default_context()
    if POP3_USE_SSL:
        M = poplib.POP3_SSL(POP3_HOST, POP3_PORT, context=ctx, timeout=60)
    else:
        M = poplib.POP3(POP3_HOST, POP3_PORT, timeout=60)

    processed_local = set()
    try:
        M.user(POP3_USER)
        M.pass_(POP3_PASS)

        # メール件数
        resp, listing, octets = M.list()
        total = len(listing)
        if total == 0:
            print("新規メールなし")
            return

        # UIDL（各通の一意ID）取得
        resp, uidl_list, _ = M.uidl()
        uidl_map = {}  # msg_num -> uidl
        for row in uidl_list:
            parts = row.decode("utf-8", errors="replace").split()
            if len(parts) >= 2:
                msg_num = int(parts[0])
                uidl = parts[1]
                uidl_map[msg_num] = uidl

        # ローカル重複管理（任意）
        already = load_state()

        # 直近N通だけ処理
        start = max(1, total - POP3_MAX_FETCH + 1)

        # CSVをBOM付きUTF-8で書く（Excel想定）
        with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
            writer.writeheader()

            for i in range(start, total + 1):
                uidl = uidl_map.get(i, f"NOUIDL-{i}")
                if LOCAL_DEDUPE and uidl in already:
                    continue  # 既に出力済み（ローカル重複回避ON時）

                # メールを取得
                try:
                    resp, lines, octets = M.retr(i)
                    raw = b"\r\n".join(lines)
                    msg = BytesParser(policy=policy.default).parsebytes(raw)

                    # ヘッダ
                    subject = decode_mime_header(msg["Subject"])
                    from_ = decode_mime_header(msg.get("From", ""))
                    to_ = decode_mime_header(msg.get("To", ""))
                    cc_ = decode_mime_header(msg.get("Cc", ""))

                    # 受信日時
                    date_iso = ""
                    mail_dt = None
                    if msg.get("Date"):
                        try:
                            mail_dt = parsedate_to_datetime(msg.get("Date"))
                            if mail_dt.tzinfo is None:
                                # TZが無いヘッダはJSTと仮定
                                mail_dt = mail_dt.replace(tzinfo=JST)
                            date_iso = mail_dt.astimezone().strftime("%Y-%m-%d %H:%M")
                        except Exception:
                            mail_dt = None
                            date_iso = ""

                    # フィルタ評価（指定があれば）
                    if FILTER_RULE:
                        try:
                            # From/To/CCアドレス配列を準備
                            from_addrs = extract_addresses_only(from_)
                            to_addrs = extract_addresses_only(to_)
                            cc_addrs = extract_addresses_only(cc_)
                            if not parse_and_eval(
                                rule=FILTER_RULE,
                                subject=subject,
                                from_addrs=from_addrs,
                                to_addrs=to_addrs,
                                cc_addrs=cc_addrs,
                                mail_dt=mail_dt
                                ):
                                continue  # 条件外はスキップ
                        except Exception as e:
                            # ルール記述ミス時は安全側（取り込まない）
                            print(f"[WARN] フィルタ評価失敗: {e}", file=sys.stderr)
                            continue

                    # 本文抽出（text/plain優先、なければHTML→テキスト化）
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

                    # テキスト優先：無ければHTML→テキスト化
                    if not text_body and html_raw:
                        text_body = html_to_text(html_raw)

                    html_clean = extract_body_content(sanitize_html(html_raw or ""),strip_script_style=True)

                    # CSV行として書き出し
                    row = {
                        FIELD_UIDL: uidl,
                        FIELD_SUBJECT: subject,
                        FIELD_FROM: from_,
                        FIELD_DATE: date_iso,
                        FIELD_TEXT: text_body or "",
                        FIELD_HTML: html_clean or "",
                    }
                    writer.writerow(row)
                    processed_local.add(uidl)

                except Exception as e:
                    # 1通の失敗はログだけ出して継続
                    print(f"[WARN] メール#{i}の処理に失敗: {e}", file=sys.stderr)
                    continue

        # ローカル重複管理を更新
        save_state(processed_local)

        print(f"CSV出力: {out_path}")

    finally:
        try:
            M.quit()
        except Exception:
            pass
    print(f"------ 処理終了 {get_today_date('/')+' '+get_now_time(':')} ------")

if __name__ == "__main__":
    main()