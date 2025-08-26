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
from html import unescape
import configparser

# ------- 任意：HTML整形用（無ければフォールバック） -------
try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None

# ====== 設定読み込み ======
CONFIG_PATH = pathlib.Path(__file__).with_name("config.ini")

config = configparser.ConfigParser()
config.read(CONFIG_PATH, encoding="utf-8")

# POP3設定
POP3_HOST = config.get("POP3", "host")
POP3_PORT = config.getint("POP3", "port", fallback=995)
POP3_USER = config.get("POP3", "user")
POP3_PASS = config.get("POP3", "password")
POP3_USE_SSL = config.getboolean("POP3", "use_ssl", fallback=True)
POP3_MAX_FETCH = config.getint("POP3", "max_fetch", fallback=200)

# 出力
OUT_DIR = config.get("OUTPUT", "out_dir", fallback="./out_csv")
FILENAME_PREFIX = config.get("OUTPUT", "filename_prefix", fallback="mails_")
LOCAL_DEDUPE = config.getboolean("OUTPUT", "local_dedupe", fallback=False)
STATE_FILE = config.get("OUTPUT", "state_file", fallback="./processed_uidl.txt")

# kintone設定
KINTONE_SUBDOMAIN = config.get("KINTONE", "subdomain")
KINTONE_DOMAIN = config.get("KINTONE", "domain", fallback="cybozu.com")
KINTONE_APP_ID = config.getint("KINTONE", "app_id")
KINTONE_API_TOKEN = config.get("KINTONE", "api_token")

# フィールドコード
FIELD_UIDL = config.get("FIELD", "uidl", fallback="Uidl")
FIELD_SUBJECT = config.get("FIELD", "subject", fallback="Subject")
FIELD_FROM = config.get("FIELD", "from", fallback="From")
FIELD_DATE = config.get("FIELD", "date", fallback="Date")
FIELD_TEXT = config.get("FIELD", "text", fallback="TextBody")
FIELD_HTML = config.get("FIELD", "html", fallback="HtmlBody")

CSV_HEADERS = [FIELD_UIDL, FIELD_SUBJECT, FIELD_FROM, FIELD_DATE, FIELD_TEXT, FIELD_HTML]

# ====== ユーティリティ ======
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

# ====== メイン処理 ======
def main():
    # 入力チェック
    missing = [k for k, v in {
        "POP3_HOST": POP3_HOST, "POP3_USER": POP3_USER, "POP3_PASS": POP3_PASS
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

                    # 日時（ISO 8601で）
                    date_hdr = msg.get("Date")
                    date_iso = ""
                    if date_hdr:
                        try:
                            dt = parsedate_to_datetime(date_hdr)
                            if dt.tzinfo is None:
                                dt = dt.replace(tzinfo=timezone.utc)
                            date_iso = dt.astimezone().isoformat(timespec="seconds")
                        except Exception:
                            date_iso = ""

                    # 本文抽出（text/plain優先）
                    text_body = None
                    html_raw = None
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

                    html_clean = sanitize_html(html_raw or "")

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

if __name__ == "__main__":
    main()