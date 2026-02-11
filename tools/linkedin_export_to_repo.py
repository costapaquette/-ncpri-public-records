#!/usr/bin/env python3
import csv
import hashlib
import os
import re
import subprocess
from datetime import datetime
from pathlib import Path

# Where LinkedIn export is extracted
EXPORT_DIR = Path(os.environ.get("LINKEDIN_EXPORT_DIR", "")).expanduser()

# Where posts go in this repo
OUT_DIR = Path(os.environ.get("LINKEDIN_OUT_DIR", "posts")).expanduser()

# We ONLY treat these file names as eligible sources of "posts"
# (LinkedIn exports vary; keep this list conservative.)
ALLOWED_FILE_HINTS = ("posts", "shares", "updates", "ugc", "article", "activity")

# We NEVER treat these as posts (avoid accidental imports)
BLOCKED_FILE_HINTS = (
    "message", "messages", "inmail", "learning", "coach", "role_play",
    "guide_messages", "whatsapp", "email addresses", "phonenumbers"
)

# Columns that might contain the body text of a real post/article
# IMPORTANT: we intentionally exclude "subject" and "message" now.
TEXT_COL_CANDIDATES = (
    "content", "text", "sharecommentary", "commentary", "post", "body",
    "article text", "articletext", "update text", "updatetext"
)

DATE_COL_CANDIDATES = ("date", "timestamp", "time", "created at", "createdat")
URL_COL_CANDIDATES  = ("url", "link", "permalink")

def run(cmd, cwd=None):
    subprocess.run(cmd, cwd=cwd, check=True)

def slugify(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s[:80] if s else "post"

def sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()

def parse_date(s: str) -> str:
    s = (s or "").strip()
    for fmt in (
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
    ):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    if re.match(r"^\d{4}-\d{2}-\d{2}", s):
        return s[:10]
    return datetime.now().strftime("%Y-%m-%d")

def is_blocked_csv(path: Path) -> bool:
    name = path.name.lower()
    return any(k in name for k in BLOCKED_FILE_HINTS)

def is_allowed_csv(path: Path) -> bool:
    name = path.name.lower()
    return any(k in name for k in ALLOWED_FILE_HINTS)

def sniff_posts_csv(export_dir: Path):
    """
    Search CSVs under export_dir and select best candidate for posts:
    - ignore blocked (messages, learning, etc.)
    - require allowed filename hint (posts/shares/updates/ugc/article/activity)
    - must have a plausible text column (NOT subject/message)
    - prefer files with more non-empty rows
    Returns: (csv_path, text_col, date_col, url_col)
    """
    csv_files = list(export_dir.rglob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found under {export_dir}")

    candidates = []
    for p in csv_files:
        if is_blocked_csv(p):
            continue
        if not is_allowed_csv(p):
            # keep conservative: skip random profile CSVs
            continue

        try:
            with p.open("r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                if not reader.fieldnames:
                    continue

                headers = {h.strip().lower(): h for h in reader.fieldnames}

                text_col = None
                for cand in TEXT_COL_CANDIDATES:
                    if cand in headers:
                        text_col = headers[cand]
                        break
                if not text_col:
                    continue

                date_col = next((headers[c] for c in DATE_COL_CANDIDATES if c in headers), None)
                url_col  = next((headers[c] for c in URL_COL_CANDIDATES if c in headers), None)

                nonempty = 0
                checked = 0
                for row in reader:
                    checked += 1
                    if (row.get(text_col) or "").strip():
                        nonempty += 1
                    if checked >= 3000:
                        break

                if nonempty < 1:
                    continue

                # score: nonempty rows + small bonus for date/url presence
                score = nonempty
                if date_col:
                    score += 10
                if url_col:
                    score += 5

                candidates.append((score, p, text_col, date_col, url_col))
        except Exception:
            continue

    if not candidates:
        # Helpful error: list what CSVs exist
        seen = "\n- ".join(sorted(str(p.relative_to(export_dir)) for p in csv_files)[:80])
        raise FileNotFoundError(
            "No suitable Posts/Shares/Updates/Articles CSV found in this LinkedIn export.\n"
            "This archive likely does NOT include your feed posts/shares.\n"
            f"CSV files seen under {export_dir}:\n- {seen}"
        )

    candidates.sort(key=lambda x: x[0], reverse=True)
    _, path, text_col, date_col, url_col = candidates[0]
    return path, text_col, date_col, url_col

def main():
    if not EXPORT_DIR or not EXPORT_DIR.exists():
        raise RuntimeError("LINKEDIN_EXPORT_DIR is not set or invalid")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    csv_path, text_col, date_col, url_col = sniff_posts_csv(EXPORT_DIR)

    created = 0
    updated = 0

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            text = (row.get(text_col) or "").strip()
            if not text:
                continue

            date = parse_date(row.get(date_col) if date_col else "")
            url = (row.get(url_col) or "").strip() if url_col else ""

            preview = text.splitlines()[0][:80]
            slug = slugify(preview)
            digest = sha1(text)[:10]

            fname = f"{date}-{slug}-{digest}.md"
            path = OUT_DIR / fname

            lines = ["---", f"date: {date}", "platform: linkedin"]
            if url:
                lines.append(f"source: {url}")
            lines += ["---", "", text, ""]

            content = "\n".join(lines)

            if path.exists():
                old = path.read_text(encoding="utf-8")
                if old == content:
                    continue
                path.write_text(content, encoding="utf-8")
                updated += 1
            else:
                path.write_text(content, encoding="utf-8")
                created += 1

    print(f"Selected CSV: {csv_path}")
    print(f"Text column: {text_col}")
    print(f"New posts: {created}, Updated: {updated}")

    # Git commit/push only if changes exist
    try:
        run(["git", "add", str(OUT_DIR)])
        status = subprocess.check_output(["git", "status", "--porcelain"], text=True).strip()
        if status:
            msg = f"Import LinkedIn posts ({created} new, {updated} updated)"
            run(["git", "commit", "-m", msg])
            run(["git", "push"])
            print("Committed and pushed archive repo.")
        else:
            print("No archive changes to commit.")
    except Exception as e:
        print(f"Git step skipped/failed (archive repo): {e}")

if __name__ == "__main__":
    main()
