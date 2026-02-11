#!/usr/bin/env python3
import csv
import hashlib
import os
import re
import subprocess
from datetime import datetime
from pathlib import Path

# Where LinkedIn export is extracted
EXPORT_DIR = Path(os.environ.get("LINKEDIN_EXPORT_DIR", ""))

# Where posts go in this repo
OUT_DIR = Path(os.environ.get("LINKEDIN_OUT_DIR", "posts"))

CSV_CANDIDATES = [
    "Posts.csv",
    "Shares.csv",
    "posts.csv",
    "shares.csv",
]

def run(cmd, cwd=None):
    subprocess.run(cmd, cwd=cwd, check=True)

def slugify(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s[:80] if s else "post"

def sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()

def find_csv(export_dir: Path) -> Path:
    for name in CSV_CANDIDATES:
        p = export_dir / name
        if p.exists():
            return p

    for p in export_dir.rglob("*.csv"):
        if "post" in p.name.lower() or "share" in p.name.lower():
            return p

    raise FileNotFoundError(f"No CSV found in {export_dir}")

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

def main():
    if not EXPORT_DIR or not EXPORT_DIR.exists():
        raise RuntimeError("LINKEDIN_EXPORT_DIR is not set or invalid")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = find_csv(EXPORT_DIR)

    created = 0
    updated = 0

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        headers = [h.lower() for h in (reader.fieldnames or [])]

        def pick(names):
            for n in names:
                for h in reader.fieldnames or []:
                    if h.lower().strip() == n:
                        return h
            return None

        date_col = pick(["date", "timestamp", "time", "created at", "createdat"])
        text_col = pick(["content", "text", "sharecommentary", "commentary", "post", "body"])
        url_col  = pick(["url", "link", "permalink"])

        if not text_col:
            raise RuntimeError(f"Cannot find text column in {reader.fieldnames}")

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

            lines = [
                "---",
                f"date: {date}",
                "platform: linkedin",
            ]

            if url:
                lines.append(f"source: {url}")

            lines += [
                "---",
                "",
                text,
                "",
            ]

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

    print(f"Imported from: {csv_path}")
    print(f"New posts: {created}, Updated: {updated}")

    try:
        run(["git", "add", str(OUT_DIR)])
        status = subprocess.check_output(
            ["git", "status", "--porcelain"], text=True
        ).strip()

        if status:
            msg = f"Import LinkedIn posts ({created} new, {updated} updated)"
            run(["git", "commit", "-m", msg])
            run(["git", "push"])
            print("Committed and pushed.")
        else:
            print("No changes to commit.")

    except Exception as e:
        print(f"Git step skipped: {e}")

if __name__ == "__main__":
    main()

