"""Phase 11 -- check every reference in ``paper/references.bib`` against its publisher record.

For each entry with a DOI, Crossref is queried and the title, year and (where given) volume,
issue and pages are compared with the BibTeX fields. Entries with a URL are checked for an
HTTP 200 answer. Standard library only.

Usage::

    python tools/check_references.py
"""

from __future__ import annotations

import json
import re
import sys
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from similarity_check import BIB, bib_field, tokens  # noqa: E402

HEADERS = {"User-Agent": "cao-df-reference-check (mailto:mustaqeemshaikh7862@gmail.com)"}
# Papers with no DOI: verified against the publisher's page.
MANUAL = {"alipourfard2017cherrypick": "USENIX NSDI 2017 proceedings, pp. 469-482"}


def get(url: str, head: bool = False):
    req = urllib.request.Request(url, headers=HEADERS, method="HEAD" if head else "GET")
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.status if head else json.load(r)


def main() -> int:
    problems = 0
    blocks = [b for b in re.split(r"\n(?=@)", BIB.read_text(encoding="utf-8")) if re.match(r"@\w+\{", b.strip())]
    for block in blocks:
        key = re.match(r"@\w+\{([^,]+),", block.strip())[1]
        doi = bib_field(block, "doi")
        url = bib_field(block, "url") or next(iter(re.findall(r"\\url\{([^}]+)\}", block)), "")
        arxiv = re.search(r"arXiv:(\d{4}\.\d{4,5})", block)
        notes = []
        if doi:
            try:
                msg = get("https://api.crossref.org/works/" + urllib.parse.quote(doi))["message"]
            except Exception as exc:
                notes.append(f"DOI lookup failed ({exc})")
            else:
                title = re.sub(r"<[^>]+>", "", " ".join((msg.get("title") or []) + (msg.get("subtitle") or [])))
                bib_title = bib_field(block, "title")
                mine_t, theirs_t = "".join(tokens(bib_title)), "".join(tokens(title))
                if theirs_t != mine_t and not (theirs_t and mine_t.startswith(theirs_t)):
                    notes.append(f"title differs: {title!r}")
                parts = (msg.get("issued") or {}).get("date-parts") or [[None]]
                year = bib_field(block, "year")
                if year and str(parts[0][0]) != year:
                    notes.append(f"year {year} vs Crossref {parts[0][0]}")
                for field, cr in (("volume", "volume"), ("number", "issue"), ("pages", "page")):
                    mine = re.sub(r"\b\d+:", "", bib_field(block, field).replace("--", "-"))
                    theirs = str(msg.get(cr, ""))
                    if mine and theirs and mine != theirs:
                        notes.append(f"{field} {mine} vs Crossref {theirs}")
        if url:
            try:
                status = get(url, head=True)
                if status != 200:
                    notes.append(f"URL status {status}")
            except Exception as exc:
                notes.append(f"URL failed ({exc})")
        if arxiv and not doi:
            try:
                with urllib.request.urlopen(f"https://export.arxiv.org/api/query?id_list={arxiv[1]}", timeout=60) as r:
                    feed = r.read().decode("utf-8")
                title = re.findall(r"<title>(.*?)</title>", feed, re.S)[-1]
                if tokens(title) != tokens(bib_field(block, "title")):
                    notes.append(f"arXiv title differs: {' '.join(title.split())!r}")
            except Exception as exc:
                notes.append(f"arXiv lookup failed ({exc})")
        if not doi and not url and not arxiv:
            notes.append("no DOI or URL to check")
        if notes == ["no DOI or URL to check"] and key in MANUAL:
            notes = []
            print(f"OK    {key} (checked by hand: {MANUAL[key]})")
            continue
        problems += bool(notes)
        print(f"{'OK  ' if not notes else 'CHECK'} {key}" + ("" if not notes else ": " + "; ".join(notes)))
    print(f"\n{len(blocks)} references, {problems} to look at")
    return 0


if __name__ == "__main__":
    sys.exit(main())
