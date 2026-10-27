"""Phase 11 -- a local text-similarity check of the paper (a proxy, not Turnitin/iThenticate).

What it does
------------
1. Extracts the prose of ``paper/sections/*.tex`` (LaTeX commands, maths, comments,
   figure/table environments and the back matter removed) and splits it into sentences.
2. Builds a comparison corpus from
   * the abstracts of every cited reference, fetched from OpenAlex by DOI (or by title
     when there is no DOI) -- this is where copied related-work wording would come from;
   * the author's own earlier documents in ``literature/`` (literature review DOCX and
     the planning Markdown files), which may already be in a plagiarism database.
3. Marks every paper word that lies inside a run of ``N`` consecutive words (default 8)
   that also appears in a source, and reports
   * an overall similarity index (share of paper words inside a matched run),
   * the index per source, and
   * each flagged sentence with its matching source.

Only the Python standard library is used. Fetched abstracts are cached in
``tools/.cache/`` (git-ignored) so a re-run works offline.

Limits: abstracts are not full texts, and web pages and student-paper databases are not
searched. The official check of the venue (iThenticate / Turnitin) is still needed.

Usage::

    python tools/similarity_check.py            # 8-word runs, writes docs/similarity_report.md
    python tools/similarity_check.py --n 6
"""

from __future__ import annotations

import argparse
import json
import re
import time
import urllib.parse
import urllib.request
import zipfile
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SECTIONS = sorted((ROOT / "paper" / "sections").glob("*.tex"))
BIB = ROOT / "paper" / "references.bib"
OWN_DOCS = [ROOT / "literature" / "Literature_Review_Shaikh.docx",
            ROOT / "literature" / "CAO-DF_Project_Master_Document.md",
            ROOT / "literature" / "Final_Execution_Plan.md",
            ROOT / "literature" / "Phase0_Technical_Understanding.md"]
CACHE = ROOT / "tools" / ".cache" / "openalex_abstracts.json"
REPORT = ROOT / "docs" / "similarity_report.md"
SKIP_FILES = {"10_backmatter.tex"}
SKIP_ENVS = ("figure", "figure*", "table", "table*", "equation", "equation*", "align", "align*",
             "algorithmic", "tabular")


# --- Text extraction --------------------------------------------------------------

def latex_to_text(tex: str) -> str:
    tex = re.sub(r"(?<!\\)%.*", "", tex)
    for env in SKIP_ENVS:
        e = re.escape(env)
        tex = re.sub(rf"\\begin\{{{e}\}}.*?\\end\{{{e}\}}", " ", tex, flags=re.S)
    tex = re.sub(r"\$\$.*?\$\$|\\\[.*?\\\]|\$[^$]*\$", " ", tex, flags=re.S)
    tex = re.sub(r"\\(cite|ref|label|eqref|url|includegraphics)\*?(\[[^\]]*\])?\{[^}]*\}", " ", tex)
    tex = re.sub(r"\\(begin|end)\{[^}]*\}", " ", tex)
    tex = re.sub(r"\\(section|subsection|subsubsection|paragraph)\*?\{([^}]*)\}", r"\2. ", tex)
    for _ in range(3):
        tex = re.sub(r"\\[a-zA-Z]+\*?(\[[^\]]*\])?\{([^{}]*)\}", r"\2", tex)
    tex = re.sub(r"\\[a-zA-Z]+\*?", " ", tex)
    tex = tex.replace("~", " ").replace("--", "-").replace("``", '"').replace("''", '"')
    tex = re.sub(r"[{}\\]", " ", tex)
    return re.sub(r"\s+", " ", tex).strip()


def sentences(text: str) -> list[str]:
    parts = re.split(r"(?<=[.!?])\s+(?=[A-Z(])", text)
    return [p.strip() for p in parts if len(p.split()) >= 4]


def tokens(text: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", text.lower())


def docx_text(path: Path) -> str:
    with zipfile.ZipFile(path) as z:
        xml = z.read("word/document.xml").decode("utf-8")
    return re.sub(r"<[^>]+>", "", xml.replace("</w:p>", "\n"))


# --- Reference abstracts ----------------------------------------------------------

def bib_field(block: str, name: str) -> str:
    m = re.search(rf"\b{name}\s*=\s*\{{(.*?)\}}\s*,?\s*\n", block, re.S)
    return re.sub(r"\s+", " ", re.sub(r"[{}]", "", m[1])).strip() if m else ""


def bib_entries(path: Path) -> list[dict]:
    entries = []
    for block in re.split(r"\n(?=@)", path.read_text(encoding="utf-8")):
        key = re.match(r"@\w+\{([^,]+),", block.strip())
        if key:
            entries.append({"key": key[1].strip(), "title": bib_field(block, "title"),
                            "doi": bib_field(block, "doi")})
    return entries


def openalex(url: str) -> dict | None:
    req = urllib.request.Request(url, headers={"User-Agent": "cao-df-similarity-check"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.load(r)
    except Exception:
        return None


def abstract_of(work: dict | None) -> str:
    index = (work or {}).get("abstract_inverted_index") or {}
    words = sorted((pos, word) for word, positions in index.items() for pos in positions)
    return " ".join(word for _, word in words)


def fetch_abstracts(entries: list[dict], refresh: bool) -> dict[str, dict]:
    cache = json.loads(CACHE.read_text(encoding="utf-8")) if CACHE.exists() and not refresh else {}
    for e in entries:
        if e["key"] in cache:
            continue
        work = None
        if e["doi"]:
            work = openalex("https://api.openalex.org/works/doi:" + urllib.parse.quote(e["doi"]))
        if work is None and e["title"]:
            found = openalex("https://api.openalex.org/works?per-page=1&search=" + urllib.parse.quote(e["title"]))
            hit = (found or {}).get("results") or []
            if hit and tokens(hit[0].get("title") or "")[:6] == tokens(e["title"])[:6]:
                work = hit[0]
        cache[e["key"]] = {"title": e["title"], "found": work is not None, "abstract": abstract_of(work)}
        time.sleep(0.2)
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    CACHE.write_text(json.dumps(cache, indent=1), encoding="utf-8")
    return cache


# --- Matching ---------------------------------------------------------------------

def ngrams(words: list[str], n: int) -> set[tuple[str, ...]]:
    return {tuple(words[i:i + n]) for i in range(len(words) - n + 1)}


def match(paper_sents: list[str], sources: dict[str, str], n: int):
    """Return total words, matched words, matched words per source and flagged sentences."""
    source_grams = {name: ngrams(tokens(text), n) for name, text in sources.items()}
    total_words = matched_words = 0
    per_source = dict.fromkeys(sources, 0)
    flagged = []
    for sent in paper_sents:
        words = tokens(sent)
        total_words += len(words)
        covered: set[int] = set()
        hits: dict[str, set[int]] = {}
        for i in range(len(words) - n + 1):
            gram = tuple(words[i:i + n])
            for name, grams in source_grams.items():
                if gram in grams:
                    covered.update(range(i, i + n))
                    hits.setdefault(name, set()).update(range(i, i + n))
        matched_words += len(covered)
        for name, idx in hits.items():
            per_source[name] += len(idx)
        if hits:
            flagged.append((sent, len(covered) / len(words), sorted(hits)))
    return total_words, matched_words, per_source, flagged


# --- Report -----------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Local n-gram similarity check of the paper.")
    parser.add_argument("--n", type=int, default=8, help="words per matching run (default 8)")
    parser.add_argument("--refresh", action="store_true", help="re-download the reference abstracts")
    args = parser.parse_args()

    text = " ".join(latex_to_text(p.read_text(encoding="utf-8")) for p in SECTIONS if p.name not in SKIP_FILES)
    paper_sents = sentences(text)

    entries = bib_entries(BIB)
    abstracts = fetch_abstracts(entries, args.refresh)
    refs = {f"[ref] {k}": v["abstract"] for k, v in abstracts.items() if v["abstract"]}
    own = {f"[own] {d.name}": docx_text(d) if d.suffix == ".docx" else d.read_text(encoding="utf-8")
           for d in OWN_DOCS if d.exists()}

    total, matched, per_source, flagged = match(paper_sents, {**refs, **own}, args.n)
    _, matched_refs, _, _ = match(paper_sents, refs, args.n)
    missing = [k for k, v in abstracts.items() if not v["abstract"]]

    lines = [
        "# Similarity check of the CAO-DF paper (local proxy)",
        "",
        f"Generated by `python tools/similarity_check.py --n {args.n}` on {date.today().isoformat()}.",
        "",
        "> **This is not an official plagiarism check.** It compares the paper only with the",
        "> abstracts of the cited works (from OpenAlex) and with the author's own earlier documents.",
        "> Full texts, web pages and student-paper databases are not searched. Run the venue's",
        "> iThenticate / Turnitin check before submission.",
        "",
        "## Summary",
        "",
        "| Measure | Value |",
        "|---|---:|",
        f"| Paper sentences checked (abstract + Sections I-IX, no back matter) | {len(paper_sents)} |",
        f"| Paper words checked | {total} |",
        f"| Matching run length | {args.n} consecutive words |",
        f"| Cited works with an abstract available | {len(refs)} of {len(entries)} |",
        f"| **Similarity index vs. cited-work abstracts** | **{matched_refs / total:.1%}** |",
        f"| Similarity index vs. all sources (incl. own documents) | {matched / total:.1%} |",
        f"| Sentences with any match | {len(flagged)} |",
        "",
        "## Matched words per source",
        "",
        "| Source | Matched words | Share of paper |",
        "|---|---:|---:|",
    ]
    rows = [f"| {name} | {w} | {w / total:.1%} |" for name, w in sorted(per_source.items(), key=lambda kv: -kv[1]) if w]
    lines += rows or ["| (none) | 0 | 0.0% |"]
    lines += ["", "## Flagged sentences", ""]
    if flagged:
        lines += ["| # | Paper sentence | Words matched | Source(s) |", "|---:|---|---:|---|"]
        lines += [f"| {i} | {s.replace('|', '/')} | {share:.0%} | {', '.join(names)} |"
                  for i, (s, share, names) in enumerate(flagged, 1)]
    else:
        lines.append("None.")
    lines += ["", "## Cited works without an abstract in OpenAlex", ""]
    lines += [f"- `{k}` -- {abstracts[k]['title']}" for k in missing] or ["None."]
    REPORT.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"{len(paper_sents)} sentences, {total} words; n = {args.n}")
    print(f"similarity vs cited abstracts: {matched_refs / total:.1%} ({len(refs)}/{len(entries)} abstracts)")
    print(f"similarity vs all sources:     {matched / total:.1%}; flagged sentences: {len(flagged)}")
    print(f"wrote {REPORT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
