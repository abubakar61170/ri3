import os, re, json, time
from pathlib import Path
from datetime import datetime
import pdfplumber
from dotenv import load_dotenv

load_dotenv()
if not os.getenv("JOCS_PDF_DIR") or not os.getenv("JOCS_PARSED_DIR"):
    raise ValueError("Set JOCS_PDF_DIR and JOCS_PARSED_DIR in .env")



PDF_DIR = Path(os.getenv("JOCS_PDF_DIR"))
PARSED_DIR = Path(os.getenv("JOCS_PARSED_DIR"))
ERROR_LOG = PARSED_DIR / "error.log"

OUT_DIR = PARSED_DIR.parent / "jocs_fallback_timeouts_pdfplumber"
OUT_DIR.mkdir(parents=True, exist_ok=True)

STAGE_TIMEOUT_ERROR_LOG = OUT_DIR / "still_error_timeouts_stage2.log"



# SUPERSCRIPT
SUP_TO_DIGIT = str.maketrans("⁰¹²³⁴⁵⁶⁷⁸⁹", "0123456789")
DIGIT_TO_SUP = str.maketrans("0123456789", "⁰¹²³⁴⁵⁶⁷⁸⁹")


# REGEXES
RE_ARTINFO = re.compile(
    r"(ARTICLE\s+INFO|A\s*R\s*T\s*I\s*C\s*L\s*E\s+I\s*N\s*F\s*O|a\s*r\s*t\s*i\s*c\s*l\s*e\s+i\s*n\s*f\s*o)",
    re.I
)
RE_ABS_HDR = re.compile(
    r"(ABSTRACT|A\s*B\s*S\s*T\s*R\s*A\s*C\s*T|a\s*b\s*s\s*t\s*r\s*a\s*c\s*t)",
    re.I
)
RE_STOP_ANY = re.compile(
    r"(ARTICLE\s+INFO|ABSTRACT|Keywords|Key\s*words|Article\s+history|Received|Accepted|Available\s+online)",
    re.I
)

# Stop for abstract/keywords extraction
RE_INTRO = re.compile(r"(^|\s)(Introduction|1\.\s*Introduction)\b", re.I)
RE_COPY = re.compile(r"(©|Copyright)", re.I)
RE_KEYWORDS_HDR = re.compile(r"\b(Key\s*words|Keywords)\b", re.I)


# UTILITIES
AFF_CUES = re.compile(
    r"\b(Department|Dept\. ?|School|Institute|Faculty|Center|Centre|Laboratory|Lab|University|Universit|College|Hospital|Academy|Research\s+Group|IIT|MIT|ETH|CNRS|INRIA)\b",
    re.I
)
AFF_CUES_MERGED = re.compile(
    r"(Department|School|Institute|Faculty|Center|Centre|Laboratory|University|College|Hospital|Academy)",
    re.I
)

def fix_merged_text(text: str) -> str:
    if not text:
        return ""
    text = text.strip()
    words = text.split()
    if len(words) >= 3:
        return text
    if len(text) > 15 and len(words) <= 2:
        fixed = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)
        fixed = re.sub(r'\b(of|and|the|for|in|at|to|with|from|by|de|la|le|des|du)([A-Z])', r'\1 \2', fixed, flags=re.I)
        fixed = re.sub(r'\b(of)(the|a|an)\b', r'\1 \2', fixed, flags=re.I)
        return fixed
    if len(words) == 1 and len(text) > 10:
        fixed = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)
        return fixed
    return text

def is_likely_affiliation(text: str) -> bool:
    if not text:
        return False
    t = text.strip()
    t_collapsed = re.sub(r"\s+", "", t).lower()
    aff_keywords = [
        "department","school","institute","faculty","center","centre","laboratory",
        "university","college","hospital","academy","iit","mit","eth","cnrs","inria"
    ]
    for kw in aff_keywords:
        if kw in t_collapsed:
            return True
    if re.search(r"\d{5,6}", t):
        return True
    if re.search(r"\b(USA|UK|India|China|Germany|France|Japan|Canada|Australia)\b", t, re.I):
        return True
    return False

def is_header_banner(text: str) -> bool:
    if not text:
        return False
    t = text.strip()
    t_lower = t.lower()
    t_collapsed = re.sub(r"\s+", "", t_lower)
    if len(t) < 3:
        return False
    if "journalofcomputationalscience" in t_collapsed:
        return True
    if t_lower.startswith("journal of computational science"):
        return True
    if re.match(r"^journal\s+of\s+computational\s+science\s*$", t_lower):
        return True
    if re.search(r"journal\s+of\s+computational\s+science\s+\d+\s*\(\d{4}\)", t_lower):
        return True
    if re.match(r"^\d+\s*\(\d{4}\)\s*\d+[\s–\-]*\d*$", t.strip()):
        return True
    for pattern in [
        "sciencedirect","contents lists available","contentslistsavailable",
        "journal homepage","journalhomepage","www.elsevier.com","elsevier.com/locate",
        "check for updates","checkforupdates","crossmark"
    ]:
        if pattern in t_collapsed or pattern in t_lower:
            return True
    if t_collapsed in {"sciencedirect", "elsevier"}:
        return True
    return False

def find_header_end_y(lines, page_height):
    banner_y_positions = []
    for li in lines:
        if li["y"] > page_height * 0.35:
            break
        if is_header_banner(li["text"]):
            banner_y_positions.append(li["y"])
    return (max(banner_y_positions) + 5) if banner_y_positions else (page_height * 0.10)

def cluster_lines(words, y_tol=3.0, x_tol=3.0):
    """
    Improved clustering:
    - Added x_tol to preserve spaces between tight words.
    - Added sorting to ensure lines are read left-to-right.
    """
    buckets = []
    for w in words:
        y = w["top"]
        for b in buckets:
            if abs(b["y"] - y) <= y_tol:
                b["ws"].append(w)
                break
        else:
            buckets.append({"y": y, "ws": [w]})

    lines = []
    for b in sorted(buckets, key=lambda x: x["y"]):
        # Sort words by x0 (left-most position)
        ws = sorted(b["ws"], key=lambda x: x["x0"])
        
        # Build text with explicit space handling
        parts = []
        for i, w in enumerate(ws):
            parts.append(w["text"])
            # If the next word is significantly far, ensure space (optional)
            if i < len(ws)-1 and (ws[i+1]["x0"] - w["x1"]) > x_tol:
                parts.append(" ")
        
        text = " ".join(parts).strip()
        text = re.sub(r'\s+', ' ', text) # Clean double spaces
        
        if text:
            lines.append({
                "y": b["y"],
                "text": text,
                "max_size": max(w.get("size", 0) for w in ws)
            })
    return lines

def find_y(lines, regex):
    for li in lines:
        if regex.search(li["text"]):
            return li["y"]
    return None


# AUTHOR PARSING
def extract_tokens_from_lines(lines):
    tokens = []
    for li in lines:
        tokens.extend(re.findall(r"[A-Za-zÀ-ž]+|[0-9]+|[⁰¹²³⁴⁵⁶⁷⁸⁹]|[,.*∗⁎]", li["text"]))
    return tokens

def finalize_author(cur):
    name = " ".join(cur["name_parts"]).strip()
    name = fix_merged_text(name)
    return {"name": name, "markers": sorted(cur["markers"]), "corresponding": cur["corresponding"]}

def parse_authors_with_markers(author_lines):
    tokens = extract_tokens_from_lines(author_lines)
    authors = []
    cur = {"name_parts": [], "markers": set(), "corresponding": False}

    for tok in tokens:
        if tok == ",":
            if cur["name_parts"]:
                a = finalize_author(cur)
                if a["name"] and not is_likely_affiliation(a["name"]):
                    authors.append(a)
                cur = {"name_parts": [], "markers": set(), "corresponding": False}
            continue

        if tok in {"*", "∗", "⁎"}:
            cur["corresponding"] = True
            continue

        if re.fullmatch(r"[a-z]", tok):
            cur["markers"].add(tok)
            continue

        if re.fullmatch(r"[0-9]+", tok):
            cur["markers"].add(tok)
            continue

        if tok in "⁰¹²³⁴⁵⁶⁷⁸⁹":
            cur["markers"].add(tok.translate(SUP_TO_DIGIT))
            continue

        cur["name_parts"].append(tok)

    if cur["name_parts"]:
        a = finalize_author(cur)
        if a["name"] and not is_likely_affiliation(a["name"]):
            authors.append(a)

    return authors

def parse_authors_without_markers(author_text):
    author_text = (author_text or "").strip()
    if not author_text:
        return [], ""

    extracted_aff = ""
    aff_match = AFF_CUES.search(author_text)
    if aff_match:
        author_part = author_text[:aff_match.start()].strip(" ,")
        extracted_aff = author_text[aff_match.start():].strip()
        author_text = author_part

    if not extracted_aff:
        aff_match_merged = AFF_CUES_MERGED.search(author_text)
        if aff_match_merged:
            author_part = author_text[:aff_match_merged.start()].strip(" ,")
            extracted_aff = author_text[aff_match_merged.start():].strip()
            author_text = author_part

    author_text = fix_merged_text(author_text)
    parts = re.split(r"\s*,\s*", author_text)
    authors = []

    for p in parts:
        p = p.strip()
        if not p:
            continue
        if re.fullmatch(r"\d+", p):
            continue

        if is_likely_affiliation(p):
            extracted_aff = (extracted_aff + ", " + p).strip(", ") if extracted_aff else p
            continue

        corresponding = bool(re.search(r"[*∗⁎]", p))
        p = re.sub(r"[*∗⁎]", "", p).strip()
        p = fix_merged_text(p)

        if is_likely_affiliation(p):
            extracted_aff = (extracted_aff + ", " + p).strip(", ") if extracted_aff else p
            continue

        if p:
            authors.append({"name": p, "markers": [], "corresponding": corresponding})

    extracted_aff = fix_merged_text(extracted_aff)
    return authors, extracted_aff


# AFFILIATIONS PARSING
def parse_affiliations_with_markers(aff_lines):
    affiliations, last_key = {}, None
    for li in aff_lines:
        t = li["text"].strip()
        if not t:
            continue
        if RE_ARTINFO.search(t) or RE_ABS_HDR.search(t) or RE_STOP_ANY.search(t):
            break

        m = re.match(r"^\s*([a-z])\s+(.*)$", t)
        if m:
            last_key = m.group(1)
            affiliations[last_key] = fix_merged_text(m.group(2).strip())
            continue

        m2 = re.match(r"^\s*([a-z])([A-Z]. *)$", t)
        if m2:
            last_key = m2.group(1)
            affiliations[last_key] = fix_merged_text(m2.group(2).strip())
            continue

        if last_key:
            affiliations[last_key] = (affiliations[last_key] + " " + fix_merged_text(t)).strip()

    return affiliations

def parse_affiliations_without_markers(aff_text):
    aff_text = (aff_text or "").strip()
    if not aff_text:
        return {}
    if RE_ARTINFO.search(aff_text) or RE_ABS_HDR.search(aff_text):
        return {}
    aff_text = re.sub(r"\s+", " ", aff_text).strip()
    aff_text = fix_merged_text(aff_text)
    return {"all": aff_text} if aff_text else {}


# FRONT MATTER PARSER (PAGE 1)
def parse_front_matter_page1(pdf_path: Path):
    with pdfplumber.open(str(pdf_path)) as pdf:
        page = pdf.pages[0]
        H = page.height

        words = page.extract_words(extra_attrs=["size"], use_text_flow=True) or []
        if not words:
            return {"title": "", "authors": [], "affiliations": {}}

        lines = cluster_lines(words, y_tol=3.0)

        header_end_y = find_header_end_y(lines, H)
        absinfo_y = find_y(lines, RE_ARTINFO) or find_y(lines, RE_ABS_HDR)
        if absinfo_y is None:
            absinfo_y = H * 0.62

        content_start_y = header_end_y
        content_end_y = min(absinfo_y, H * 0.45)

        top = [li for li in lines
               if content_start_y <= li["y"] < content_end_y
               and not is_header_banner(li["text"])]

        if not top:
            return {"title": "", "authors": [], "affiliations": {}}

        max_font = max(li["max_size"] for li in top)
        title_candidates = [li for li in top
                            if li["max_size"] >= max_font - 1.5
                            and not is_header_banner(li["text"])]
        title_candidates.sort(key=lambda x: x["y"])

        title_lines, last_y = [], None
        for li in title_candidates:
            if is_header_banner(li["text"]):
                continue
            if last_y is None or abs(li["y"] - last_y) <= 20:
                title_lines.append(li)
                last_y = li["y"]
            else:
                break

        title = " ".join(li["text"] for li in title_lines).strip()
        title = fix_merged_text(title)
        title_bottom_y = title_lines[-1]["y"] if title_lines else content_start_y

        zone = [li for li in lines
                if title_bottom_y + 5 < li["y"] < absinfo_y
                and not is_header_banner(li["text"])]

        has_markers = False
        aff_start = None
        for i, li in enumerate(zone):
            t = li["text"].strip()
            if RE_ARTINFO.search(t) or RE_ABS_HDR.search(t) or RE_STOP_ANY.search(t):
                aff_start = i
                break
            if re.match(r"^\s*[a-z]\s+[A-Z]", t) or re.match(r"^\s*[a-z][A-Z]", t):
                has_markers = True
                aff_start = i
                break
            if AFF_CUES.search(t):
                has_markers = False
                aff_start = i
                break

        if aff_start is None:
            author_lines = zone[:2]
            aff_lines = zone[2:]
        else:
            author_lines = zone[:aff_start]
            aff_lines = zone[aff_start:]

        author_text = " ".join(li["text"] for li in author_lines).strip()

        if has_markers:
            authors = parse_authors_with_markers(author_lines)
            affiliations = parse_affiliations_with_markers(aff_lines)
        else:
            authors, extra_aff = parse_authors_without_markers(author_text)
            aff_text = " ".join(li["text"] for li in aff_lines).strip()
            combined_aff = (extra_aff + " " + aff_text).strip() if extra_aff or aff_text else ""
            affiliations = parse_affiliations_without_markers(combined_aff)

        return {"title": title, "authors": authors, "affiliations": affiliations}



# KEYWORDS + ABSTARCT
def extract_kw_abs_from_lines(lines):
    clean_lines = [li for li in lines if not is_header_banner(li["text"])]

    kw_idx = None
    abs_idx = None
    intro_idx = None

    for i, li in enumerate(clean_lines):
        t = li["text"]
        if kw_idx is None and RE_KEYWORDS_HDR.search(t):
            kw_idx = i
        if abs_idx is None and RE_ABS_HDR.search(t):
            abs_idx = i
        if intro_idx is None and RE_INTRO.search(t):
            intro_idx = i

    # KEYWORDS
    keywords_text = ""
    if kw_idx is not None:
        if abs_idx is not None and abs_idx > kw_idx:
            end_kw = abs_idx
        elif intro_idx is not None and intro_idx > kw_idx:
            end_kw = intro_idx
        else:
            end_kw = len(clean_lines)

        kw_lines = clean_lines[kw_idx:end_kw]
        first_line = kw_lines[0]["text"]
        m = re.search(r"(?i)\bKeywords?\b\s*:?\s*(.*)$", first_line)
        parts = [m.group(1).strip()] if m and m.group(1) else []

        for li in kw_lines[1:]:
            t = li["text"].strip()
            if RE_ABS_HDR.search(t) or RE_INTRO.search(t):
                break
            parts.append(t)

        if parts:
            keywords_text = re.sub(r"\s+", " ", " ".join(parts)).strip()

    # ABSTRACT
    abstract_text = ""
    if abs_idx is not None:
        if intro_idx is not None and intro_idx > abs_idx:
            end_abs = intro_idx
        else:
            end_abs = len(clean_lines)

        abs_lines = clean_lines[abs_idx:end_abs]
        first_line = abs_lines[0]["text"]
        m = re.search(r"(?i)\bAbstract\b\s*:?\s*(.*)$", first_line)
        parts = [m.group(1).strip()] if m and m.group(1) else []

        for li in abs_lines[1:]:
            t = li["text"].strip()
            if RE_INTRO.search(t):
                break
            parts.append(t)

        if parts:
            abstract_text = re.sub(r"\s+", " ", " ".join(parts)).strip()

    return keywords_text, abstract_text


# FLATTENERS (FLAT JSON OUTPUT)
def flatten_authors(authors):
    if not authors:
        return ""
    parts = []
    for a in authors:
        name = (a.get("name") or "").strip()
        if not name:
            continue

        markers = a.get("markers") or []
        numeric = "".join(m for m in markers if re.fullmatch(r"\d+", m))
        if numeric:
            name = name + numeric.translate(DIGIT_TO_SUP)

        if a.get("corresponding"):
            name = name + " ∗"

        parts.append(name)

    return ", ".join(parts).strip()

def flatten_affiliations(affiliations):
    if not affiliations:
        return ""
    if isinstance(affiliations, dict):
        if "all" in affiliations and isinstance(affiliations["all"], str):
            return affiliations["all"].strip()
        items = []
        for k in sorted(affiliations.keys()):
            v = affiliations.get(k)
            if isinstance(v, str) and v.strip():
                # keep marker label so mapping isn't lost
                items.append(f"{k} {v.strip()}")
        return " ".join(items).strip()
    return str(affiliations).strip()


# TIMEOUT FILES FROM error.log
def load_timeout_filenames(error_log_path: Path):
    if not error_log_path.exists():
        raise FileNotFoundError(f"error.log not found: {error_log_path}")

    files, seen = [], set()
    with open(error_log_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if "TIMEOUT>90s" not in line:
                continue
            parts = line.split("\t")
            if len(parts) < 2:
                continue
            fn = parts[1].strip()
            if fn.lower().endswith(".pdf") and fn not in seen:
                seen.add(fn)
                files.append(fn)
    return files

def log_stage2_error(fn: str, reason: str):
    ts = datetime.now().isoformat(timespec="seconds")
    with open(STAGE_TIMEOUT_ERROR_LOG, "a", encoding="utf-8") as f:
        f.write(f"{ts}\t{fn}\t{reason}\n")


# MAIN
def main():
    files = load_timeout_filenames(ERROR_LOG)
    print(f"Found {len(files)} TIMEOUT PDFs in error.log")
    print(f"Output folder: {OUT_DIR}")
    print(f"Stage2 error log: {STAGE_TIMEOUT_ERROR_LOG}\n")

    ok = 0
    err = 0
    missing = 0

    for i, fn in enumerate(files, start=1):
        pdf_path = PDF_DIR / fn
        out_path = OUT_DIR / f"{pdf_path.stem}.json"

        print(f"[{i}/{len(files)}] {fn}", end=" ... ")
        t0 = time.time()

        if not pdf_path.exists():
            missing += 1
            log_stage2_error(fn, "FILE_NOT_FOUND")
            print("MISSING")
            continue

        try:
            # meta
            fm = parse_front_matter_page1(pdf_path)

            # layout lines for kw/abs
            with pdfplumber.open(str(pdf_path)) as pdf:
                page = pdf.pages[0]
                words = page.extract_words(extra_attrs=["size"], use_text_flow=True) or []
                lines = cluster_lines(words, y_tol=3.0) if words else []

            keywords_text, abstract_text = extract_kw_abs_from_lines(lines)

            result = {
                "filename": fn,
                "title": (fm.get("title") or ""),
                "authors": flatten_authors(fm.get("authors", [])),
                "affiliations": flatten_affiliations(fm.get("affiliations", {})),
                "keywords": (keywords_text or ""),
                "abstract": (abstract_text or "")
            }

            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False, indent=2)

            ok += 1
            print(f"OK ({time.time() - t0:.2f}s)")

        except Exception as e:
            err += 1
            reason = repr(e)
            log_stage2_error(fn, reason)
            print(f"ERROR ({time.time() - t0:.2f}s) -> {reason[:160]}")

    print("\nDone.")
    print(f"OK: {ok}")
    print(f"ERROR: {err}")
    print(f"MISSING: {missing}")
    print(f"Outputs: {OUT_DIR}")
    print(f"Stage2 errors: {STAGE_TIMEOUT_ERROR_LOG}")


if __name__ == "__main__":
    main()
