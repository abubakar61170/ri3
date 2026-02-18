import os, json, re, random, time
from pathlib import Path
from multiprocessing import Process, Queue
from datetime import datetime
from dotenv import load_dotenv

from docling.document_converter import DocumentConverter

load_dotenv()
if not os.getenv("JOCS_PDF_DIR") or not os.getenv("JOCS_PARSED_DIR"):
    raise ValueError("Set JOCS_PDF_DIR and JOCS_PARSED_DIR in .env")


PDF_DIR = Path(os.getenv("JOCS_PDF_DIR"))
OUT_DIR = Path(os.getenv("JOCS_PARSED_DIR"))
OUT_DIR.mkdir(parents=True, exist_ok=True)

SUCCESS_LOG = OUT_DIR / "success.log"
ERROR_LOG = OUT_DIR / "error.log"

MAX_SECONDS_PER_PDF = 90
# N_RANDOM = 20 #test


HEADER_MARKER = re.compile(r"locate/jocs\s*\n\n", re.DOTALL)
ARTICLE_INFO_START = re.compile(r"(?i)A\s*R\s*T\s*I\s*C\s*L\s*E\s+I\s*N\s*F\s*O", re.MULTILINE)
ABSTRACT_START = re.compile(r"(?i)^#*\s*A\s*B\s*[S\$]\s*T\s*R\s*A\s*[C€]\s*T", re.MULTILINE)

def clean_markdown_tags(text):
    return re.sub(r"<!--.*?-->", "", text, flags=re.DOTALL).strip()

def _log_line(path: Path, line: str):
    with open(path, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def log_success(fn, seconds, out_name):
    ts = datetime.now().isoformat(timespec="seconds")
    _log_line(SUCCESS_LOG, f"{ts}\t{fn}\t{seconds:.2f}s\t{out_name}")

def log_error(fn, seconds, reason):
    ts = datetime.now().isoformat(timespec="seconds")
    _log_line(ERROR_LOG, f"{ts}\t{fn}\t{seconds:.2f}s\t{reason}")


# worker must be top-level for Windows multiprocessing
def worker_convert_and_parse(pdf_path, out_path, q: Queue):
    try:
        converter = DocumentConverter()
        doc = converter.convert(pdf_path, max_num_pages=1).document
        text = clean_markdown_tags(doc.export_to_markdown())

        # METADATA
        split_header = HEADER_MARKER.split(text)
        title, authors, affiliations = "", "", ""
        if len(split_header) > 1:
            meta_section = ARTICLE_INFO_START.split(split_header[1])[0].strip()
            blocks = [b.strip() for b in meta_section.split("\n\n") if b.strip()]
            if len(blocks) >= 3:
                title = blocks[0].replace("## ", "").replace("##", "").strip()
                authors = blocks[1].strip()
                affiliations = blocks[2].strip()

        # KEYWORDS
        keywords_text = ""
        kw_match = re.search(
            r"(?i)Keywords:\s*(.*?)(?=\n\n|A\s*B\s*S\s*T\s*R\s*A\s*C\s*T|Introduction|©)",
            text, re.DOTALL
        )
        if kw_match:
            raw_content = kw_match.group(1).replace('\n', ' ').strip()
            split_content = re.sub(r'(?<=[a-z0-9])\s+(?=[A-Z])', ', ', raw_content)
            keywords_text = re.sub(r'[_*]', '', split_content).strip()

        # ABSTRACT
        abstract_text = ""
        abs_parts = ABSTRACT_START.split(text)
        if len(abs_parts) > 1:
            after_abstract = abs_parts[1].strip()
            candidate = re.split(r'(?i)Introduction|\n{2,}', after_abstract)[0].strip()
            if len(candidate) < 100:
                candidate = "\n\n".join(after_abstract.split('\n\n')[:2]).strip()
            abstract_text = candidate.strip()

        result = {
            "filename": Path(pdf_path).name,
            "title": title,
            "authors": authors,
            "affiliations": affiliations,
            "keywords": keywords_text,
            "abstract": abstract_text
        }

        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        q.put({"ok": True})

    except Exception as e:
        # Python exceptions will be captured here; native crashes will not.
        q.put({"ok": False, "err": repr(e)})


def main():
    # all_files = [f for f in os.listdir(PDF_DIR) if f.lower().endswith(".pdf")]
    # if len(all_files) > N_RANDOM:
    #     files = random.sample(all_files, N_RANDOM)
    # else:
    #     files = all_files
    
    files = [f for f in os.listdir(PDF_DIR) if f.lower().endswith(".pdf")]


    print(f"Testing {len(files)} random PDFs...")

    for i, fn in enumerate(files, start=1):
        pdf_path = os.path.join(PDF_DIR, fn)
        out_path = str(OUT_DIR / f"{Path(fn).stem}.json")

        t0 = time.time()
        q = Queue()
        p = Process(target=worker_convert_and_parse, args=(pdf_path, out_path, q), daemon=True)

        print(f"[{i}/{len(files)}] {fn}", end=" ... ")
        p.start()
        p.join(timeout=MAX_SECONDS_PER_PDF)

        elapsed = time.time() - t0

        if p.is_alive():
            p.terminate()
            p.join(timeout=5)
            log_error(fn, elapsed, f"TIMEOUT>{MAX_SECONDS_PER_PDF}s")
            print(f"TIMEOUT ({elapsed:.2f}s)")
            continue

        # If the worker died due to a native crash, queue will likely be empty.
        if q.empty():
            log_error(fn, elapsed, "WORKER_CRASHED (native PDF/parser crash)")
            print(f"CRASHED ({elapsed:.2f}s)")
            continue

        msg = q.get()
        if msg.get("ok"):
            log_success(fn, elapsed, Path(out_path).name)
            print(f"OK ({elapsed:.2f}s)")
        else:
            log_error(fn, elapsed, msg.get("err", "Unknown error"))
            print(f"ERROR ({elapsed:.2f}s)")

    print("Done.")
    print("Success log:", SUCCESS_LOG)
    print("Error log:", ERROR_LOG)


if __name__ == "__main__":
    main()