import os
import re
import json
import time
import requests
import xml.etree.ElementTree as ET
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import random

JOCS_PDF_DIR = Path(r"D:\ITMO Big Data & ML School\semester 3\ri3_repo\data\data_files\pdfs\jocs")
TEST_OUTPUT_DIR = Path(r"D:\ITMO Big Data & ML School\semester 3\ri3_repo\data\data_files\parsed\jocs_grobid\all")
GROBID_URL = "http://localhost:8070/api/processFulltextDocument"

TEST_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def extract_keywords_robust(root, ns):
    """Extract and format keywords as comma-separated list."""
    keywords_list = []

    # Multiple XPath locations for keywords
    xpath_patterns = [
        './/tei:profileDesc//tei:keywords//tei:term',
        './/tei:text//tei:keywords//tei:term',
        './/tei:keywords//tei:term',
        './/tei:biblStruct//tei:keywords//tei:term'
    ]

    for xpath in xpath_patterns:
        for term in root.findall(xpath, ns):
            if term.text and term.text.strip():
                kw = term.text.strip()
                keywords_list.append(kw)

    # If no structured terms, try raw keywords text
    if not keywords_list:
        for keywords_node in root.findall('.//tei:keywords', ns):
            raw_text = "".join(keywords_node.itertext()).strip()

            # Remove prefix
            raw_text = re.sub(r'^(Keywords?|Index Terms?|Key words?)[:\s]*', '', 
                             raw_text, flags=re.IGNORECASE)

            if raw_text:
                # Split by common academic keyword separators
                candidates = re.split(r'[\n;]', raw_text)

                for candidate in candidates:
                    kw = candidate.strip()
                    if kw and len(kw) > 2:
                        # Split long phrases by capitalized words
                        sub_words = re.findall(r'\b[A-Z][a-z]+(?:\s+[a-z]+)*(?=\s+[A-Z]|$)', kw)
                        keywords_list.extend(sub_words)

    # Clean, dedupe, format
    keywords_list = [kw.strip() for kw in keywords_list if len(kw) > 2 and not kw.isdigit()]
    keywords_list = list(dict.fromkeys(keywords_list))[:12]  # Unique, max 12
    return ", ".join(keywords_list)

def parse_grobid_xml(xml_string, filename):
    try:
        ns = {'tei': 'http://www.tei-c.org/ns/1.0'}
        root = ET.fromstring(xml_string)

        # Title
        title_node = root.find('.//tei:titleStmt/tei:title', ns)
        title = "".join(title_node.itertext()).strip() if title_node is not None else ""

        # FULL NAMES + AFFILIATIONS
        authors_data = []
        for author in root.findall('.//tei:sourceDesc//tei:author', ns):
            persName = author.find('tei:persName', ns)
            if persName is None: continue

            # Extract ALL name components
            forename_nodes = persName.findall('tei:forename', ns)
            surname_node = persName.find('tei:surname', ns)

            name_parts = []
            for node in persName.iter():
                if node.text and node.tag.endswith('forename'):
                    name_parts.append(node.text.strip())
                elif node.tag.endswith('surname') and node.text:
                    name_parts.append(node.text.strip())

            if not name_parts:
                full_name = "".join(persName.itertext()).strip()
            else:
                full_name = " ".join(name_parts)

            full_name = re.sub(r' [a-z,∗]+$', '', full_name).strip()

            # Extract affiliations
            affils = []
            for affil in author.findall('tei:affiliation', ns):
                parts = [org.text.strip() for org in affil.findall('.//tei:orgName', ns) if org.text]
                addr = [node.text.strip() for node in affil.findall('.//tei:address/*', ns) if node.text]
                combined = ", ".join(list(dict.fromkeys(parts + addr)))
                if combined: 
                    affils.append(combined)

            authors_data.append({
                "name": full_name, 
                "affiliations": affils
            })

        # Abstract
        abstract_node = root.find('.//tei:profileDesc/tei:abstract', ns)
        abstract = "".join(abstract_node.itertext()).strip() if abstract_node is not None else ""

        # KEYWORDS
        keywords_str = extract_keywords_robust(root, ns)

        return {
            "title": title,
            "authors": authors_data,
            "keywords": keywords_str,
            "abstract": abstract,
            "filename": filename
        }
    except Exception as e:
        return {"error": str(e), "filename": filename}

def process_pdf(pdf_path):
    try:
        json_file = TEST_OUTPUT_DIR / f"{pdf_path.stem}.json"

        if json_file.exists():
            return "SKIP"

        with open(pdf_path, 'rb') as f:
            files = {'input': f}
            response = requests.post(
                GROBID_URL, 
                files=files, 
                headers={'Accept': 'application/xml'}, 
                timeout=60
            )

        if response.status_code != 200:
            return f"ERROR_{response.status_code}"

        data = parse_grobid_xml(response.text, pdf_path.name)

        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        return "SUCCESS" if "error" not in data else "PARSE_ERROR"

    except Exception as e:
        return f"CRASH_{str(e)}"

all_pdfs = list(JOCS_PDF_DIR.glob("*.pdf"))
# test_pdfs = random.sample(all_pdfs, 40)
print(f"Found {len(all_pdfs)} PDFs to process")
print(f"Output: {TEST_OUTPUT_DIR}")

success = 0
skipped = 0
errors = 0

start_time = time.time()

with ThreadPoolExecutor(max_workers=4) as executor:
    futures = {executor.submit(process_pdf, p): p for p in all_pdfs}

    for i, future in enumerate(as_completed(futures)):
        result = future.result()

        if result == "SUCCESS":
            success += 1
        elif result == "SKIP":
            skipped += 1
        else:
            errors += 1
            print(f"{futures[future].name}: {result}")

        print(f"[{i+1}/20] {result}")

elapsed = (time.time() - start_time)
print(f"\nTEST COMPLETE in {elapsed:.1f}s")
print(f"Success: {success}/20")
print(f"Skipped: {skipped}/20")
print(f"Errors:  {errors}/20")

if success > 0:
    print(f"Check first result: {TEST_OUTPUT_DIR / list(TEST_OUTPUT_DIR.glob('*.json'))[0]}")