import os
import re
import json
import time
import requests
import xml.etree.ElementTree as ET
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# ICCS_PDF_DIR = Path(r"D:\ITMO Big Data & ML School\semester 3\RI3\notebooks\data\pdfs\icss_truncated\2025")
# ICCS_PARSED_DIR = Path(r"D:\ITMO Big Data & ML School\semester 3\RI3\parsed\iccs_2025_test")
ICCS_PDF_DIR = Path(r"D:\ITMO Big Data & ML School\semester 3\RI3\notebooks\data\pdfs\icss_truncated\2024")
ICCS_PARSED_DIR = Path(r"D:\ITMO Big Data & ML School\semester 3\RI3\parsed\iccs_2024_test")

# Using consolidateHeader=1 to improve metadata accuracy via external lookup
GROBID_URL_HEADER = "http://localhost:8070/api/processHeaderDocument?consolidateHeader=1"

def extract_keywords_robust(root, ns):
    """Extracts keywords using explicit namespace mapping to avoid dictionary bugs."""
    keywords_list = []
    for term in root.findall('.//tei:profileDesc//tei:keywords//tei:term', namespaces=ns):
        if term.text:
            keywords_list.append(term.text.strip())
    
    if not keywords_list:
        for node in root.findall('.//tei:keywords', namespaces=ns):
            text = "".join(node.itertext()).strip()
            # Remove "Keywords" prefix if present
            text = re.sub(r'^(Keywords?|Index Terms?)[:\s]*', '', text, flags=re.IGNORECASE)
            keywords_list.extend([k.strip() for k in re.split(r'[;\n]', text) if len(k.strip()) > 2])

    return ", ".join(list(dict.fromkeys(keywords_list))[:12])

def parse_grobid_xml(xml_string, filename):
    """Parses TEI XML with specific logic for author-affiliation marker matching."""
    try:
        ns = {'tei': 'http://www.tei-c.org/ns/1.0'}
        root = ET.fromstring(xml_string)

        # 1. Map ALL global affiliations first (Fallback for markers)
        global_affils = {}
        for i, aff in enumerate(root.findall('.//tei:sourceDesc//tei:affiliation', ns)):
            aff_id = aff.get('{http://www.w3.org/XML/1998/namespace}id')
            # Extract orgNames (Dept, University, etc.)
            parts = [org.text.strip() for org in aff.findall('.//tei:orgName', ns) if org.text]
            content = ", ".join(parts) if parts else "".join(aff.itertext()).strip()
            
            # Index by 1-based count and by XML ID
            global_affils[str(i+1)] = content
            if aff_id: global_affils[aff_id] = content

        # 2. Extract Authors and resolve their specific affiliations
        authors_data = []
        for author in root.findall('.//tei:sourceDesc//tei:author', ns):
            persName = author.find('tei:persName', ns)
            if persName is None: continue
            
            # --- Name Extraction ---
            first = " ".join([n.text for n in persName.findall('tei:forename', namespaces=ns) if n.text])
            last = persName.findtext('tei:surname', default="", namespaces=ns)
            raw_full_name = f"{first} {last}".strip()

            # --- Marker & ORCID Capture ---
            # Extract ORCID separately
            orcid_match = re.search(r'(\d{4}-\d{4}-\d{4}-\d{3}[0-9X])', raw_full_name)
            orcid = orcid_match.group(1) if orcid_match else None
            
            # Extract numerical marker (e.g., the '1' in 'Misaki Iwai1')
            marker_match = re.search(r'(\d+)', raw_full_name)
            marker = marker_match.group(1) if marker_match else None
            
            # Clean name for final JSON (remove brackets, digits, and ORCIDs)
            clean_name = re.sub(r'\[.*?\]', '', raw_full_name)
            clean_name = re.sub(r'\d+', '', clean_name).strip()

            # --- Affiliation Logic ---
            final_affils = []
            
            # Strategy A: Nested Affiliations (Preferred)
            for aff in author.findall('tei:affiliation', namespaces=ns):
                curr = ", ".join([org.text for org in aff.findall('.//tei:orgName', namespaces=ns) if org.text])
                if not curr: curr = "".join(aff.itertext()).strip()
                if curr: final_affils.append(curr)

            # Strategy B: Marker Match (Fallback)
            if not final_affils and marker and marker in global_affils:
                final_affils.append(global_affils[marker])

            authors_data.append({
                "name": clean_name, 
                "orcid": orcid,
                "affiliations": list(dict.fromkeys(final_affils))
            })

        # 3. Final Metadata Assembly
        title_node = root.find('.//tei:titleStmt/tei:title', ns)
        title = "".join(title_node.itertext()).strip() if title_node is not None else "Unknown Title"
        
        abstract_node = root.find('.//tei:profileDesc/tei:abstract', ns)
        abstract = "".join(abstract_node.itertext()).strip() if abstract_node is not None else ""
        
        keywords = extract_keywords_robust(root, ns)

        return {
            "title": title, 
            "authors": authors_data, 
            "keywords": keywords,
            "abstract": abstract, 
            "filename": filename
        }
    except Exception as e:
        return {"error": str(e), "filename": filename}

def process_pdf(pdf_path):
    """API wrapper with directory handling and retry logic."""
    try:
        # Create output subfolders based on source year
        year_folder = pdf_path.parent.name
        output_folder = ICCS_PARSED_DIR / year_folder
        output_folder.mkdir(parents=True, exist_ok=True)
        
        json_file = output_folder / f"{pdf_path.stem}.json"
        if json_file.exists(): return "SKIP"

        with open(pdf_path, 'rb') as f:
            response = requests.post(
                GROBID_URL_HEADER,
                files={'input': f},
                headers={"Accept": "application/xml"},
                timeout=150  # Generous timeout for consolidation
            )
        
        if response.status_code == 200:
            data = parse_grobid_xml(response.text, pdf_path.name)
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            return "SUCCESS"
        return f"ERROR_{response.status_code}"
    except Exception as e:
        return f"CRASH_{str(e)}"

if __name__ == "__main__":
    if not ICCS_PDF_DIR.exists():
        print(f"Directory not found: {ICCS_PDF_DIR}")
        exit(1)

    all_pdfs = list(ICCS_PDF_DIR.rglob("*.pdf"))
    print(f"--- Starting Processing ---")
    print(f"Target: {len(all_pdfs)} files")
    print(f"Hardware: RTX 3050 (Docker GPU Mode)")

    start_time = time.time()
    success, skipped, errors = 0, 0, 0

    # max_workers=6 is the sweet spot for an RTX 3050 
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {executor.submit(process_pdf, p): p for p in all_pdfs}
        for i, future in enumerate(as_completed(futures)):
            res = future.result()
            if res == "SUCCESS": success += 1
            elif res == "SKIP": skipped += 1
            else: 
                errors += 1
                print(f"Failed: {futures[future].name} -> {res}")
            
            if (i + 1) % 5 == 0 or (i + 1) == len(all_pdfs):
                print(f"Status: [{i+1}/{len(all_pdfs)}] | Success: {success} | Errors: {errors}")

    total_time = time.time() - start_time
    print(f"\n--- Final Report ---")
    print(f"Total Time: {total_time:.2f}s (Avg: {total_time/max(1,success):.2f}s/file)")
    print(f"Results saved to: {ICCS_PARSED_DIR}")