# Modeling topics, researcher profiles, and methods for assessing the quality of articles based on scientific publication data (NLP)

## Overview

This repository contains the experimental and data engineering pipeline developed for analyzing scientific publication ecosystems using **ICCS (International Conference on Computational Science)** and **JoCS (Journal of Computational Science)** datasets.

The project focuses on building a structured corpus combining metadata, full-text signals, and author information to support downstream research tasks such as:

* Modeling thematic evolution of research communities
* Detecting semantic relationships between conference and journal publications
* Constructing co-authorship and instituional collaboration networks
* Supporting experiments in semantic similarity, topic modeling, and publication trajectory analysis

The repository primarily represents the **data acquisition, enrichment, parsing, and exploratory experimentation layer** of the broader research workflow.

---

## Repository Structure

```
ri3-main/
│
├── data/
│   └── code/
│       ├── fetch_dblp.py                     # DBLP metadata collection and DOI extraction
│       ├── data_quality.ipynb                # Data inspection and validation
│       ├── iccs_openalex.ipynb               # ICCS OpenAlex ingestion & enrichment
│       ├── iccs_openalex_authors.ipynb       # ICCS author-level enrichment
│       ├── jocs_openalex.ipynb               # JoCS OpenAlex ingestion & enrichment
│       ├── jocs_openalex_authors.ipynb       # JoCS author-level enrichment
│       ├── pdfs_download.ipynb               # Automated PDF acquisition
│       │
│       └── parsing/
│           ├── iccs_test_grobid.py           # ICCS GROBID parsing test
│           ├── jocs_grobid_test.py           # JoCS GROBID parsing test
│           ├── parse_jocs_docling.py         # Document parsing via Docling
│           ├── parse_jocs_fallback_pdfplumber.py  # Fallback PDF parsing
│           └── testing_parsing_iccs.ipynb    # ICCS parsing experimentation
│
├── new experiments.ipynb                     # Ongoing experimental notebook
├── previous_experiment.ipynb                 # Archived experiments
└── .gitignore
```

---

## Pipeline Conceptual Workflow

The repository implements a staged data pipeline:

### 1. Metadata Acquisition

* DBLP API querying for ICCS and JoCS publications
* DOI normalization and extraction
* JSONL metadata persistence

### 2. Cross-source Enrichment

* OpenAlex metadata integration
* Author disambiguation and affiliation enrichment
* Citation and topic augmentation

### 3. Full-text Acquisition

* DOI-based PDF downloading
* Coverage verification and logging

### 4. Document Parsing

* Structured extraction via GROBID
* Alternative parsing via Docling and pdfplumber
* Generation of machine-readable text representations

### 5. Experimental Analysis

* Semantic similarity experiments
* Topic and classification experiments
* Corpus quality and coverage diagnostics

---

## Requirements

Typical environment:

* Python ≥ 3.9
* requests
* tqdm
* python-dotenv
* pandas / numpy
* GROBID (external service)
* Jupyter

Install core dependencies:

```bash
pip install requests tqdm python-dotenv pandas numpy jupyter
```

Additional parsing tools may require separate installation.

---

## Environment Configuration

The pipeline relies on `.env` configuration for data directories.

Example:

```
DBLP_DIR=/path/to/dblp_storage
JOCS_PDF_DIR=/path/to/jocs_pdfs
ICCS_PDF_DIR=/path/to/iccs_pdfs
GROBID_OUTPUT_DIR=/path/to/grobid_outputs
```

Directory structure is expected to contain `raw/` and `interim/` subfolders for staging data.

---

## Usage

### Fetch DBLP metadata

```bash
python data/code/fetch_dblp.py
```

This step retrieves publication records and extracts DOI information for downstream enrichment.

### Run enrichment and parsing

Use notebooks in `data/code/` sequentially:

1. OpenAlex ingestion notebooks
2. Author enrichment notebooks
3. PDF download notebook
4. Parsing notebooks

Execution order corresponds to the pipeline stages described above.

---

## Notes

* Notebooks represent exploratory and experimental stages; reproducibility scripts may be consolidated separately.
* Parsing quality depends on PDF structure and availability.
* This repository focuses on **data preparation and exploratory analysis**, not final modeling artifacts.

---

## Research Context

* Computational modeling of scientific publication ecosystems
* Semantic conference-to-journal transformation analysis

---



**Repository status**: active research development
**Version**: experimental