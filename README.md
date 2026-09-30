# Data Scraping Suite

A production-grade collection of asynchronous scrapers, web crawlers, and dataset extractors. Designed to collect, parse, and structure data from **Medium publications, GitHub repositories, Excel workbooks, and specialized cybersecurity threat intelligence hubs**.

---

## 📂 Repository Structure

```text
Data_scraping/
├── medium_resource_scraper/          # Medium publication scraper & PDF/Markdown converter
│   ├── cli/                          # CLI tool with Excel batch scraping
│   ├── medium_scraper/               # Core async scraping library
│   ├── docs/                         # Detailed library & CLI documentation
│   └── pyproject.toml                # Package configuration
│
└── web-and-github-scrapers/          # Web, Git, and Threat Intelligence scrapers
    ├── core_scrapers/
    │   ├── workbook_resource_scraper.py           # Multi-source Excel workbook crawler
    │   ├── github_languages_repos_scraper.py     # Concurrent GitHub repo downloader (25+ languages)
    │   └── scrape_git_top_1000.py                 # Top 1,000 GitHub repositories extractor
    │
    ├── threat_intelligence_and_compliance/
    │   ├── scrape_threat_intel.py                 # MITRE ATT&CK, NIST, Exploit-DB, OWASP & Code4rena
    │   └── scrape_sans_and_research.py            # SANS Whitepapers, PortSwigger Labs & CTF datasets
    │
    └── utilities_and_data/
        ├── clean_scraper_logs.py                  # Log & residue cleanup tool
        ├── extract_code_corpus.py                 # High-speed clean source code extractor
        └── data/                                  # Seed spreadsheets & repository metadata
```

---

## 🚀 Key Modules & Quick Usage

### 1. Medium Resource Scraper (Python Library & CLI)
Functions as both an **installable Python library (`import medium_scraper`)** and a **batch-processing CLI**. It supports async article discovery, request abstraction, and converting Medium articles into publication-ready PDFs or clean Markdown:

```python
# Use programmatically as a Python library:
from medium_scraper import MediumExplorer, MediumParser

explorer = MediumExplorer()
articles = await explorer.get_tag_articles("cybersecurity", limit=5)

parser = MediumParser()
parser.to_pdf(article_html, output_path="article.pdf")
```

```bash
# Or install and run via CLI:
cd medium_resource_scraper
pip install .

# Batch scrape Medium URLs from Excel into styled PDFs
python cli/medium_scraper_cli.py scrape -i articles.xlsx -o output_dir -w 10
```

### 2. Multi-Language GitHub Scraper
Downloads full source code tarballs in parallel across 25+ programming languages, filtered for AI/SLM training:
```bash
cd web-and-github-scrapers/core_scrapers

# Download Python or PowerShell repositories from Excel
python github_languages_repos_scraper.py --python --xlsx ../utilities_and_data/data/languages_repositories.xlsx
python github_languages_repos_scraper.py --powershell
```

### 3. Workbook Resource Scraper
Reads multi-sheet Excel workbooks (`Vendor`, `Product`, `URL`), converts web pages to clean Markdown, strips cookie banners and ads, and organizes output by category:
```bash
python workbook_resource_scraper.py --excel my_links.xlsx --dest ./scraped_docs
```

### 4. Threat Intelligence & Security Research
Extracts security frameworks and academic research papers (with headless Playwright fallback for JavaScript-rendered sites):
```bash
cd web-and-github-scrapers/threat_intelligence_and_compliance

# Scrape MITRE ATT&CK, NIST, Exploit-DB & Code4rena audits
python scrape_threat_intel.py

# Scrape SANS Institute whitepapers and PortSwigger research
python scrape_sans_and_research.py
```

### 5. Maintenance & Cleanup Utility
Safely removes temporary tracking files, logs, and cache folders across any directory:
```bash
cd web-and-github-scrapers/utilities_and_data

# Dry run (safe preview)
python clean_scraper_logs.py ../../

# Apply deletion
python clean_scraper_logs.py ../../ --apply
```

---

## 🛠️ Installation

Clone the repository and install dependencies:
```bash
git clone https://github.com/Muskanpandey0304/Data_scraping.git
cd Data_scraping
```

Install the required Python dependencies:
```bash
pip install -r medium_resource_scraper/requirements.txt
pip install beautifulsoup4 openpyxl requests aiohttp trafilatura playwright
playwright install chromium
```

---

## 👤 Author
- **Muskan Pandey** ([@Muskanpandey0304](https://github.com/Muskanpandey0304))
- Email: `muskanpandey0304@gmail.com`
