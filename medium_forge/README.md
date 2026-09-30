# Medium Scraper

A high-scale, asynchronous Medium scraper and parser for Python. Quickly extract, paginate, and convert Medium articles into styled PDF documents or clean Markdown directly from the CLI or programmatically as a library. Supports automated batch processing from Excel (`.xlsx`) files.

---

## ✨ Features

- **HTML-to-PDF & Markdown Parsing**: Convert Medium articles into styled, publication-ready PDFs or clean Markdown.
- **Excel (`.xlsx`) Batch Processing**: Automatically read article URLs from spreadsheets with organized folder outputs.
- **Async Concurrency**: High-performance asynchronous processing with worker-based rate limiting.
- **Request Abstraction**: Configurable request backends (direct requests, proxies, or API scrapers).
- **Persistent Caching**: Built-in SQLite caching to prevent duplicate network calls.

---

## 🛠️ Installation

```bash
# Clone the repository
git clone <YOUR_REPOSITORY_URL>
cd medium-scraper

# Install dependencies
pip install -r requirements.txt
```

Alternatively, install as a package:
```bash
pip install .
```

---

## 🖥️ CLI Tool & Excel Batch Processing

Run the CLI using:
```bash
python cli/medium_scraper_cli.py --help
```

### 1. Batch Scrape from Excel (.xlsx)
Scrape Medium article URLs directly from an Excel spreadsheet into formatted PDFs:

```bash
# Process specific Excel file with 10 concurrent workers
python cli/medium_scraper_cli.py scrape -i my_articles.xlsx -o medium -w 10
```

#### CLI Options for `scrape`:
- `-i`, `--input`: Path to input file (`.xlsx` Excel spreadsheet, `.json`, or `.txt` file containing URLs).
- `-o`, `--output`: Root directory to save generated PDFs (default: `medium`).
- `-s`, `--sheet`: Target sheet name(s) to process in Excel (default: all sheets).
- `-w`, `--workers`: Concurrency / number of parallel scraping workers (default: `10`).

#### Generated Directory Hierarchy:
Outputs are automatically organized based on sheet names, message IDs, and sanitized titles:
```text
medium/
├── <Sheet_Name>/
│   └── <msg_id>/
│       └── <Sanitized_Title>.pdf
```

---

### 2. Paginate Articles by Tag
Discover article URLs under a specific Medium tag across dates:

```bash
python cli/medium_scraper_cli.py paginate python --year 2024 --month 6 --output urls.json
```

---

## 📚 Library Usage

You can also use the scraper programmatically inside your own Python projects:

```python
import asyncio
from medium_scraper import MediumExplorer, MediumParser

async def main():
    explorer = MediumExplorer()
    articles = await explorer.get_tag_articles("technology", limit=5)
    
    for article in articles:
        print(f"Title: {article.title}")
        print(f"URL: {article.url}")

asyncio.run(main())
```

### PDF & Markdown Export
```python
from medium_scraper.parser import MediumParser

parser = MediumParser()
# Convert HTML to clean Markdown
markdown = parser.to_markdown(article_html)

# Or export to formatted PDF
parser.to_pdf(article_html, output_path="output.pdf")
```

---

## 📖 Documentation

For detailed information about internal modules, see:
- [Library Documentation](docs/Library.md)
- [CLI Documentation](docs/CLI.md)
