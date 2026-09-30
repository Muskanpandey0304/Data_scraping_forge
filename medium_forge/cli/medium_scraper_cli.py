from __future__ import annotations

import asyncio
import json
import re
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

import typer
from rich import print
from rich.progress import Progress

from medium_scraper import (
    CachedRequestSender,
    DecodoScraperRequestSender,
    MediumExplorer,
    scrape_pdf_all,
    RequestsRequestSender,
)

app = typer.Typer(
    name="Medium Scraper to PDF",
    add_completion=False,
    help=(
        "Async Medium scraper that converts articles directly from HTML to styled PDF documents, "
        "supporting Excel (.xlsx), JSON, and text inputs with custom directory hierarchies."
    ),
)


def _load_proxies(proxies_path: Optional[str]):
    if not proxies_path:
        return None
    p = Path(proxies_path)
    if not p.exists():
        raise typer.BadParameter(f"Proxies file not found: {proxies_path}")
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data
    except Exception as exc:
        raise typer.BadParameter(f"Invalid proxies file (expect JSON dict or list[dict]): {exc}")


def _slugify(text: str) -> str:
    text = text.strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"\-+", "-", text)
    return text or "article"


def _sender_from_opts(
    sender: str,
    decodo_api_key: Optional[str],
    advanced: bool,
    proxies,
    cache_db: Optional[str],
    timeout: float,
):
    sender = (sender or "auto").lower()
    if sender not in {"auto", "requests", "decodo"}:
        raise typer.BadParameter("--sender must be one of: auto, requests, decodo")

    inner = None
    if sender == "decodo" or (sender == "auto" and decodo_api_key):
        if not decodo_api_key:
            raise typer.BadParameter("--decodo-api-key is required when --sender=decodo")
        inner = DecodoScraperRequestSender(api_key=decodo_api_key, advanced=advanced, timeout=timeout)
    else:
        inner = RequestsRequestSender(proxies=proxies, timeout=timeout)

    return CachedRequestSender(inner, db_path=cache_db or "cache.db")


def _parse_date(s: str) -> date:
    if s.lower() in {"today", "now"}:
        return date.today()
    return datetime.strptime(s, "%Y-%m-%d").date()


def _month_iter(start: date, end: date) -> List[Tuple[int, int]]:
    months = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        months.append((y, m))
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1
    return months


def _load_input_items(input_path: str, sheets_filter: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """Parse input file (.xlsx, .json, .txt) and extract item dictionaries."""
    p = Path(input_path)
    if not p.exists():
        raise typer.BadParameter(f"Input file not found: {input_path}")

    items: List[Dict[str, Any]] = []

    if p.suffix.lower() in {".xlsx", ".xls"}:
        import openpyxl
        wb = openpyxl.load_workbook(p, data_only=True)
        sheet_names = wb.sheetnames
        norm_filter = {sf.strip().lower() for sf in sheets_filter} if sheets_filter else None
        target_sheets = [s for s in sheet_names if not norm_filter or s.strip().lower() in norm_filter]

        for sname in target_sheets:
            sheet = wb[sname]
            rows = list(sheet.iter_rows(values_only=True))
            if not rows:
                continue

            # Header inspection
            header_row = [str(cell).strip() if cell is not None else "" for cell in rows[0]]
            header_lower = [h.lower() for h in header_row]

            url_col_idx = -1
            msg_id_col_idx = -1
            filename_col_idx = -1

            for idx, h in enumerate(header_lower):
                if any(kw in h for kw in ["url", "link", "medium", "article", "href", "site", "web"]):
                    if url_col_idx == -1:
                        url_col_idx = idx
                if any(kw in h for kw in ["msg_id", "message_id", "msg id", "message id", "id", "msg"]):
                    if msg_id_col_idx == -1:
                        msg_id_col_idx = idx
                if any(kw in h for kw in ["sanitized_file_name", "sanitized file name", "file_name", "filename", "file name", "name", "title"]):
                    if filename_col_idx == -1:
                        filename_col_idx = idx

            data_rows = rows[1:] if (url_col_idx != -1 or msg_id_col_idx != -1 or filename_col_idx != -1) else rows
            for row_idx, row in enumerate(data_rows, start=1):
                url_val = ""
                msg_id_val = f"msg_{row_idx}"
                filename_val = ""

                if url_col_idx != -1 and url_col_idx < len(row):
                    url_val = str(row[url_col_idx] or "").strip()
                if msg_id_col_idx != -1 and msg_id_col_idx < len(row):
                    val = str(row[msg_id_col_idx] or "").strip()
                    if val and val.lower() != "none":
                        msg_id_val = val
                if filename_col_idx != -1 and filename_col_idx < len(row):
                    val = str(row[filename_col_idx] or "").strip()
                    if val and val.lower() != "none":
                        filename_val = val

                # Fallback: scan all cells in row for Medium / HTTP URL
                if not url_val:
                    for cell in row:
                        cell_str = str(cell or "").strip()
                        if cell_str.startswith("http://") or cell_str.startswith("https://"):
                            url_val = cell_str
                            break
                        elif cell_str.startswith("medium.com/") or cell_str.startswith("www.medium.com/"):
                            url_val = "https://" + cell_str
                            break

                if url_val and url_val.lower() != "none":
                    items.append({
                        "url": url_val,
                        "sheet_name": sname,
                        "msg_id": msg_id_val,
                        "sanitized_file_name": filename_val,
                    })

    elif p.suffix.lower() == ".json":
        data = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(data, list):
            for idx, entry in enumerate(data, start=1):
                if isinstance(entry, dict) and entry.get("url"):
                    items.append({
                        "url": str(entry["url"]).strip(),
                        "sheet_name": entry.get("sheet_name", "default"),
                        "msg_id": str(entry.get("msg_id") or entry.get("message_id") or f"msg_{idx}"),
                        "sanitized_file_name": str(entry.get("sanitized_file_name") or entry.get("file_name") or ""),
                    })
                elif isinstance(entry, str) and entry.strip():
                    items.append({
                        "url": entry.strip(),
                        "sheet_name": "default",
                        "msg_id": f"msg_{idx}",
                        "sanitized_file_name": "",
                    })
    else:
        # Plain text file (URL per line)
        lines = [ln.strip() for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()]
        for idx, line in enumerate(lines, start=1):
            items.append({
                "url": line,
                "sheet_name": "default",
                "msg_id": f"msg_{idx}",
                "sanitized_file_name": "",
            })

    return items


@app.command()
def paginate(
    tag: str = typer.Argument(..., help="Medium tag slug (e.g., tryhackme)"),
    year: Optional[int] = typer.Option(None, help="Target year (e.g., 2022)"),
    month: Optional[int] = typer.Option(None, help="Target month (1-12)"),
    page_size: int = typer.Option(50, help="Number of posts per request"),
    sender: str = typer.Option("auto", help="Request backend: auto | requests | decodo"),
    decodo_api_key: Optional[str] = typer.Option(None, envvar="DECODO_API_KEY", help="Decodo API key (required if --sender=decodo)"),
    advanced: bool = typer.Option(False, help="Decodo Advanced plan (adds headless rendering)"),
    proxies_path: Optional[str] = typer.Option(None, help="Path to JSON proxies (dict or list[dict]); used only for requests sender"),
    cache_db: Optional[str] = typer.Option("cache.db", help="SQLite cache DB path (persistent between runs)"),
    timeout: float = typer.Option(30.0, help="Request timeout in seconds"),
    output: Optional[str] = typer.Option(None, help="Output JSON file; prints to stdout if omitted"),
    from_date: Optional[str] = typer.Option(None, help="Start date YYYY-MM-DD or 'today'"),
    to_date: Optional[str] = typer.Option(None, help="End date YYYY-MM-DD or 'today'"),
):
    """List articles for a tag as JSON (single month or date range)."""
    if (year is None or month is None) and (not from_date or not to_date):
        raise typer.BadParameter("Provide --year and --month, or --from-date and --to-date")

    proxies = _load_proxies(proxies_path)
    client = _sender_from_opts(sender, decodo_api_key, advanced, proxies, cache_db, timeout)

    async def _run():
        explorer = MediumExplorer(client)
        results = []
        if from_date and to_date:
            start = _parse_date(from_date)
            end = _parse_date(to_date)
            ym_list = _month_iter(start, end)
            with Progress() as progress:
                task = progress.add_task("Fetching feed (range)...", total=len(ym_list))
                for y, m in ym_list:
                    items = await explorer.get_articles_by_category(tag, year=y, month=m, page_size=page_size)
                    results.extend(items)
                    progress.advance(task)
        else:
            with Progress() as progress:
                task = progress.add_task("Fetching feed...", total=None)
                items = await explorer.get_articles_by_category(tag, year=year or 0, month=month or 0, page_size=page_size)
                progress.update(task, completed=1)
                results.extend(items)

        data = [item.__dict__ for item in results]
        if output:
            Path(output).write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        else:
            print(json.dumps(data, ensure_ascii=False, indent=2))

    asyncio.run(_run())


@app.command()
def scrape(
    input_file: Optional[str] = typer.Option(None, "-i", "--input", "--urls-file", help="Path to input Excel (.xlsx), JSON, or text file containing Medium URLs"),
    url: List[str] = typer.Argument(None, help="One or more Medium post URLs to scrape"),
    output_dir: str = typer.Option("medium", "-o", "--output", "--out-dir", help="Root directory to save PDF folder hierarchy"),
    sheet: Optional[List[str]] = typer.Option(None, "-s", "--sheet", help="Specific sheet name(s) to process from Excel input"),
    workers: int = typer.Option(10, "-w", "--workers", "--concurrency", help="Number of concurrent scraping workers"),
    sender: str = typer.Option("auto", help="Request backend: auto | requests | decodo"),
    decodo_api_key: Optional[str] = typer.Option(None, envvar="DECODO_API_KEY", help="Decodo API key (required if --sender=decodo)"),
    advanced: bool = typer.Option(False, help="Decodo Advanced plan (adds headless rendering)"),
    proxies_path: Optional[str] = typer.Option(None, help="Path to proxies input: JSON dict/list OR newline-delimited proxy URLs"),
    cache_db: Optional[str] = typer.Option("cache.db", help="SQLite cache DB path"),
    timeout: float = typer.Option(30.0, help="Request timeout in seconds"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview items and output folder paths without sending network requests or creating files"),
):
    """Scrape Medium URLs and save styled PDFs inside medium/<Sheet_Name>/<msg_id>/<Sanitized_File_Name>.pdf."""
    items: List[Dict[str, Any]] = []
    if url:
        for u in url:
            items.append({"url": u, "sheet_name": "default", "msg_id": "msg_1", "sanitized_file_name": ""})
    if input_file:
        items.extend(_load_input_items(input_file, sheets_filter=sheet))

    if not items:
        raise typer.BadParameter("No URLs provided. Provide positional URL arguments or --input (-i).")

    if dry_run:
        root_path = Path(output_dir)
        print(f"[bold cyan][DRY-RUN PREVIEW][/bold cyan] Found [bold green]{len(items)}[/bold green] items to scrape from input.")
        for idx, item in enumerate(items, start=1):
            sname = _slugify(item.get("sheet_name") or "default")
            mid = _slugify(str(item.get("msg_id") or "msg_1"))
            s_fname = item.get("sanitized_file_name")
            if s_fname and str(s_fname).strip():
                fname = f"{_slugify(str(s_fname))}.pdf"
            else:
                fname = "<article-title-slug>.pdf"
            target_file = root_path / sname / mid / fname
            print(f"  [yellow]{idx:2d}.[/yellow] Sheet: [bold]{item.get('sheet_name')}[/bold] | Msg ID: [bold]{item.get('msg_id')}[/bold] | URL: {item.get('url')}")
            print(f"      -> Destination PDF: [green]{target_file}[/green]")
        print(f"\n[bold green]Dry run completed successfully. {len(items)} items validated. No network requests sent and no files created.[/bold green]")
        return

    proxies = None
    if proxies_path:
        ptext = Path(proxies_path).read_text(encoding="utf-8")
        try:
            proxies = json.loads(ptext)
        except Exception:
            lines = [ln.strip() for ln in ptext.splitlines() if ln.strip()]
            proxies = lines

    client = _sender_from_opts(sender, decodo_api_key, advanced, proxies, cache_db, timeout)
    root_path = Path(output_dir)
    root_path.mkdir(parents=True, exist_ok=True)

    # Automatic Resume: Filter out items whose target PDF already exists on disk
    pending_items = []
    skipped_count = 0
    for item in items:
        sname = _slugify(item.get("sheet_name") or "medium_articles")
        mid = _slugify(str(item.get("msg_id") or "msg_1"))
        s_fname = item.get("sanitized_file_name")
        if s_fname and str(s_fname).strip():
            filename_stem = _slugify(str(s_fname))
            pdf_path = root_path / sname / mid / f"{filename_stem}.pdf"
            if pdf_path.exists() and pdf_path.stat().st_size > 1000:
                skipped_count += 1
                continue
        pending_items.append(item)

    if skipped_count > 0:
        print(f"[yellow]Resuming run: Automatically skipped {skipped_count} PDFs already generated on disk.[/yellow]")

    items = pending_items

    async def _run():
        manifest = []

        def _save_pdf_immediately(res: Dict[str, Any]):
            item_manifest = {k: v for k, v in res.items() if k != "pdf_bytes"}
            pdf_b = res.get("pdf_bytes")
            if res.get("ok") and pdf_b and len(pdf_b) > 1000:
                raw_sheet = res.get("sheet_name") or "medium_articles"
                sname = _slugify(raw_sheet)
                mid = _slugify(str(res.get("msg_id") or "msg_1"))
                
                target_dir = root_path / sname / mid
                target_dir.mkdir(parents=True, exist_ok=True)

                s_fname = res.get("sanitized_file_name")
                if s_fname and str(s_fname).strip():
                    filename_stem = _slugify(str(s_fname))
                else:
                    url_val = res["url"]
                    url_slug = _slugify(url_val.rsplit("/", 1)[-1])
                    filename_stem = url_slug if (url_slug and url_slug != "article") else _slugify(res.get("title") or "article")

                filename = f"{filename_stem}.pdf"
                pdf_path = target_dir / filename
                pdf_path.write_bytes(res["pdf_bytes"])
                item_manifest["pdf_file"] = str(pdf_path)

            manifest.append(item_manifest)

        with Progress() as progress:
            t = progress.add_task("Scraping articles to PDF...", total=len(items))

            def _cb(c, tot, pct, stats):
                progress.update(t, completed=c)

            results = await scrape_pdf_all(
                items,
                client,
                concurrency=workers,
                progress_callback=_cb,
                on_item_done=_save_pdf_immediately,
            )

        (root_path / "results.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[green]Saved {len(results)} PDFs into structured hierarchy under {root_path}[/green]")

    asyncio.run(_run())


@app.command("scrape-tag")
def scrape_tag(
    tag: str = typer.Argument(..., help="Medium tag slug (e.g., tryhackme)"),
    year: Optional[int] = typer.Option(None, help="Target year (e.g., 2022)"),
    month: Optional[int] = typer.Option(None, help="Target month (1-12)"),
    page_size: int = typer.Option(50, help="Number of posts per request"),
    output_dir: str = typer.Option("medium", "-o", "--output", "--out-dir", help="Root output directory"),
    workers: int = typer.Option(10, "-w", "--workers", "--concurrency", help="Number of concurrent scraping workers"),
    sender: str = typer.Option("auto", help="Request backend: auto | requests | decodo"),
    decodo_api_key: Optional[str] = typer.Option(None, envvar="DECODO_API_KEY", help="Decodo API key (required if --sender=decodo)"),
    advanced: bool = typer.Option(False, help="Decodo Advanced plan (adds headless rendering)"),
    proxies_path: Optional[str] = typer.Option(None, help="Path to JSON proxies"),
    cache_db: Optional[str] = typer.Option("cache.db", help="SQLite cache DB path"),
    timeout: float = typer.Option(30.0, help="Request timeout in seconds"),
    from_date: Optional[str] = typer.Option(None, help="Start date YYYY-MM-DD or 'today'"),
    to_date: Optional[str] = typer.Option(None, help="End date YYYY-MM-DD or 'today'"),
):
    """Paginate a tag and scrape all posts to PDFs inside medium/<tag>/<msg_id>/<article-slug>.pdf."""
    if (year is None or month is None) and (not from_date or not to_date):
        raise typer.BadParameter("Provide --year and --month, or --from-date and --to-date")

    proxies = _load_proxies(proxies_path)
    client = _sender_from_opts(sender, decodo_api_key, advanced, proxies, cache_db, timeout)
    root_path = Path(output_dir)
    root_path.mkdir(parents=True, exist_ok=True)

    async def _run():
        explorer = MediumExplorer(client)
        urls: List[str] = []
        if from_date and to_date:
            start = _parse_date(from_date)
            end = _parse_date(to_date)
            ym_list = _month_iter(start, end)
            with Progress() as progress:
                t1 = progress.add_task("Fetching feed (range)...", total=len(ym_list))
                for y, m in ym_list:
                    items = await explorer.get_articles_by_category(tag, year=y, month=m, page_size=page_size)
                    urls.extend([a.url for a in items if a.url])
                    progress.advance(t1)
        else:
            with Progress() as progress:
                t1 = progress.add_task("Fetching feed...", total=None)
                items = await explorer.get_articles_by_category(tag, year=year or 0, month=month or 0, page_size=page_size)
                progress.update(t1, completed=1)
                urls.extend([a.url for a in items if a.url])

        if not urls:
            print("[yellow]No URLs found[/yellow]")
            return

        input_items = [{"url": u, "sheet_name": tag, "msg_id": f"msg_{i+1}", "sanitized_file_name": ""} for i, u in enumerate(urls)]
        with Progress() as progress:
            t2 = progress.add_task("Scraping articles to PDF...", total=len(input_items))
            results = await scrape_pdf_all(input_items, client, concurrency=workers)
            progress.update(t2, completed=len(input_items))

        manifest = []
        for res in results:
            item_manifest = {k: v for k, v in res.items() if k != "pdf_bytes"}
            if res.get("ok") and res.get("pdf_bytes"):
                sname = _slugify(res.get("sheet_name") or tag)
                mid = _slugify(str(res.get("msg_id") or "msg_1"))
                target_dir = root_path / sname / mid
                target_dir.mkdir(parents=True, exist_ok=True)

                filename_stem = _slugify(res.get("title") or "article")
                filename = f"{filename_stem}.pdf"
                pdf_path = target_dir / filename
                pdf_path.write_bytes(res["pdf_bytes"])
                item_manifest["pdf_file"] = str(pdf_path)

            manifest.append(item_manifest)

        (root_path / "results.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[green]Saved {len(results)} PDFs to {root_path}[/green]")

    asyncio.run(_run())


if __name__ == "__main__":
    app()