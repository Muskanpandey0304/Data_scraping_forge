from __future__ import annotations

import asyncio
from typing import Any, Callable, Dict, List, Mapping, Optional, Union

from .request_sender.base import RequestSender
from .parser.medium_parser import MediumPdfParser

ProgressCallback = Callable[[int, int, float, Dict[str, Any]], None]

async def fetch_all(
    urls: List[str],
    sender: RequestSender,
    *,
    concurrency: int = 10,
    timeout: Optional[float] = None,
    headers: Optional[Mapping[str, str]] = None,
    request_kwargs: Optional[Dict[str, Any]] = None,
    progress_callback: Optional[ProgressCallback] = None,
) -> List[Dict[str, Any]]:
    """Fetch a list of URLs concurrently using the provided sender."""
    sem = asyncio.Semaphore(concurrency)
    req_kwargs = request_kwargs or {}
    
    completed = 0
    total = len(urls)
    stats = {"success": 0, "failed": 0, "current_url": ""}
    
    if progress_callback:
        progress_callback(0, total, 0.0, stats.copy())

    async def _one(url: str) -> Dict[str, Any]:
        nonlocal completed, stats
        async with sem:
            if progress_callback:
                stats["current_url"] = url
            try:
                resp = await sender.fetch(url, timeout=timeout, headers=headers, **req_kwargs)
                result = {
                    "url": resp.url,
                    "ok": True,
                    "status_code": resp.status_code,
                    "headers": resp.headers,
                    "text": resp.text,
                }
                stats["success"] += 1
            except Exception as exc:
                result = {
                    "url": url,
                    "ok": False,
                    "error": str(exc),
                }
                stats["failed"] += 1
            
            completed += 1
            if progress_callback:
                percentage = (completed / total) * 100
                progress_callback(completed, total, percentage, stats.copy())
            
            return result

    tasks = [asyncio.create_task(_one(u)) for u in urls]
    return await asyncio.gather(*tasks)


async def scrape_pdf_all(
    items: List[Union[str, Dict[str, Any]]],
    sender: RequestSender,
    *,
    concurrency: int = 10,
    timeout: Optional[float] = None,
    headers: Optional[Mapping[str, str]] = None,
    request_kwargs: Optional[Dict[str, Any]] = None,
    progress_callback: Optional[ProgressCallback] = None,
    on_item_done: Optional[Callable[[Dict[str, Any]], None]] = None,
    custom_css: str = "",
) -> List[Dict[str, Any]]:
    """Fetch and parse Medium articles directly to styled PDF concurrently."""
    sem = asyncio.Semaphore(concurrency)
    req_kwargs = request_kwargs or {}
    
    completed = 0
    total = len(items)
    stats = {"success": 0, "failed": 0, "parse_failed": 0, "current_url": ""}
    
    if progress_callback:
        progress_callback(0, total, 0.0, stats.copy())

    async def _one(item: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
        nonlocal completed, stats
        if isinstance(item, str):
            url = item
            meta = {}
        else:
            url = item.get("url", "")
            meta = dict(item)

        async with sem:
            if progress_callback:
                stats["current_url"] = url
            try:
                resp = await sender.fetch(url, timeout=timeout, headers=headers, **req_kwargs)
                parser = MediumPdfParser()
                parsed = parser.parse_html(resp.text, source_url=resp.url, custom_css=custom_css)
                if parsed.error:
                    result = {
                        **meta,
                        "url": resp.url or url,
                        "ok": False,
                        "error": parsed.message or "parse failed"
                    }
                    stats["parse_failed"] += 1
                else:
                    result = {
                        **meta,
                        "url": resp.url or url,
                        "ok": True,
                        "title": parsed.title,
                        "pdf_bytes": parsed.pdf_bytes,
                    }
                    stats["success"] += 1
            except Exception as exc:
                result = {**meta, "url": url, "ok": False, "error": str(exc)}
                stats["failed"] += 1
            
            completed += 1
            if on_item_done:
                try:
                    on_item_done(result)
                except Exception:
                    pass

            if progress_callback:
                percentage = (completed / total) * 100
                progress_callback(completed, total, percentage, stats.copy())
            
            return result

    tasks = [asyncio.create_task(_one(it)) for it in items]
    return await asyncio.gather(*tasks)


scrape_all = scrape_pdf_all