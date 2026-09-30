from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class HttpResponse:
    url: str
    status_code: int
    headers: Dict[str, str]
    text: str


@dataclass
class ArticleParseResult:
    error: bool
    title: Optional[str] = None
    message: Optional[str] = None
    pdf_bytes: Optional[bytes] = None
    html_content: Optional[str] = None

    @classmethod
    def pdf_success(cls, pdf_bytes: bytes, title: str, html_content: Optional[str] = None) -> "ArticleParseResult":
        return cls(error=False, title=title, pdf_bytes=pdf_bytes, html_content=html_content)

    @classmethod
    def success(cls, pdf_bytes: bytes, title: str, html_content: Optional[str] = None) -> "ArticleParseResult":
        return cls(error=False, title=title, pdf_bytes=pdf_bytes, html_content=html_content)

    @classmethod
    def failure(cls, message: str) -> "ArticleParseResult":
        return cls(error=True, message=message) 