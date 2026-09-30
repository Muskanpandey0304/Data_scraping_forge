from __future__ import annotations
import uuid
import io
import re
import traceback
from typing import Optional

from bs4 import BeautifulSoup, Tag
from xhtml2pdf import pisa

from ..models import ArticleParseResult

def normalize_title(title: str) -> str:
	"""Normalize the title to a valid filename."""
	title = title.strip()
	title = re.sub(r"[^\w\s-]", "", title)
	title = re.sub(r"\s+", "-", title)
	return title or "article"


def html_to_pdf(html_content: str) -> bytes:
	"""Convert an HTML string into PDF bytes using xhtml2pdf."""
	buf = io.BytesIO()
	pisa_status = pisa.CreatePDF(io.StringIO(html_content), dest=buf)
	if pisa_status.err:
		raise RuntimeError(f"xhtml2pdf error status: {pisa_status.err}")
	return buf.getvalue()


DEFAULT_PDF_CSS = """
@page {
	size: letter;
	margin: 0.8in;
}
body {
	font-family: Helvetica, Arial, sans-serif;
	font-size: 11pt;
	line-height: 1.6;
	color: #24292e;
}
h1 {
	font-size: 22pt;
	line-height: 1.25;
	margin-top: 0;
	margin-bottom: 12px;
	color: #111111;
}
h2 {
	font-size: 16pt;
	margin-top: 22px;
	margin-bottom: 10px;
	color: #1b1f23;
	border-bottom: 1px solid #eaecef;
	padding-bottom: 4px;
}
h3 {
	font-size: 13pt;
	margin-top: 18px;
	margin-bottom: 8px;
	color: #24292e;
}
p {
	margin-top: 0;
	margin-bottom: 12px;
}
a {
	color: #0366d6;
	text-decoration: underline;
}
img {
	max-width: 100%;
	height: auto;
	margin: 12px auto;
	display: block;
}
figure {
	margin: 16px 0;
	text-align: center;
}
figcaption {
	font-size: 9pt;
	color: #6a737d;
	font-style: italic;
	margin-top: 6px;
}
pre {
	background-color: #f6f8fa;
	border: 1px solid #e1e4e8;
	border-radius: 6px;
	padding: 12px;
	font-family: Courier, monospace;
	font-size: 9pt;
	white-space: pre-wrap;
	word-wrap: break-word;
	margin: 14px 0;
}
code {
	font-family: Courier, monospace;
	background-color: #f6f8fa;
	font-size: 9.5pt;
	padding: 2px 4px;
}
blockquote {
	border-left: 4px solid #0366d6;
	padding-left: 12px;
	margin: 14px 0;
	color: #6a737d;
	font-style: italic;
}
ul, ol {
	margin-top: 0;
	margin-bottom: 12px;
	padding-left: 24px;
}
li {
	margin-bottom: 4px;
}
.article-meta {
	font-size: 9.5pt;
	color: #586069;
	margin-bottom: 18px;
	padding-bottom: 8px;
	border-bottom: 1px solid #e1e4e8;
}
"""


def _pick_best_image_url(srcset: str) -> str:
	"""Extract highest resolution URL from srcset or data-srcset."""
	candidates = [c.strip() for c in (srcset or "").split(",") if c.strip()]
	if not candidates:
		return ""
	last_candidate = candidates[-1]
	parts = last_candidate.split()
	return parts[0] if parts else ""


class MediumPdfParser:
	"""Parses a Medium article's HTML directly into a styled PDF document with preserved images."""

	def parse_html(self, html: str, source_url: str, custom_css: str = "") -> ArticleParseResult:
		"""Convert article HTML into PDF bytes and return ArticleParseResult.

		- html: Full HTML string of a Medium post page
		- source_url: URL used for reference annotations in the output
		"""
		try:
			# HackerOne JSON Report parsing
			if "vulnerability_information" in html:
				try:
					import json, html as html_lib
					data = json.loads(html)
					raw_title = data.get("title") or "HackerOne Report"
					report_id = data.get("id") or ""
					team = data.get("team", {}).get("name") if isinstance(data.get("team"), dict) else ""
					severity = data.get("severity_rating") or ""
					vuln_text = data.get("vulnerability_information") or ""
					
					vuln_html = html_lib.escape(vuln_text).replace("\n", "<br>")
					css = DEFAULT_PDF_CSS + "\n" + custom_css
					
					full_html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>{raw_title}</title>
<style>
{css}
</style>
</head>
<body>
<h1 class="article-title">{raw_title}</h1>
<div style="color: #666; font-size: 13px; margin-bottom: 20px;">
    HackerOne Report #{report_id} {f"| Target: {team}" if team else ""} {f"| Severity: {severity}" if severity else ""}
</div>
<hr>
<div class="article-body">
    {vuln_html}
</div>
</body>
</html>"""
					pdf_bytes = html_to_pdf(full_html)
					return ArticleParseResult.success(pdf_bytes, raw_title, full_html)
				except Exception:
					pass

			soup = BeautifulSoup(html, "html.parser")
			article = soup.find("article") or soup.find("main") or soup.find("body") or soup
			if not article:
				return ArticleParseResult.failure("Error: Empty HTML content.")

			# Title
			h1 = article.find("h1")
			raw_title = ""
			meta = soup.find("meta", attrs={"name": "title"})
			if meta and meta.get("content"):
				raw_title = meta.get("content").strip()
			if not raw_title:
				raw_title = h1.get_text(strip=True) if h1 else f"Untitled-Article-{uuid.uuid4()}"
			title = normalize_title(raw_title)

			# Remove non-content interactive widgets
			for img in article.select("img[data-testid='authorPhoto']"):
				parent_a = img.find_parent("a"); (parent_a or img).decompose()
			for img in article.select("img[data-testid='publicationPhoto']"):
				parent_a = img.find_parent("a"); (parent_a or img).decompose()
			for a in article.select("a[href*='/m/signin']"):
				a.decompose()
			for el in article.select("[data-testid='publicationName']"):
				parent_div = el.find_parent("div"); (parent_div or el).decompose()
			for el in article.select("[data-testid='storyReadTime']"):
				parent_span = el.find_parent("span"); (parent_span or el).decompose()
			for el in article.select("[data-testid='storyPublishDate']"):
				parent_span = el.find_parent("span"); (parent_span or el).decompose()
			for el in article.select("[data-testid='authorName']"):
				el.decompose()
			for el in article.select("[data-testid='headerClapButton']"):
				el.decompose()

			# Strip Medium lightbox zoom prompt text ("Press enter or click to view image in full size")
			for text_node in article.find_all(string=re.compile(r"click to view image|press enter to view", re.IGNORECASE)):
				if text_node.parent:
					text_node.parent.decompose()

			# Strip inline styles, external stylesheets, and script tags to prevent xhtml2pdf font parsing errors
			for s in article.find_all(["style", "link", "script"]):
				s.decompose()

			# Preserve & resolve all images inside article (<figure>, <picture>, <img>)
			for tag in article.find_all(["figure", "picture", "img"]):
				best_src = ""
				# 1. Try <source srcset>
				source_node = tag.find("source")
				if source_node and (source_node.get("srcset") or source_node.get("data-srcset")):
					best_src = _pick_best_image_url(source_node.get("srcset") or source_node.get("data-srcset") or "")
				# 2. Try tag attributes
				if not best_src and (tag.get("srcset") or tag.get("data-srcset")):
					best_src = _pick_best_image_url(tag.get("srcset") or tag.get("data-srcset") or "")
				if not best_src and tag.get("data-src"):
					best_src = tag.get("data-src")
				if not best_src and tag.get("src"):
					best_src = tag.get("src")
				
				if best_src:
					import urllib.parse
					best_src = urllib.parse.urljoin(source_url, best_src)

				# If tag is figure or picture, locate/create internal <img>
				if tag.name in ["figure", "picture"]:
					img_el = tag.find("img")
					if not img_el and best_src:
						img_el = soup.new_tag("img", src=best_src)
						tag.append(img_el)
					elif img_el and best_src:
						img_el["src"] = best_src
				elif tag.name == "img" and best_src:
					tag["src"] = best_src

			# Absolute link targets
			for a in article.find_all("a"):
				href = a.get("href", "")
				if href.startswith("/"):
					a["href"] = "https://medium.com" + href
				a["target"] = "_blank"

			# Comprehensive non-content cleanup:
			# 1. Remove structural non-content containers
			for nav in article.find_all(["header", "footer", "nav", "aside", "form"]):
				nav.decompose()

			# 2. Remove widgets: clap buttons, comment boxes, response sections, author cards, paywalls
			for selector in [
				"[data-testid*='clap']", "[data-testid*='response']", "[data-testid*='share']",
				"[data-testid*='follow']", "[data-testid*='authorCard']", "[data-testid*='authorBio']",
				"[data-testid*='paywall']", "[data-testid*='footer']", "[data-testid*='header']",
				"div[class*='responses']", "section[class*='responses']", "div[class*='clap']",
				"div[class*='share']", "div[class*='author']"
			]:
				for el in article.select(selector):
					el.decompose()

			# 3. Strip text elements containing junk: "Share", "--", "Source:", "Read more", "Follow me", "Written by", "Responses"
			for text_node in article.find_all(string=re.compile(r"^\s*(share|--|source:)\s*$", re.IGNORECASE)):
				if text_node.parent:
					text_node.parent.decompose()

			for pattern in [r"read more", r"follow me", r"more from medium", r"responses \(\d+\)", r"write a response"]:
				for rm in article.find_all(string=re.compile(pattern, re.IGNORECASE)):
					parent = rm.find_parent(["h1", "h2", "h3", "h4", "h5", "div", "section", "p"])
					if parent:
						if parent.name in ["h1", "h2", "h3", "h4", "h5"]:
							nxt = parent.find_next_sibling()
							while nxt and nxt.name in ["ul", "ol", "div", "p", "section"]:
								nxt_sibling = nxt.find_next_sibling()
								nxt.decompose()
								nxt = nxt_sibling
						parent.decompose()

			# 4. Strip standalone tag lists/chips before title
			for tag_sec in article.select("ul[class*='tag'], div[class*='tag'], nav[class*='tag']"):
				tag_sec.decompose()

			css = DEFAULT_PDF_CSS + "\n" + custom_css
			full_html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>{raw_title}</title>
<style>
{css}
</style>
</head>
<body>
{str(article)}
</body>
</html>"""

			pdf_bytes = html_to_pdf(full_html)
			return ArticleParseResult.pdf_success(
				pdf_bytes=pdf_bytes,
				title=title,
				html_content=full_html,
			)
		except Exception as exc:
			return ArticleParseResult.failure(f"{exc}\n{traceback.format_exc()}")


MediumParser = MediumPdfParser