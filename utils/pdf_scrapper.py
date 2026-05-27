"""
Enhanced PDF Scraper for crawl4ai
Uses PyMuPDF (pymupdf) for robust content parsing with smart chunking.

Requirements:
    pip install pymupdf crawl4ai
"""

from __future__ import annotations

import time
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pymupdf
from crawl4ai.async_logger import AsyncLogger
from crawl4ai.processors.pdf import PDFContentScrapingStrategy
from crawl4ai.processors.pdf.processor import (
    PDFMetadata,
    PDFPage,
    PDFProcessorStrategy,
    PDFProcessResult,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
 
def _parse_pdf_date(raw: Optional[str]) -> Optional[datetime]:
    """Convert a PDF date string like ``D:20231005143000`` to a datetime."""
    if not raw:
        return None
    raw = raw.strip().lstrip("D:")
    try:
        return datetime.strptime(raw[:14], "%Y%m%d%H%M%S")
    except ValueError:
        return None
 
 
def _extract_metadata(doc: pymupdf.Document, file_path: Path) -> PDFMetadata:
    raw: Dict[str, Any] = doc.metadata or {}
    return PDFMetadata(
        title=raw.get("title") or None,
        author=raw.get("author") or None,
        producer=raw.get("producer") or None,
        created=_parse_pdf_date(raw.get("creationDate")),
        modified=_parse_pdf_date(raw.get("modDate")),
        pages=doc.page_count,
        encrypted=doc.is_encrypted,
        file_size=file_path.stat().st_size if file_path.exists() else None,
    )
 
 
def _process_page(page: pymupdf.Page, page_number: int) -> PDFPage:
    """Extract plain text and links from a single page."""
    raw_text = page.get_text("text")
 
    # Simple markdown: preserve the text as-is inside a fenced block
    markdown = raw_text.strip()
 
    # Minimal HTML wrapper
    escaped = raw_text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    html = f"<div class='pdf-page'><pre>{escaped}</pre></div>"
 
    links = [lnk["uri"] for lnk in page.get_links() if lnk.get("uri")]
 
    return PDFPage(
        page_number=page_number,
        raw_text=raw_text,
        markdown=markdown,
        html=html,
        images=[],
        links=links,
        layout=[],
    )
 
 
# ---------------------------------------------------------------------------
# PDFProcessorStrategy
# ---------------------------------------------------------------------------
 
class PyMuPDFProcessor(PDFProcessorStrategy):
    """
    PDF processor built on PyMuPDF.
 
    Parameters
    ----------
    batch_size:
        Number of parallel workers used by :meth:`process_batch`.
    """
 
    def __init__(self, batch_size: int = 4) -> None:
        self.batch_size = batch_size
        self.logger = logging.getLogger("PyMuPDFProcessor")
 
    # ------------------------------------------------------------------
    def process(self, pdf_path: Path) -> PDFProcessResult:
        """Process all pages sequentially."""
        start = time.perf_counter()
        pdf_path = Path(pdf_path)
 
        doc = pymupdf.open(str(pdf_path))
        try:
            metadata = _extract_metadata(doc, pdf_path)
            pages = [
                _process_page(doc.load_page(i), i + 1)
                for i in range(doc.page_count)
            ]
        finally:
            doc.close()
 
        return PDFProcessResult(
            metadata=metadata,
            pages=pages,
            processing_time=time.perf_counter() - start,
            version="2.0",
        )
 
    # ------------------------------------------------------------------
    def process_batch(self, pdf_path: Path) -> PDFProcessResult:
        """Like process() but processes PDF pages in parallel batches.
 
        Each worker opens its own pymupdf.Document handle — PyMuPDF is not
        thread-safe when a single Document is shared across threads.
        Pages are returned in their original document order.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import threading

        if not hasattr(threading.current_thread(), "_children"):
            threading.current_thread()._children = set()  # type: ignore[attr-defined]
 
        start = time.perf_counter()
        pdf_path = Path(pdf_path)
 
        # Metadata + page count from the main thread
        doc = pymupdf.open(str(pdf_path))
        try:
            metadata    = _extract_metadata(doc, pdf_path)
            total_pages = doc.page_count
        finally:
            doc.close()
 
        def _worker(page_index: int) -> PDFPage:
            thread_doc = pymupdf.open(str(pdf_path))
            try:
                return _process_page(thread_doc.load_page(page_index), page_index + 1)
            finally:
                thread_doc.close()
 
        ordered: List[Optional[PDFPage]] = [None] * total_pages
 
        with ThreadPoolExecutor(max_workers=self.batch_size) as ex:
            future_to_idx = {ex.submit(_worker, i): i for i in range(total_pages)}
            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    ordered[idx] = future.result()
                except Exception as exc:
                    self.logger.error("Failed to process page %d of '%s': %s", idx + 1, pdf_path.name, exc)
                    raise
 
        return PDFProcessResult(
            metadata=metadata,
            pages=ordered,          # type: ignore[arg-type]
            processing_time=time.perf_counter() - start,
            version="2.0-batch",
        )


class PDFContentSmarterScraper(PDFContentScrapingStrategy):
    def __init__(
        self,
        logger: AsyncLogger = None,
    ):
        self.logger = logger
        self.pdf_processor = PyMuPDFProcessor(
        )
        self._temp_files = []  # Track temp files for cleanup


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python enhanced_pdf_scraper.py <path_to_pdf>")
        sys.exit(1)

    pdf_file = Path(sys.argv[1])
    processor = PyMuPDFProcessor(max_block_chars=600, extract_images=False)
    proc_result = processor.process(pdf_file)

    print(f"=== PDF: {pdf_file.name} ===")
    print(f"Title   : {proc_result.metadata.title}")
    print(f"Author  : {proc_result.metadata.author}")
    print(f"Pages   : {proc_result.metadata.pages}")
    print(f"Time    : {proc_result.processing_time:.3f}s")
    print()

    for pg in proc_result.pages[:3]:  # show first 3 pages
        print(f"--- Page {pg.page_number} ---")
        chunks = [b for b in pg.layout if b["type"].startswith("chunk_")]
        texts = [b for b in pg.layout if b["type"] == "text"]
        heads = [b for b in pg.layout if b["type"] == "heading"]
        print(
            f"  headings={len(heads)}  plain={len(texts)}  chunk_blocks={len(chunks) // 3}"
        )
        print(f"  links={len(pg.links)}")
        if pg.layout:
            first = pg.layout[0]
            preview = first["text"][:120].replace("\n", " ")
            print(f"  first block [{first['type']}]: {preview!r}")
        print()
