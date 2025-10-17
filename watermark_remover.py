"""
Watermark removal functionality for PDF files.
"""
import re
from pathlib import Path
from typing import Optional

import fitz


class WatermarkRemover:
    """Handles watermark removal from PDF files."""
    
    def __init__(self, target_size: float = 108.9, tolerance: float = 0.05):
        """
        Initialize watermark remover.
        
        Args:
            target_size: Target font size for watermark detection
            tolerance: Tolerance for font size matching
        """
        self.target_size = target_size
        self.tolerance = tolerance
        
        # Regular expressions for watermark removal
        self.re_tf = re.compile(rb"/[^\s]+?\s+([+-]?\d+(?:\.\d+)?)\s+Tf")
        self.re_tj = re.compile(rb"\((?:\\.|[^\)])*\)\s*Tj")
        self.re_TJ = re.compile(rb"\[.*?\]\s*TJ", re.S)
    
    def scrub_stream(self, buf: bytes) -> bytes:
        """Remove watermark text from PDF stream."""
        out = bytearray()
        pos = 0
        current_size = None
        
        for m in self.re_tf.finditer(buf):
            out += buf[pos : m.start()]
            out += buf[m.start() : m.end()]
            
            try:
                current_size = float(m.group(1))
            except Exception:
                current_size = None
            
            pos = m.end()
            # region until next Tf (or end)
            nxt = self.re_tf.search(buf, pos)
            chunk_end = nxt.start() if nxt else len(buf)
            chunk = buf[pos:chunk_end]
            
            if current_size is not None and abs(current_size - self.target_size) <= self.tolerance:
                chunk = self.re_tj.sub(b"() Tj", chunk)
                chunk = self.re_TJ.sub(b"[] TJ", chunk)
            
            out += chunk
            pos = chunk_end
        
        if pos < len(buf):
            tail = buf[pos:]
            if current_size is not None and abs(current_size - self.target_size) <= self.tolerance:
                tail = self.re_tj.sub(b"() Tj", tail)
                tail = self.re_TJ.sub(b"[] TJ", tail)
            out += tail
        
        return bytes(out)
    
    def remove_watermarks(self, pdf_path: Path, output_path: Path) -> bool:
        """
        Remove watermarks from PDF.
        
        Args:
            pdf_path: Path to input PDF file
            output_path: Path to save cleaned PDF
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with fitz.open(pdf_path) as doc:
                for page in doc:
                    cx = page.get_contents()
                    cxrefs = [cx] if isinstance(cx, int) else (cx or [])
                    for xref in cxrefs:
                        buf = doc.xref_stream(xref)
                        if not buf:
                            continue
                        new_buf = self.scrub_stream(buf)
                        if new_buf != buf:
                            doc.update_stream(xref, new_buf)
                
                doc.save(output_path, deflate=True)
            
            return True
            
        except Exception as e:
            print(f"Failed to remove watermarks from {pdf_path}: {str(e)}")
            return False


# Global instance for convenience
watermark_remover = WatermarkRemover()