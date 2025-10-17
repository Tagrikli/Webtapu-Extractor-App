"""
Table extraction functionality using Camelot.
"""
import logging
from pathlib import Path
from typing import List, Optional

import camelot
import pandas as pd

logger = logging.getLogger(__name__)


class TableExtractor:
    """Handles table extraction from PDF files using Camelot."""
    
    def extract_tables(self, pdf_path: Path) -> List[pd.DataFrame]:
        """
        Extract tables from PDF using Camelot.
        
        Args:
            pdf_path: Path to PDF file
            
        Returns:
            List of DataFrames containing extracted tables
        """
        try:
            tables = camelot.read_pdf(str(pdf_path), flavor="lattice", pages="all", parallel=True)
            dfs = [t.df for t in tables]
            logger.info(f"Extracted {len(dfs)} tables from {pdf_path.name}")
            return dfs
        except Exception as e:
            logger.error(f"Failed to extract tables from {pdf_path}: {str(e)}")
            return []
    
    def fix_continuation(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fix continuation lines in the DataFrame."""
        from text_processor import text_processor
        
        fixed_rows = []
        
        for i, row in df.iterrows():
            row_clean = text_processor.clean(row[0])
            
            if row_clean is None:
                prev_row = fixed_rows[-1]
                merged = [
                    str(p) + " " + str(r) if str(r).strip() != "" else str(p)
                    for p, r in zip(prev_row, row)
                ]
                fixed_rows[-1] = merged
            else:
                fixed_rows.append(list(row))
        
        clean_df = pd.DataFrame(fixed_rows, columns=df.columns)
        return clean_df


# Global instance for convenience
table_extractor = TableExtractor()