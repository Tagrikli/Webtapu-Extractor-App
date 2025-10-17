"""
Simplified PDF Processor for WebtapuApp
Uses refactored modules for cleaner code organization.
"""
import logging
import traceback
from pathlib import Path
from typing import Dict, List, Optional, Any, Callable

import pandas as pd
from tqdm.auto import tqdm

from text_processor import text_processor
from watermark_remover import watermark_remover
from table_extractor import table_extractor
from data_extractor import data_extractor

logger = logging.getLogger(__name__)


class PDFProcessor:
    """
    Main PDF processing class that coordinates between different modules.
    
    This class provides a unified interface for the PDF processing pipeline.
    """
    
    def __init__(self, temp_dir: Optional[Path] = None):
        """
        Initialize the PDF processor.
        
        Args:
            temp_dir: Temporary directory for storing intermediate files
        """
        self.temp_dir = temp_dir or Path("/tmp/webtapu")
        self.temp_dir.mkdir(parents=True, exist_ok=True)
    
    def process_single_pdf(self, pdf_path: Path, clean_watermarks: bool = True) -> Optional[pd.DataFrame]:
        """
        Process a single PDF file through the entire pipeline.
        
        Args:
            pdf_path: Path to PDF file
            clean_watermarks: Whether to remove watermarks first
            
        Returns:
            DataFrame with extracted data or None if processing failed
        """
        try:
            # Remove watermarks if requested
            if clean_watermarks:
                cleaned_path = self.temp_dir / "cleaned" / pdf_path.name
                if not watermark_remover.remove_watermarks(pdf_path, cleaned_path):
                    logger.warning(f"Watermark removal failed, using original: {pdf_path}")
                    cleaned_path = pdf_path
            else:
                cleaned_path = pdf_path
            
            # Extract tables
            file_dfs = table_extractor.extract_tables(cleaned_path)
            if not file_dfs:
                logger.error(f"No tables extracted from {pdf_path}")
                return None
            
            # Extract general information
            general_info = data_extractor.extract_general_info(file_dfs)
            if not general_info:
                logger.error(f"Failed to extract general info from {pdf_path}")
                return None
            
            # Extract property restriction declarations
            df_masb = data_extractor.extract_mulkiyete_ait_serh_beyan(file_dfs)
            if df_masb.empty:
                logger.warning(f"No property restriction data found in {pdf_path}")
                return None
            
            # Clean and process data
            df_masb = df_masb.map(text_processor.clean)
            df_masb = df_masb.map(text_processor.upper)
            
            # Extract additional fields
            df_aciklama = data_extractor.extract_aciklama(df_masb)
            df_date_yevmiye = data_extractor.extract_date_yevmiye(df_masb)
            df_icra_dairesi = data_extractor.extract_icra_dairesi(df_aciklama)
            
            # Combine all data
            df_final = pd.concat([df_masb, df_aciklama, df_date_yevmiye, df_icra_dairesi], axis=1)
            df_final = df_final.assign(**general_info)
            df_final = df_final.assign(
                source_file=pdf_path.name,
                # source_dir=str(pdf_path.parent),
            )
            
            logger.info(f"Successfully processed {pdf_path}")
            return df_final
            
        except Exception as e:
            logger.error(f"Failed to process {pdf_path}: {str(e)}")
            logger.debug(traceback.format_exc())
            return None
    
    def process_multiple_pdfs(
        self,
        pdf_paths: List[Path],
        clean_watermarks: bool = True,
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> pd.DataFrame:
        """
        Process multiple PDF files and combine results.
        
        Args:
            pdf_paths: List of PDF file paths
            clean_watermarks: Whether to remove watermarks first
            
        Returns:
            Combined DataFrame with data from all successfully processed PDFs
        """
        frames: List[pd.DataFrame] = []
        failures = 0
        total = len(pdf_paths)

        if progress_callback:
            progress_callback({
                "event": "start",
                "total": total,
                "message": f"Processing {total} PDF file(s)"
            })

        for index, pdf_path in enumerate(tqdm(pdf_paths, desc="Processing PDFs", unit="file"), start=1):
            df = self.process_single_pdf(pdf_path, clean_watermarks)
            if df is not None and not df.empty:
                frames.append(df)
                status = "processed"
            else:
                failures += 1
                status = "failed"

            if progress_callback:
                percent = int((index / total) * 100) if total else 100
                progress_callback({
                    "event": "progress",
                    "current": index,
                    "total": total,
                    "percent": percent,
                    "status": status,
                    "file": pdf_path.name,
                    "message": f"Processing PDFs: {index}/{total} file(s) [{percent}%] ({status})"
                })

        if not frames:
            logger.warning("No dataframes produced from any PDF")
            if progress_callback:
                progress_callback({
                    "event": "error",
                    "message": "No dataframes produced from any PDF"
                })
            return pd.DataFrame()

        all_final = pd.concat(frames, ignore_index=True)
        
        # Reorder columns
        cols = list(all_final.columns)
        front = [c for c in ("source_file",) if c in cols]
        rest = [c for c in cols if c not in front]
        all_final = all_final[front + rest]
        
        logger.info(f"Processed {len(frames)} PDFs successfully, {failures} failures")
        if progress_callback:
            progress_callback({
                "event": "complete",
                "processed": len(frames),
                "failures": failures,
                "message": f"Processed {len(frames)} PDF file(s) with {failures} failure(s)"
            })
        return all_final
    
    def generate_excel(self, df: pd.DataFrame, output_path: Path) -> bool:
        """
        Generate Excel file from processed data.
        
        Args:
            df: Processed DataFrame
            output_path: Path to save Excel file
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if df.empty:
                logger.warning("No data to export to Excel")
                return False
            
            # Select and rename columns for Excel output
            df_clean = df[['source_file', 's/b/i', 'haciz_type', 'aciklama',
                          'aciklama_ext', 'tarih', 'aciklama_fileno', 'yevmiye', 'icra_dairesi',
                          'il', 'ilce', 'mahalle', 'bagimsiz_bolum_nitelik', 'ada', 'parsel',
                          'blok', 'kat', 'giris', 'bbno']]
            
            df_clean.columns = [
                "Kaynak Dosyasi",
                "S/B/I",
                "Haciz Turu",
                "Aciklama",
                "Aciklama Extracted",
                "Tarih",
                "Dosya Numarasi",
                "Yevmiye",
                "Icra Dairesi",
                "Il",
                "Ilce",
                "Mahalle",
                "Bagimsiz Bolum Nitelik",
                "Ada",
                "Parsel",
                "Blok",
                "Kat",
                "Giris",
                "BBNo",
            ]
            
            output_path.parent.mkdir(parents=True, exist_ok=True)
            df_clean.to_excel(output_path, index=False)
            logger.info(f"Excel file generated: {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to generate Excel file: {str(e)}")
            return False
    
    def generate_csv(self, df: pd.DataFrame, output_path: Path) -> bool:
        """
        Generate CSV file from processed data.
        
        Args:
            df: Processed DataFrame
            output_path: Path to save CSV file
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if df.empty:
                logger.warning("No data to export to CSV")
                return False
            
            # Use the same cleaned data as Excel
            df_clean = df[['source_file', 's/b/i', 'haciz_type', 'aciklama',
                          'aciklama_ext', 'tarih', 'aciklama_fileno', 'yevmiye', 'icra_dairesi',
                          'il', 'ilce', 'mahalle', 'bagimsiz_bolum_nitelik', 'ada', 'parsel',
                          'blok', 'kat', 'giris', 'bbno']]
            
            df_clean.columns = [
                "Kaynak Dosyasi",
                "S/B/I",
                "Haciz Turu",
                "Aciklama",
                "Aciklama Extracted",
                "Tarih",
                "Dosya Numarasi",
                "Yevmiye",
                "Icra Dairesi",
                "Il",
                "Ilce",
                "Mahalle",
                "Bagimsiz Bolum Nitelik",
                "Ada",
                "Parsel",
                "Blok",
                "Kat",
                "Giris",
                "BBNo",
            ]
            
            output_path.parent.mkdir(parents=True, exist_ok=True)
            df_clean.to_csv(output_path, index=False, encoding='utf-8-sig')
            logger.info(f"CSV file generated: {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to generate CSV file: {str(e)}")
            return False


# Convenience function for Flask integration
def process_pdf_files(
    pdf_files: List[Path],
    output_format: str = "excel",
    output_path: Optional[Path] = None,
    clean_watermarks: bool = True,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    """
    Convenience function for Flask integration.
    
    Args:
        pdf_files: List of PDF file paths
        output_format: Output format ('excel', 'csv', 'text')
        output_path: Output file path (optional)
        clean_watermarks: Whether to remove watermarks
        
    Returns:
        Dictionary with processing results
    """
    processor = PDFProcessor()
    
    # Process PDFs
    df = processor.process_multiple_pdfs(
        pdf_files,
        clean_watermarks,
        progress_callback=progress_callback,
    )
    
    if df.empty:
        if progress_callback:
            progress_callback({
                "event": "error",
                "message": "No data extracted from PDFs"
            })
        return {"success": False, "message": "No data extracted from PDFs"}
    
    # Generate output
    if output_path is None:
        output_path = processor.temp_dir / f"output.{output_format}"
    
    success = False
    if output_format.lower() == "excel":
        success = processor.generate_excel(df, output_path)
    elif output_format.lower() == "csv":
        success = processor.generate_csv(df, output_path)
    else:
        error_message = f"Unsupported output format: {output_format}"
        if progress_callback:
            progress_callback({
                "event": "error",
                "message": error_message
            })
        return {"success": False, "message": error_message}
    
    if success:
        if progress_callback:
            progress_callback({
                "event": "output_ready",
                "message": f"Output file generated: {output_path.name}",
                "output_path": str(output_path)
            })
        return {
            "success": True,
            "message": f"Successfully processed {len(pdf_files)} PDFs",
            "output_path": output_path,
            "dataframe_shape": df.shape
        }
    else:
        if progress_callback:
            progress_callback({
                "event": "error",
                "message": "Failed to generate output file"
            })
        return {"success": False, "message": "Failed to generate output file"}


if __name__ == "__main__":
    # Example usage
    processor = PDFProcessor()
    
    # Process a single PDF
    test_pdf = Path("test.pdf")
    if test_pdf.exists():
        result = processor.process_single_pdf(test_pdf)
        if result is not None:
            print(f"Processed {len(result)} rows from {test_pdf}")
            
            # Generate outputs
            processor.generate_excel(result, Path("output.xlsx"))
            processor.generate_csv(result, Path("output.csv"))
    else:
        print("Test PDF not found. Please create a test.pdf file for testing.")
