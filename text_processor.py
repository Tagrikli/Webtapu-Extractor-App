"""
Text processing utilities for Turkish text normalization and cleaning.
"""
import re
from typing import Optional

from icu import UnicodeString, Locale


class TextProcessor:
    """Handles Turkish text normalization, cleaning, and formatting."""
    
    def __init__(self):
        """Initialize with Turkish locale."""
        self.tr_locale = Locale("TR")
    
    def clean(self, value: Optional[str]) -> Optional[str]:
        """Clean and normalize text by removing newlines and extra spaces."""
        if value is None:
            return None
        
        value = str(value).replace("\n", "")
        parts = value.split()
        
        if len(parts) > 0:
            value = " ".join(value.split()).strip()
        else:
            value = None
        
        return value
    
    def upper(self, text: Optional[str]) -> Optional[str]:
        """Convert text to uppercase using Turkish locale."""
        if text is None:
            return None
        
        s = UnicodeString(str(text))
        text = str(s.toUpper(self.tr_locale))
        return text
    
    def lower(self, text: Optional[str]) -> Optional[str]:
        """Convert text to lowercase using Turkish locale."""
        if text is None:
            return None
        
        s = UnicodeString(str(text))
        text = str(s.toLower(self.tr_locale))
        return text
    
    def capitalize(self, text: Optional[str]) -> Optional[str]:
        """Capitalize text using Turkish locale."""
        if text is None:
            return None
        
        s = UnicodeString(str(text))
        text = str(s.toTitle(self.tr_locale))
        return text
    
    def extract_ada_parsel(self, value: str) -> tuple[Optional[str], Optional[str]]:
        """Extract ada and parsel from value."""
        values = [self.clean(part) for part in str(value).split('/')]
        if len(values) >= 2:
            return values[0], values[1]
        return None, None
    
    def extract_blok_kat_giris_bbno(self, value: str) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
        """Extract blok, kat, giris, and bbno from value."""
        parts = str(value).split('/')
        parts = [self.clean(part) for part in parts]
        
        if len(parts) < 4:
            return None, None, None, None
        
        blok = parts[0]
        kat = parts[1]
        giris = parts[2]
        bbno = parts[3]
        
        if kat is not None and not kat.isdigit():
            kat = re.sub(r"(\d+)\.(\S)", r"\1. \2", kat)
            kat = " ".join([str(self.capitalize(word)) for word in kat.split()])
        
        return blok, kat, giris, bbno
    
    def extract_il_ilce(self, value: str) -> tuple[Optional[str], Optional[str]]:
        """Extract il and ilce from value."""
        parts = str(value).split('/')
        parts = [self.clean(part) for part in parts]
        parts = [self.capitalize(part) for part in parts]
        
        if len(parts) >= 2:
            return parts[0], parts[1]
        return None, None


# Global instance for convenience
text_processor = TextProcessor()