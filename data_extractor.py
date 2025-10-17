"""
Data extraction and processing functionality for Turkish property documents.
"""
import logging
import re
from typing import Dict, List, Optional, Tuple

import pandas as pd

from text_processor import text_processor

logger = logging.getLogger(__name__)


class DataExtractor:
    """Handles data extraction and processing from extracted tables."""
    
    def extract_general_info(self, dfs: List[pd.DataFrame]) -> Dict[str, Optional[str]]:
        """
        Extract general property information from the first two tables.
        
        Args:
            dfs: List of DataFrames from extracted tables
            
        Returns:
            Dictionary containing property information
        """
        if len(dfs) < 2:
            return {}
        
        col_1 = dfs[0]
        col_2 = dfs[1]
        
        try:
            tasinmaz_kimlik_no = int(str(col_1.iloc[1, 1]))
            il, ilce = text_processor.extract_il_ilce(str(col_1.iloc[2, 1]))
            blok, kat, giris, bbno = text_processor.extract_blok_kat_giris_bbno(str(col_2.iloc[5, 1]))
            kurum_adi = text_processor.clean(str(col_1.iloc[3, 1]))
            mahalle = text_processor.capitalize(str(col_1.iloc[4, 1]))
            ada, parsel = text_processor.extract_ada_parsel(str(col_2.iloc[0, 1]))
            bagimsiz_bolum_nitelik = text_processor.capitalize(text_processor.clean(str(col_2.iloc[2, 1])))
            
            info = {
                "tasinmaz_kimlik_no": tasinmaz_kimlik_no,
                "il": il,
                "ilce": ilce,
                "kurum_adi": kurum_adi,
                "mahalle": mahalle,
                "ada": ada,
                "parsel": parsel,
                "bagimsiz_bolum_nitelik": bagimsiz_bolum_nitelik,
                "blok": blok,
                "kat": kat,
                "giris": giris,
                "bbno": bbno,
            }
            
            return info
            
        except Exception as e:
            logger.error(f"Failed to extract general info: {str(e)}")
            return {}
    
    def extract_mulkiyete_ait_serh_beyan(self, dfs: List[pd.DataFrame]) -> pd.DataFrame:
        """Extract property restriction declarations."""
        from table_extractor import table_extractor
        
        dfs_ = [df for df in dfs if len(df.columns) == 6]
        
        if not dfs_:
            return pd.DataFrame()
        
        df = pd.concat(dfs_, ignore_index=True)
        
        if pd.isna(df.iloc[0, 0]) or df.iloc[0, 0] == "":
            df = df.drop(df.index[0]).reset_index(drop=True)
        
        df = table_extractor.fix_continuation(df)
        df = df.drop(df.index[0]).reset_index(drop=True)
        
        df.columns = [
            "s/b/i",
            "aciklama",
            "kisitli_malik_ad_soyad",
            "malik/lehtar",
            "tesis_kurum/tarih/yevmiye",
            "tarkin_sebebi/yevmiye",
        ]
        
        return df
    
    def extract_aciklama(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract and parse explanation fields."""
        r_tarih_fileno = r"(\d{2}\/\d{2}\/\d{4}|BİLA) TARİH (\d+\/\d+|\d+-\d+|\d+|[A-Fa-f0-9]{32})"
        r_aciklama_relevant = r": (.*(\d{2}\/\d{2}\/\d{4}|BİLA) TARİH (\d+\/\d+|\d+-\d+|\d+|[A-Fa-f0-9]{32}))"
        r_haciz_type = r'^([^:(]+)\s*:'
        
        haciz_type = df["aciklama"].str.extract(r_haciz_type)[0].fillna('').map(text_processor.clean).map(text_processor.capitalize)
        mask = haciz_type.notna()
        
        if mask.any():
            aciklama_relevant = df.loc[mask, "aciklama"].str.extract(r_aciklama_relevant)[0]
            tarih_fileno = df.loc[mask, "aciklama"].str.extract(r_tarih_fileno)
        else:
            aciklama_relevant = pd.Series(None, index=df.index)
            tarih_fileno = pd.DataFrame(None, index=df.index, columns=[0])
        
        concat = pd.concat([haciz_type, aciklama_relevant, tarih_fileno], axis=1)
        concat.columns = ['haciz_type', 'aciklama_ext', 'aciklama_date', 'aciklama_fileno']
        return concat
    
    def extract_date_yevmiye(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract date and journal number."""
        r_tarih_yevmiye = r'(\d{2}-\d{2}-\d{4}).*- (\d+)'
        concat = df['tesis_kurum/tarih/yevmiye'].str.extract(r_tarih_yevmiye)
        concat.columns = ['tarih', 'yevmiye']
        concat["yevmiye"] = pd.to_numeric(concat["yevmiye"], errors="coerce")
        concat["tarih"] = pd.to_datetime(concat["tarih"], format="%d-%m-%Y").dt.strftime("%d/%m/%Y")
        return concat
    
    def get_icra_dairesi(self, value):
        """Extract enforcement office from text."""
        if not pd.isna(value) and "NİN" in str(value):
            return str(value).split("NİN")[0].strip()
        return None
    
    def extract_icra_dairesi(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract and clean enforcement office information."""
        icra_dairesi = df["aciklama_ext"].map(self.get_icra_dairesi)
        icra_dairesi = icra_dairesi.str.replace(r'(?<=\d)\.(?!\s)', '. ', regex=True)
        icra_dairesi = icra_dairesi.str.replace(r'\.\.$', '', regex=True)
        icra_dairesi = icra_dairesi.str.replace(r'\bT\.C\. ?', '', regex=True)
        icra_dairesi = icra_dairesi.str.replace(r'İCRA MÜDÜRLÜĞÜ', 'İCRA DAİRESİ', regex=False)
        icra_dairesi = icra_dairesi.str.replace(r'GEBZE 4 İCRA DAİRESİ', 'GEBZE 4. İCRA DAİRESİ', regex=False)
        icra_dairesi = icra_dairesi.str.replace(r'ANADOLU 1 TÜKETİCİ', 'ANADOLU 1. TÜKETİCİ', regex=False)
        icra_dairesi = icra_dairesi.str.replace(r'İCRA DAİRESİ MÜDÜRLÜĞÜ', 'İCRA DAİRESİ', regex=False)
        icra_dairesi = icra_dairesi.str.replace(r'MEHKEMESİ', 'MAHKEMESİ', regex=False)
        icra_dairesi = icra_dairesi.str.replace(r'MAHKEMESİNE', 'MAHKEMESİ', regex=False)
        icra_dairesi = icra_dairesi.str.replace(r'([A-ZÇĞİÖŞÜ])BELEDİYESİ', r'\1 BELEDİYESİ', regex=True)
        icra_dairesi = icra_dairesi.str.replace(r'S.G.M.', r'SOSYAL GÜVENLİK MERKEZİ', regex=True)
        
        icra_dairesi = icra_dairesi.map(text_processor.clean).map(text_processor.capitalize)
        icra_dairesi.name = "icra_dairesi"
        icra_dairesi.index = df.index
        return icra_dairesi.to_frame()


# Global instance for convenience
data_extractor = DataExtractor()