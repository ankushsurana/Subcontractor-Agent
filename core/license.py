import logging
import os
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlparse
import re
import asyncio
from functools import lru_cache

import pandas as pd
from rapidfuzz import fuzz, process
import numpy as np
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

class LicenseVerifier:

    def __init__(self, csv_path: str = None, chunk_size: int = 500000):
        if csv_path is None:
            csv_path = os.environ.get('TDLR_CSV_PATH', 'D:\Subcontractor Research Agent\dataset\TDLR_All_Licenses.csv')

        self.csv_path = os.path.normpath(csv_path)
        self.license_data = None
        self.chunk_size = chunk_size
        self.business_name_index = None
        self.license_number_index = None
        self.executor = ThreadPoolExecutor(max_workers=os.cpu_count())
        self._load_license_data()

    def _find_csv_file(self) -> str:
        """Find the CSV file using multiple possible paths"""
        if os.path.exists(self.csv_path):
            return self.csv_path
            
        logger.warning(f"CSV file not found at {self.csv_path}, trying alternate paths")
        
        alternate_paths = [
            os.path.normpath('dataset/TDLR_All_Licenses.csv'),  
            os.path.normpath('./dataset/TDLR_All_Licenses.csv'),
            os.path.normpath('../dataset/TDLR_All_Licenses.csv'),  
            os.path.normpath('/app/dataset/TDLR_All_Licenses.csv'),
        ]
        
        if '\\' in self.csv_path:
            alternate_paths.append(self.csv_path.replace('\\', '/'))
        if '/' in self.csv_path:
            alternate_paths.append(self.csv_path.replace('/', '\\'))
            
        for path in alternate_paths:
            if os.path.exists(path):
                return path
                
        try:
            if os.path.exists('dataset'):
                logger.error(f"Dataset directory contents: {os.listdir('dataset')}")
        except Exception as e:
            logger.error(f"Failed to list directory contents: {e}")
        
        raise FileNotFoundError(f"License CSV file not found at {self.csv_path} or any alternate paths")

    def _identify_columns(self, sample_df: pd.DataFrame) -> Tuple[str, str, str]:
        lic_number_col = self._find_column(sample_df, ['LICENSE NUMBER', 'LICENSE #', 'LIC_NUMBER', 'LICENSE_NUMBER'])
        expiry_col = self._find_column(sample_df, ['LICENSE EXPIRATION DATE', 'EXPIRATION DATE', 'EXPIRATION', 'EXP_DATE'])
        business_name_col = self._find_column(sample_df, ['BUSINESS NAME', 'NAME', 'COMPANY NAME', 'COMPANY'])
        
        if not (lic_number_col and expiry_col and business_name_col):
            logger.warning(f"Could not identify all required columns. Found: License={lic_number_col}, Expiry={expiry_col}, Business={business_name_col}")
            if not business_name_col and len(sample_df.columns) >= 1:
                business_name_col = sample_df.columns[0]
            if not lic_number_col and len(sample_df.columns) >= 2:
                lic_number_col = sample_df.columns[1]
            if not expiry_col and len(sample_df.columns) >= 9:
                expiry_col = sample_df.columns[8]
                
        return business_name_col, lic_number_col, expiry_col

    def _load_license_data(self):
        try:
            csv_path = self._find_csv_file()
            
            sample_df = pd.read_csv(csv_path, nrows=5)
            business_name_col, lic_number_col, expiry_col = self._identify_columns(sample_df)
            
            usecols = []
            col_mapping = {}
            
            if business_name_col:
                usecols.append(business_name_col)
                col_mapping[business_name_col] = 'BUSINESS_NAME'
            
            if lic_number_col:
                usecols.append(lic_number_col)
                col_mapping[lic_number_col] = 'LICENSE_NUMBER'
            
            if expiry_col:
                usecols.append(expiry_col)
                col_mapping[expiry_col] = 'EXPIRATION_DATE'
            
            self.license_data = pd.read_csv(
                csv_path,
                usecols=usecols if usecols else None,
                dtype=str,
                na_values=['NA', 'N/A', '', None],
                keep_default_na=False
            )
            
            self.license_data = self.license_data.rename(columns=col_mapping)
            
            if 'BUSINESS_NAME' in self.license_data.columns:
                self.license_data['BUSINESS_NAME'] = self.license_data['BUSINESS_NAME'].astype(str).str.strip().str.upper()
                self.license_data = self.license_data.dropna(subset=['BUSINESS_NAME'])
                
                if 'LICENSE_NUMBER' in self.license_data.columns:
                    self.license_number_index = dict(zip(
                        self.license_data['LICENSE_NUMBER'].str.strip().str.upper(),
                        range(len(self.license_data))
                    ))
                
                self.business_name_list = self.license_data['BUSINESS_NAME'].tolist()
            
            logger.info(f"Successfully loaded {len(self.license_data)} license records")
            
        except Exception as e:
            logger.error(f"Failed to load license data: {str(e)}", exc_info=True)
            raise RuntimeError("License verification unavailable - data loading failed")

    def _find_column(self, df: pd.DataFrame, possible_names: List[str]) -> Optional[str]:
        """Find a column in the DataFrame by checking possible names"""
        for name in possible_names:
            matches = [col for col in df.columns if name.upper() in col.upper()]
            if matches:
                return matches[0]
        return None

    @lru_cache(maxsize=1024)
    def _parse_expiry_date(self, date_str: str) -> Optional[datetime.date]:
        """Parse expiration date with multiple format handling and caching"""
        if not date_str or date_str == "Unknown" or pd.isna(date_str):
            return None
            
        date_str = str(date_str).strip()
        date_formats = [
            "%m%d%Y",      
            "%m/%d/%Y",     
            "%Y-%m-%d",     
            "%d-%m-%Y",     
            "%b %d, %Y",   
            "%B %d, %Y"     
        ]
        
        if re.match(r'^\d+$', date_str) and len(date_str) == 8:
            date_str = f"{date_str[:2]}/{date_str[2:4]}/{date_str[4:]}"
            date_formats.insert(0, "%m/%d/%Y") 
            
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        
        logger.warning(f"Could not parse date: {date_str}")
        return None

    async def verify_batch(self, profiles: List[Dict]) -> List[Dict]:
        if self.license_data is None:
            logger.error("License data not loaded, skipping verification")
            return profiles

        results = await asyncio.gather(*[self._verify_profile(profile) for profile in profiles])
        return results

    def _extract_business_name_from_website(self, website: str) -> str:
        """Extract business name from website domain"""
        if not website:
            return ""
            
        try:
            domain = urlparse(website).netloc
            business_name = re.sub(
                r'^www\.|\.(com|net|org|co|us|gov|info|biz|io|ai)$', 
                '', 
                domain
            ).upper()
            return business_name
        except Exception as e:
            logger.warning(f"Failed to extract business name from website: {e}")
            return ""

    def _extract_license_number(self, license_text: str) -> Optional[str]:
        if not license_text:
            return None
            
        try:
            license_match = re.search(
                r'(?:license|lic)(?:.{0,10})(?:#|number|num|no)(?:.{0,5})([A-Z0-9-]{5,15})', 
                license_text, 
                re.IGNORECASE
            )
            if license_match:
                return license_match.group(1).strip()
        except Exception:
            pass
            
        return None

    async def _verify_profile(self, profile: Dict) -> Dict:
        try:
            if self.license_data is None or self.license_data.empty:
                return {**profile, "lic_active": False, "lic_number": "Unknown", "lic_match_score": 0}
                
            business_name = str(profile.get("business_name", "")).strip().upper()
            website = str(profile.get("website", "")).strip()
            
            if not business_name and website:
                business_name = self._extract_business_name_from_website(website)

            if not business_name:
                return {**profile, "lic_active": False, "lic_number": "Unknown", "lic_match_score": 0}

            license_text = profile.get("licensing_text", "")
            extracted_license = self._extract_license_number(license_text)
            
            if extracted_license and self.license_number_index and 'LICENSE_NUMBER' in self.license_data.columns:
                extracted_license = extracted_license.upper()
                if extracted_license in self.license_number_index:
                    idx = self.license_number_index[extracted_license]
                    match_row = self.license_data.iloc[idx]
                    logger.info(f"[License] Found exact license match: {extracted_license}")
                    return await self._create_verified_response(profile, match_row, 100, match_row.get('BUSINESS_NAME', ''))
            
            if hasattr(self, 'business_name_list') and self.business_name_list:
                loop = asyncio.get_event_loop()
                match = await loop.run_in_executor(
                    self.executor,
                    lambda: process.extractOne(
                        business_name,
                        self.business_name_list,
                        scorer=fuzz.token_set_ratio,
                        score_cutoff=85
                    )
                )

                if match:
                    matched_name, score, idx = match
                    match_row = self.license_data.iloc[idx]
                    logger.info(f"[License] Found match for '{business_name}' -> '{matched_name}' with score {score}")
                    return await self._create_verified_response(profile, match_row, score, matched_name)
            
                alternate_match = await loop.run_in_executor(
                    self.executor,
                    lambda: process.extractOne(
                        business_name,
                        self.business_name_list,
                        scorer=fuzz.partial_ratio,
                        score_cutoff=90
                    )
                )
                
                if alternate_match:
                    matched_name, score, idx = alternate_match
                    match_row = self.license_data.iloc[idx]
                    logger.info(f"[License] Found alternate match for '{business_name}' -> '{matched_name}' with score {score}")
                    return await self._create_verified_response(profile, match_row, score, matched_name)

            logger.info(f"[License] No license match found for {business_name}")
            return {
                **profile, 
                "lic_active": False, 
                "lic_number": "Unknown",
                "lic_match_score": 0,
                "lic_matched_name": ""
            }
            
        except Exception as e:
            logger.error(f"[License] Verification failed: {str(e)}", exc_info=True)
            return {
                **profile,
                "lic_active": False,
                "lic_number": "Unknown",
                "lic_match_score": 0,
                "lic_matched_name": ""
            }
    
    async def _create_verified_response(self, profile, match_row, score, matched_name):
        lic_number = "Unknown"
        if 'LICENSE_NUMBER' in match_row:
            lic_number = str(match_row['LICENSE_NUMBER'])
        elif len(match_row) >= 2:
            lic_number = str(match_row.iloc[1])
        
        is_active = False
        expiry_date = None
        expiry_str = None
        
        if 'EXPIRATION_DATE' in match_row:
            expiry_str = str(match_row['EXPIRATION_DATE'])
        elif len(match_row) >= 9: 
            expiry_str = str(match_row.iloc[8])
        
        if expiry_str:
            loop = asyncio.get_event_loop()
            expiry_date = await loop.run_in_executor(
                self.executor,
                self._parse_expiry_date,
                expiry_str
            )
            
            if expiry_date:
                is_active = expiry_date > datetime.now().date()
                logger.info(f"[License] License expiry date: {expiry_date}, Active: {is_active}")
        
        return {
            **profile,
            "lic_active": is_active,
            "lic_number": lic_number,
            "lic_match_score": score,
            "lic_matched_name": matched_name,
            "lic_expiry_date": expiry_date.strftime("%Y-%m-%d") if expiry_date else "Unknown"
        }

    async def close(self):
        self.executor.shutdown(wait=False)


_license_verifier_instance = None

async def get_license_verifier(csv_path=None) -> LicenseVerifier:
    global _license_verifier_instance
    if _license_verifier_instance is None:
        _license_verifier_instance = LicenseVerifier(csv_path)
    return _license_verifier_instance