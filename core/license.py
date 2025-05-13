import logging
from datetime import datetime
from typing import List, Dict, Optional
import pandas as pd
from rapidfuzz import fuzz, process
import re
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

class LicenseVerifier:
    """Handle TX license verification with TDLR CSV data"""

    def __init__(self, csv_path: str = "dataset/TDLR_All_Licenses.csv"):
        self.csv_path = csv_path
        self.license_data = None
        self._load_license_data()

    def _load_license_data(self):
        """Load and preprocess license data from CSV"""
        try:
            sample_data = pd.read_csv(self.csv_path, nrows=5)
            logger.info(f"CSV columns found: {list(sample_data.columns)}")
            
            self.license_data = pd.read_csv(
                self.csv_path,
                dtype=str  
            )
            
            lic_number_col = self._find_column(['LICENSE NUMBER', 'LICENSE #', 'LIC_NUMBER', 'LICENSE_NUMBER'])
            expiry_col = self._find_column(['LICENSE EXPIRATION DATE', 'EXPIRATION DATE', 'EXPIRATION', 'EXP_DATE'])
            business_name_col = self._find_column(['BUSINESS NAME', 'NAME', 'COMPANY NAME', 'COMPANY'])
            
            if not (lic_number_col and expiry_col and business_name_col):
                logger.warning(f"Could not identify all required columns. Found: License={lic_number_col}, Expiry={expiry_col}, Business={business_name_col}")
                if not business_name_col and len(self.license_data.columns) >= 1:
                    business_name_col = self.license_data.columns[0]
                if not lic_number_col and len(self.license_data.columns) >= 2:
                    lic_number_col = self.license_data.columns[1]
                if not expiry_col and len(self.license_data.columns) >= 9:
                    expiry_col = self.license_data.columns[8]  
            
            column_mapping = {}
            if business_name_col:
                column_mapping[business_name_col] = 'BUSINESS_NAME'
            if lic_number_col:
                column_mapping[lic_number_col] = 'LICENSE_NUMBER'
            if expiry_col:
                column_mapping[expiry_col] = 'EXPIRATION_DATE'
                
            if column_mapping:
                self.license_data = self.license_data.rename(columns=column_mapping)
            
            if 'BUSINESS_NAME' in self.license_data.columns:
                self.license_data['BUSINESS_NAME'] = self.license_data['BUSINESS_NAME'].astype(str).str.strip().str.upper()
                self.license_data = self.license_data.dropna(subset=['BUSINESS_NAME'])
            
            logger.info(f"Successfully loaded {len(self.license_data)} license records with columns: {list(self.license_data.columns)}")
        except Exception as e:
            logger.error(f"Failed to load license data: {str(e)}")
            raise RuntimeError("License verification unavailable - data loading failed")

    def _find_column(self, possible_names):
        """Find a column in the DataFrame by checking possible names"""
        for name in possible_names:
            matches = [col for col in self.license_data.columns if name.upper() in col.upper()]
            if matches:
                return matches[0]
        return None

    def _parse_expiry_date(self, date_str):
        """Parse expiration date with multiple format handling"""
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
        """Process a batch of profiles by best fuzzy business name match (>=85%)"""
        if self.license_data is None:
            logger.error("License data not loaded, skipping verification")
            return profiles

        results = []
        for profile in profiles:
            result = await self._verify_profile(profile)
            results.append(result)
        return results

    async def _verify_profile(self, profile: Dict) -> Dict:
        """Enhanced verification with improved matching and date parsing"""
        try:
            if self.license_data is None or self.license_data.empty:
                logger.error("[License] License data not loaded, skipping verification")
                return {**profile, "lic_active": False, "lic_number": "Unknown", "lic_match_score": 0}
                
            business_name = str(profile.get("business_name", "")).strip().upper()
            website = str(profile.get("website", "")).strip()
            
            if not business_name and website:
                logger.info("[License] Extracting business name from website")
                domain = urlparse(website).netloc
                business_name = re.sub(
                    r'^www\.|\.(com|net|org|co|us|gov|info|biz|io|ai)$', 
                    '', 
                    domain
                ).upper()

            if not business_name:
                logger.warning("[License] No business name available for verification")
                return {**profile, "lic_active": False, "lic_number": "Unknown", "lic_match_score": 0}

            license_text = profile.get("licensing_text", "")
            extracted_license = None
            
            if license_text:
                license_match = re.search(r'(?:license|lic)(?:.{0,10})(?:#|number|num|no)(?:.{0,5})([A-Z0-9-]{5,15})', 
                                         license_text, re.IGNORECASE)
                if license_match:
                    extracted_license = license_match.group(1).strip()
                    logger.info(f"[License] Extracted license number from text: {extracted_license}")
            
            if extracted_license and 'LICENSE_NUMBER' in self.license_data.columns:
                license_matches = self.license_data[
                    self.license_data['LICENSE_NUMBER'].astype(str).str.strip().str.upper() == 
                    extracted_license.upper()
                ]
                
                if not license_matches.empty:
                    logger.info(f"[License] Found exact license match: {extracted_license}")
                    match_row = license_matches.iloc[0]
                    return self._create_verified_response(profile, match_row, 100, match_row.get('BUSINESS_NAME', ''))
            
            if 'BUSINESS_NAME' in self.license_data.columns:
                business_names = self.license_data['BUSINESS_NAME'].tolist()
                
                match = process.extractOne(
                    business_name,
                    business_names,
                    scorer=fuzz.token_set_ratio,
                    score_cutoff=85  
                )

                if match:
                    matched_name, score, idx = match
                    match_row = self.license_data.iloc[idx]
                    logger.info(f"[License] Found match for '{business_name}' -> '{matched_name}' with score {score}")
                    return self._create_verified_response(profile, match_row, score, matched_name)
            
            business_names = self.license_data['BUSINESS_NAME'].tolist()
            alternate_match = process.extractOne(
                business_name,
                business_names,
                scorer=fuzz.partial_ratio,  
                score_cutoff=90
            )
            
            if alternate_match:
                matched_name, score, idx = alternate_match
                match_row = self.license_data.iloc[idx]
                logger.info(f"[License] Found alternate match for '{business_name}' -> '{matched_name}' with score {score}")
                return self._create_verified_response(profile, match_row, score, matched_name)

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
    
    def _create_verified_response(self, profile, match_row, score, matched_name):
        """Create a standardized response for a verified license match"""
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
            expiry_date = self._parse_expiry_date(expiry_str)
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