import logging
from datetime import datetime
from typing import List, Dict, Optional
import pandas as pd
from rapidfuzz import fuzz, process
import re

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
            self.license_data = pd.read_csv(
                self.csv_path,
                usecols=[
                    'LICENSE NUMBER',
                    'BUSINESS NAME',
                    'LICENSE EXPIRATION DATE (MMDDCCYY)'
                ],
                dtype={
                    'LICENSE NUMBER': 'string',
                    'BUSINESS NAME': 'string',
                    'LICENSE EXPIRATION DATE (MMDDCCYY)': 'string'
                }
            )
            # Clean data
            self.license_data['BUSINESS NAME'] = self.license_data['BUSINESS NAME'].str.strip().str.upper()
            self.license_data = self.license_data.dropna(subset=['BUSINESS NAME'])
            logger.info(f"Successfully loaded {len(self.license_data)} license records")
        except Exception as e:
            logger.error(f"Failed to load license data: {str(e)}")
            raise RuntimeError("License verification unavailable - data loading failed")

    async def verify_batch(self, profiles: List[Dict]) -> List[Dict]:
        """Process a batch of profiles by best fuzzy business name match (>=98%)"""
        if self.license_data is None:
            logger.error("License data not loaded, skipping verification")
            return profiles

        results = []
        for profile in profiles:
            result = await self._verify_profile(profile)
            results.append(result)
        return results

    async def _verify_profile(self, profile: Dict) -> Dict:
        """
        Verify a single profile by best fuzzy business name match (>=95% threshold).
        Adds lic_active and lic_number fields to the profile.
        """
        try:
            if not self.license_data:
                logger.error("[License] License data not loaded, skipping verification")
                return {**profile, "lic_active": False, "lic_number": "Unknown"}
                
            business_name = profile.get("business_name", "").strip().upper()
            if not business_name:
                logger.warning(f"[License] Profile missing business_name, using website instead")
                # Try to extract business name from website as fallback
                website = profile.get("website", "")
                if website:
                    from urllib.parse import urlparse
                    domain = urlparse(website).netloc
                    # Remove www. and extension
                    domain = re.sub(r'^www\.', '', domain)
                    domain = re.sub(r'\.(com|net|org|co|us|gov)$', '', domain)
                    business_name = domain.upper()
                
            if not business_name:
                logger.warning("[License] Could not determine business name for license verification")
                return {**profile, "lic_active": False, "lic_number": "Unknown"}
                
            logger.info(f"[License] Verifying license for: {business_name}")

            # Fuzzy match with at least 95% accuracy (slightly more lenient than 98%)
            match = process.extractOne(
                business_name,
                self.license_data['BUSINESS NAME'],
                scorer=fuzz.token_set_ratio,
                score_cutoff=95
            )

            if not match:
                logger.info(f"[License] No license match found for {business_name}")
                return {**profile, "lic_active": False, "lic_number": "Unknown"}

            matched_name, score, idx = match
            match_row = self.license_data.iloc[idx]
            lic_number = match_row['LICENSE NUMBER']
            expiry_str = match_row['LICENSE EXPIRATION DATE (MMDDCCYY)']

            logger.info(f"[License] Found match: {matched_name} with score {score}%")
            logger.info(f"[License] License #: {lic_number}, Expiry: {expiry_str}")

            # Check expiry date (LICENSE EXPIRATION DATE (MMDDCCYY))
            is_active = False
            if expiry_str and isinstance(expiry_str, str) and expiry_str.isdigit() and len(expiry_str) == 8:
                try:
                    expiry_date = datetime.strptime(expiry_str, "%m%d%Y").date()
                    is_active = expiry_date > datetime.now().date()
                    logger.info(f"[License] License expiry: {expiry_date}, Active: {is_active}")
                except Exception as e:
                    logger.error(f"[License] Error parsing expiry date: {str(e)}")
                    is_active = False
            else:
                logger.warning(f"[License] Invalid expiry format: {expiry_str}")
                is_active = False

            # Create a new profile with license data added
            result = {
                **profile, 
                "lic_active": is_active, 
                "lic_number": lic_number,
                "lic_match_score": score,
                "lic_matched_name": matched_name
            }
            
            return result
        except Exception as e:
            logger.error(f"[License] Verification failed for {profile.get('business_name')}: {str(e)}")
            return {**profile, "lic_active": False, "lic_number": "Unknown"}
