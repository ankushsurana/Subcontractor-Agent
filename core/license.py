# # license.py
# import asyncio
# from asyncio.log import logger
# import httpx
# import os
# from bs4 import BeautifulSoup
# from typing import List, Dict, Optional
# import re
# import logging

# logger = logging.getLogger(__name__)

# class LicenseVerifier:
#     """Service to verify contractor licenses from extracted profiles"""
    
#     def __init__(self):
#         # Use string formatting and provide a default value if TDLR_URL is None
#         tdlr_url = os.getenv("TDLR_URL", "")
#         self.base_url = f"{tdlr_url}/contractorsearch/ContractorSearch.aspx" if tdlr_url else ""
#         self.state_license_patterns = {
#             "TX": r'(?:TX|Texas)[\s#-]?(\d{5,10})',
#             "FL": r'(?:FL|Florida)[\s#-]?(\w{9,12})',
#             "CA": r'(?:CA|California)[\s#-]?(\d{6,12})',
#             # Add more states as needed
#         }

#     async def verify_batch(self, profiles: List[Dict]) -> List[Dict]:
#         """Process a batch of profiles to verify licenses"""
#         tasks = [self._verify_profile(profile) for profile in profiles]
#         return await asyncio.gather(*tasks)
    
#     async def _verify_profile(self, profile: Dict) -> Dict:
#         """Verify license for a single profile"""
#         try:
#             # Add license verification status
#             profile['lic_active'] = False
            
#             # If we have a license field and there's text in it
#             if profile.get('license'):
#                 # For now, we just assume it's active if found
#                 # In a real system, you'd check with state license databases
#                 profile['lic_active'] = True
                
#                 # Try to detect license number format if not already extracted
#                 if not re.match(r'\d+', str(profile['license'])):
#                     raw_text = profile.get('raw_text', '')
#                     for state, pattern in self.state_license_patterns.items():
#                         match = re.search(pattern, raw_text)
#                         if match:
#                             profile['license'] = match.group(1)
#                             break
#         except Exception as e:
#             logger.error(f"Error verifying license: {str(e)}")
        
#         return profile

#     # Note: These methods have issues. Fixing the duplicate method and using the correct self-reference
#     async def _check_license(self, client: httpx.AsyncClient, profile: Dict) -> Dict:
#         """Check license status with external service"""
#         if not self.base_url or not profile.get("license"):
#             return {**profile, "lic_active": False}
            
#         try:
#             params = {"SearchType": "License", "LicenseNumber": profile["license"]}
#             response = await client.get(self.base_url, params=params)
#             profile["lic_active"] = self._parse_license_status(response.text)
#             return profile
#         except Exception as e:
#             logger.error(f"License check error: {str(e)}")
#             return {**profile, "lic_active": False}

#     def _parse_license_status(self, html: str) -> bool:
#         """Parse HTML to determine if license is active"""
#         try:
#             soup = BeautifulSoup(html, "lxml")
#             status_row = soup.find("th", string="License Status")
#             if status_row:
#                 status_text = status_row.find_next_sibling("td").text.lower()
#                 return "active" in status_text
#             return False
#         except Exception as e:
#             logger.error(f"Error parsing license status: {str(e)}")
#             return False



# license.py (updated)
import asyncio
import httpx
from bs4 import BeautifulSoup
from typing import List, Dict
import logging
import os

logger = logging.getLogger(__name__)

class LicenseVerifier:
    """Handle TX license verification with TDLR"""
    
    def __init__(self):
        self.tdlr_url = os.getenv("TDLR_URL", "https://www.tdlr.texas.gov")

    async def verify_batch(self, profiles: List[Dict]) -> List[Dict]:
        """Process a batch of profiles"""
        async with httpx.AsyncClient() as client:
            tasks = [self._verify_license(client, p) for p in profiles]
            return await asyncio.gather(*tasks)

    async def _verify_license(self, client: httpx.AsyncClient, profile: Dict) -> Dict:
        """Verify single profile's license"""
        # Use lic_number from profile, or fall back to license field for backward compatibility
        license_number = profile.get("lic_number") or profile.get("license")
        
        if not license_number:
            profile["lic_active"] = False
            return profile
            
        try:
            search_url = f"{self.tdlr_url}/contractorsearch/ContractorSearch.aspx"
            params = {"SearchType": "License", "LicenseNumber": license_number}
            
            response = await client.get(search_url, params=params)
            
            # Set license status
            profile["lic_active"] = self._parse_status(response.text)
            
            # Ensure lic_number field exists for consistency
            if not profile.get("lic_number") and profile.get("license"):
                profile["lic_number"] = profile["license"]
                
            return profile
            
        except Exception as e:
            logger.error(f"License verification failed: {str(e)}")
            profile["lic_active"] = False
            return profile

    def _parse_status(self, html: str) -> bool:
        """Parse license status from TDLR HTML"""
        try:
            soup = BeautifulSoup(html, 'lxml')
            status_label = soup.find("th", string="License Status")
            if not status_label:
                return False
                
            status_value = status_label.find_next_sibling("td")
            return bool(status_value and "active" in status_value.text.lower())
        except Exception as e:
            logger.error(f"Error parsing license status: {str(e)}")
            return False