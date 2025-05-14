import asyncio
import re
import httpx
import logging
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

class SubcontractorExtractor:
    def __init__(self):
        self.patterns = {
            "phone": r"(?:\+?\d{1,2}\s?)?(?:\(?\d{3}\)?[\s.-]?)?\d{3}[\s.-]?\d{4}\b",
            "email": r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b",
            "license": {
                "generic": r"(?:lic(?:ense)?|reg(?:istration)?)[#:]?\s*([A-Z0-9-]{8,15})",
                "texas": r"(?:TDLR|TX)\s*(?:lic(?:ense)?\s*)?#?\s*([A-Z0-9-]{8,15})"
            },
            "bond": r"\$?(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s?(million|M|thousand|K)?\s+bond",
            "address": r"\d{1,5}\s[\w\s]+\n?[\w\s]+,?\s(TX|Texas)\s\d{5}",
            "union": r"\b(non-?union|unionized|open shop|closed shop)\b"
        }
        
        self.project_keywords = [
            "project", "portfolio", "completed", "construction",
            "built", "developed", "work", "client", "hospitality",
            "commercial", "hotel", "facility"
        ]


    async def extract_profiles(self, urls: List[str]) -> List[Dict]:
        """Orchestrate parallel extraction with quality control"""
        logger.info(f"[Extractor] URLs to process: {urls}")

        valid_url = [url for url in urls if url]
        

        async with httpx.AsyncClient(timeout=30) as client:

            tasks = [self._process_url(client, url) for url in valid_url]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            # print(f"[Extractor] Raw results: {results}")
            filtered = [r for r in results if not isinstance(r, Exception) and self._validate_profile(r)]
            # print(f"[Extractor] Filtered/validated results: {filtered}")
            return filtered

    async def _process_url(self, client: httpx.AsyncClient, url: str) -> Dict:
        """Process a single URL with focused extraction"""
        try:
            
            response = await client.get(url, follow_redirects=True)
            if response.status_code != 200:
                logger.warning(f"[Extractor] Failed to fetch {url} - status code: {response.status_code}")
                return self._create_minimal_profile(url)
                
            soup = BeautifulSoup(response.text, 'html.parser')
            text = soup.get_text(" ", strip=True)
            
            business_name = self._extract_business_name(soup, url)
            logger.info(f"[Extractor] Extracted business name: {business_name}")
            
            profile = {
                "business_name": business_name,
                "website": url,
                "email": self._extract_email(soup, text),
                "hq_address": self._extract_address(text),
                "phone": self._extract_phone(soup, text),
                "licensing_text": self._extract_license_text(text),
                "bond_amount": self._parse_bond(text),
                "projects": self._extract_projects(text),
                "union_status": self._detect_union_status(text),
                "evidence_text": self._extract_evidence(soup, text),
                "last_checked": datetime.utcnow().isoformat()
            }
            
            logger.debug(f"[Extractor] Extracted profile: {profile}")
            
            if profile["hq_address"]:
                address_parts = profile["hq_address"].split(",")
                if len(address_parts) >= 2:
                    if len(address_parts) >= 3:
                        profile["city"] = address_parts[-2].strip()
                        state_zip = address_parts[-1].strip().split()
                        if state_zip:
                            profile["state"] = state_zip[0].strip()
                    else:
                        profile["city"] = address_parts[0].strip()
                        state_zip = address_parts[1].strip().split()
                        if state_zip:
                            profile["state"] = state_zip[0].strip()
            
            return profile
        
        except Exception as e:
            logger.error(f"Error processing {url}: {str(e)}", exc_info=True)
            return self._create_minimal_profile(url)

    def _extract_business_name(self, soup: BeautifulSoup, url: str) -> str:
        """Extract business name with multiple fallback strategies"""
        for selector in [
            'meta[property="og:site_name"]',
            'meta[name="application-name"]',
            'h1',
            'title'
        ]:
            element = soup.select_one(selector)
            if element:
                if selector == 'title' and element.string:
                    name = element.string.strip()
                    name = re.sub(r'\s*[|]\s*.+$', '', name)  
                    name = re.sub(r'\s*[-]\s*.+$', '', name)
                    name = re.sub(r'(Home|Official Site|Homepage|Welcome)$', '', name, flags=re.IGNORECASE)
                    return name.strip()
                elif selector.startswith('meta') and element.get('content'):
                    return element.get('content').strip()
                elif element.string:
                    return element.string.strip()
        
        for logo_sel in ['.logo', '.brand', '#logo', '#brand', '.navbar-brand']:
            logo = soup.select_one(logo_sel)
            if logo:
                if logo.string and logo.string.strip():
                    return logo.string.strip()
                img = logo.find('img')
                if img and img.get('alt') and img.get('alt').strip():
                    return img.get('alt').strip()
        
        try:
            domain = urlparse(url).netloc
            domain_name = re.sub(r'^www\.', '', domain)
            domain_name = re.sub(r'\.(com|org|net|io|co|us|gov)$', '', domain_name)
            domain_name = re.sub(r'[-_]', ' ', domain_name)
            if '.' in domain_name:
                parts = domain_name.split('.')
                if len(parts) > 1:
                    domain_name = parts[-2] 
            return domain_name.title()
        except Exception as e:
            return "Unknown Business"

    def _extract_address(self, text: str) -> Optional[str]:
        """Extract physical address from text"""
        match = re.search(self.patterns["address"], text, re.IGNORECASE)
        return match.group(0) if match else None
    
    def _extract_email(self, soup: BeautifulSoup, text: str) -> Optional[str]:
        for selector in ['a[href^="mailto:"]', '[itemprop="email"]']:
            element = soup.select_one(selector)
            if element:
                href = element.get('href', '')
                if href.startswith('mailto:'):
                    email = href.replace('mailto:', '').strip()
                    if email:
                        return email
                email_text = element.get_text(strip=True)
                if email_text:
                    return email_text
        match = re.search(self.patterns["email"], text)
        return match.group(0) if match else None
                
    def _extract_license_text(self, text: str) -> Optional[str]:
        """Extract Licensing text"""
        license_patterns = [self.patterns["license"]["generic"], self.patterns["license"]["texas"]]
        regex_pattern = r"(licensed|certified|registered).{0,50}(" + "|".join(license_patterns) + r")"
        license_matches = [m for m in re.finditer(regex_pattern, text, re.IGNORECASE)]
        return license_matches[0].group(0) if license_matches else None

    def _parse_bond(self, text: str) -> Optional[int]:
        """Parse bonding capacity with unit conversion"""
        match = re.search(self.patterns["bond"], text, re.IGNORECASE)
        if not match:
            return None

        amount = float(match.group(1).replace(',', ''))
        multiplier = {"million": 1e6, "M": 1e6, "thousand": 1e3, "K": 1e3}.get(match.group(2), 1)
        return int(amount * multiplier)

    def _detect_union_status(self, text: str) -> Optional[str]:
        """Detect union status with context awareness"""
        if 'non-union' in text.lower():
            return "Non-Union"
        if 'union' in text.lower():
            return "Union"
        return None

    def _extract_projects(self, text: str) -> List[str]:
        """Extract project snippets with keyword density analysis"""
        sentences = re.split(r'(?<=[.!?])\s+', text)
        
        return [s.strip() for s in sentences[:100]
                if sum(kw in s.lower() for kw in self.project_keywords) >= 2
                and 20 < len(s) < 500][:5]  

    def _extract_evidence(self, soup: BeautifulSoup, text: str) -> str:
        """Extract relevant evidence text chunks"""
        sections = []
        for selector in ['#about', '#services', '.projects', '#portfolio']:
            if section := soup.select_one(selector):
                sections.append(section.get_text(" ", strip=True)[:300])
        return " [...] ".join(sections) if sections else text[:500]

    def _validate_profile(self, profile: Dict) -> bool:
        """
        Validate profile data with lenient requirements.
        Only require business_name and website to be present.
        """
        if not profile:
            logger.warning("Empty profile received for validation")
            return False
            
        valid = bool(profile.get("business_name")) and bool(profile.get("website"))
        
        if not valid:
            logger.warning(f"[Extractor] Profile validation failed: missing business_name or website")
        
        return valid
        
    def _extract_phone(self, soup: BeautifulSoup, text: str) -> Optional[str]:
        """Extract phone number with multiple strategies"""
        for selector in ['a[href^="tel:"]', '[itemprop="telephone"]', '.phone', '.tel', '#phone']:
            element = soup.select_one(selector)
            if element:
                href = element.get('href', '')
                if href.startswith('tel:'):
                    phone = href.replace('tel:', '').strip()
                    if phone:
                        return self._normalize_phone(phone)
                phone_text = element.get_text(strip=True)
                if phone_text:
                    return self._normalize_phone(phone_text)
                    
        match = re.search(self.patterns["phone"], text)
        if match:
            return self._normalize_phone(match.group(0))
            
        return None
        
    def _normalize_phone(self, phone: str) -> str:
        """Normalize phone number format"""
        digits = re.sub(r'\D', '', phone)
        
        if len(digits) == 10:
            return f"({digits[0:3]}) {digits[3:6]}-{digits[6:10]}"
        elif len(digits) == 11 and digits[0] == '1':
            return f"({digits[1:4]}) {digits[4:7]}-{digits[7:11]}"
        
        return phone

    def _create_minimal_profile(self, url: str) -> Dict:
        """Fallback profile structure"""
        return {
            "business_name": urlparse(url).netloc,
            "website": url,
            "hq_address": None,
            "phone": None,
            "licensing_text": None,
            "bond_amount": None,
            "projects": [],
            "union_status": None,
            "evidence_text": "",
            "last_checked": datetime.utcnow().isoformat()
        }