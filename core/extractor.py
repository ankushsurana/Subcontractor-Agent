import asyncio
import re
import httpx
import logging
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from typing import List, Dict, Optional, Any, Tuple
from functools import lru_cache
from cachetools import TTLCache, cached
from concurrent.futures import ThreadPoolExecutor
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 20
MAX_CONCURRENT_REQUESTS = 50
MAX_RETRIES = 2
CACHE_SIZE = 1000
CACHE_TTL = 3600  

class SubcontractorExtractor:
    """Optimized extractor for subcontractor information from websites."""
    
    _PATTERNS = {
        "phone": re.compile(r"(?:\+?\d{1,2}\s?)?(?:\(?\d{3}\)?[\s.-]?)?\d{3}[\s.-]?\d{4}\b"),
        "email": re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b"),
        "license": {
            "generic": re.compile(r"(?:lic(?:ense)?|reg(?:istration)?)[#:]?\s*([A-Z0-9-]{8,15})"),
            "texas": re.compile(r"(?:TDLR|TX)\s*(?:lic(?:ense)?\s*)?#?\s*([A-Z0-9-]{8,15})")
        },
        "bond": re.compile(r"\$?(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s?(million|M|thousand|K)?\s+bond", re.IGNORECASE),
        "address": re.compile(r"\d{1,5}\s[\w\s]+\n?[\w\s]+,?\s(TX|Texas)\s\d{5}", re.IGNORECASE),
        "union": re.compile(r"\b(non-?union|unionized|open shop|closed shop)\b", re.IGNORECASE)
    }
    
    _PROJECT_KEYWORDS = frozenset([
        "project", "portfolio", "completed", "construction",
        "built", "developed", "work", "client", "hospitality",
        "commercial", "hotel", "facility"
    ])
    
    _BUSINESS_NAME_SELECTORS = [
        'meta[property="og:site_name"]',
        'meta[name="application-name"]',
        'h1',
        'title'
    ]
    
    _LOGO_SELECTORS = ['.logo', '.brand', '#logo', '#brand', '.navbar-brand']
    _PHONE_SELECTORS = ['a[href^="tel:"]', '[itemprop="telephone"]', '.phone', '.tel', '#phone']
    _EMAIL_SELECTORS = ['a[href^="mailto:"]', '[itemprop="email"]']
    _EVIDENCE_SELECTORS = ['#about', '#services', '.projects', '#portfolio']
    
    _BOND_MULTIPLIERS = {"million": 1e6, "M": 1e6, "thousand": 1e3, "K": 1e3}
    
    _result_cache = TTLCache(maxsize=CACHE_SIZE, ttl=CACHE_TTL)

    def __init__(self, 
                 max_concurrent_requests: int = MAX_CONCURRENT_REQUESTS, 
                 request_timeout: int = REQUEST_TIMEOUT,
                 max_retries: int = MAX_RETRIES):
        """
        Initialize the SubcontractorExtractor with configurable parameters.
        
        Args:
            max_concurrent_requests: Maximum number of concurrent HTTP requests
            request_timeout: Timeout in seconds for HTTP requests
            max_retries: Maximum number of retries for failed requests
        """
        self.max_concurrent_requests = max_concurrent_requests
        self.request_timeout = request_timeout
        self.max_retries = max_retries
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        
        license_patterns = [
            self._PATTERNS["license"]["generic"].pattern, 
            self._PATTERNS["license"]["texas"].pattern
        ]
        self._license_text_pattern = re.compile(
            r"(licensed|certified|registered).{0,50}(" + "|".join(license_patterns) + r")",
            re.IGNORECASE
        )

    async def extract_profiles(self, urls: List[str]) -> List[Dict]:
        """
        Orchestrate parallel extraction of profiles from multiple URLs with rate limiting.
        
        Args:
            urls: List of URLs to process
            
        Returns:
            List of extracted profile dictionaries
        """
        start_time = time.time()
        logger.info(f"[Extractor] Starting extraction for {len(urls)} URLs")
        
        valid_urls = [url for url in urls if url]
        
        if not valid_urls:
            logger.warning("[Extractor] No valid URLs provided")
            return []
        
        cached_results = []
        urls_to_process = []
        
        for url in valid_urls:
            cache_key = self._generate_cache_key(url)
            if cache_key in self._result_cache:
                cached_results.append(self._result_cache[cache_key])
                logger.debug(f"[Extractor] Cache hit for {url}")
            else:
                urls_to_process.append(url)
        
        if urls_to_process:
            limits = httpx.Limits(max_connections=self.max_concurrent_requests)
            transport = httpx.AsyncHTTPTransport(limits=limits, retries=self.max_retries)
            
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(self.request_timeout),
                follow_redirects=True,
                transport=transport
            ) as client:
                tasks = [self._process_url_with_rate_limit(client, url) for url in urls_to_process]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                processed_results = []
                for url, result in zip(urls_to_process, results):
                    if isinstance(result, Exception):
                        logger.error(f"[Extractor] Failed to process {url}: {str(result)}")
                        minimal_profile = self._create_minimal_profile(url)
                        processed_results.append(minimal_profile)
                    elif self._validate_profile(result):
                        self._result_cache[self._generate_cache_key(url)] = result
                        processed_results.append(result)
                    else:
                        minimal_profile = self._create_minimal_profile(url)
                        processed_results.append(minimal_profile)
        else:
            processed_results = []
        
        all_results = cached_results + processed_results
        
        elapsed = time.time() - start_time
        logger.info(f"[Extractor] Completed extraction of {len(all_results)} profiles in {elapsed:.2f}s")
        
        return all_results

    async def _process_url_with_rate_limit(self, client: httpx.AsyncClient, url: str) -> Dict:
        async with self.semaphore:
            return await self._process_url(client, url)


    async def _process_url(self, client: httpx.AsyncClient, url: str) -> Dict:
        try:
            start_time = time.time()
            logger.debug(f"[Extractor] Processing {url}")
            
            response = await client.get(url)
            if response.status_code != 200:
                logger.warning(f"[Extractor] Failed to fetch {url} - status code: {response.status_code}")
                return self._create_minimal_profile(url)
            
            html_content = response.text
            
            with ThreadPoolExecutor() as executor:
                soup_future = executor.submit(BeautifulSoup, html_content, 'html.parser')
                soup = soup_future.result()
            
            text = soup.get_text(" ", strip=True)
            
            business_name = self._extract_business_name(soup, url)
            email = self._extract_email(soup, text)
            address = self._extract_address(text)
            phone = self._extract_phone(soup, text)
            
            profile = {
                "business_name": business_name,
                "website": url,
                "email": email,
                "hq_address": address,
                "phone": phone,
                "licensing_text": self._extract_license_text(text),
                "bond_amount": self._parse_bond(text),
                "projects": self._extract_projects(text),
                "union_status": self._detect_union_status(text),
                "evidence_text": self._extract_evidence(soup, text),
                "last_checked": datetime.utcnow().isoformat()
            }
            
            if address:
                self._extract_location_from_address(profile, address)
            
            elapsed = time.time() - start_time
            logger.debug(f"[Extractor] Processed {url} in {elapsed:.2f}s")
            
            return profile
            
        except Exception as e:
            logger.error(f"Error processing {url}: {str(e)}", exc_info=True)
            return self._create_minimal_profile(url)

    def _extract_business_name(self, soup: BeautifulSoup, url: str) -> str:
        """
        Extract business name with multiple fallback strategies.
        
        Args:
            soup: BeautifulSoup object
            url: URL of the website
            
        Returns:
            Extracted business name or fallback
        """
        for selector in self._BUSINESS_NAME_SELECTORS:
            element = soup.select_one(selector)
            if not element:
                continue
                
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
        
        for logo_sel in self._LOGO_SELECTORS:
            logo = soup.select_one(logo_sel)
            if not logo:
                continue
                
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
        except Exception:
            return "Unknown Business"

    def _extract_address(self, text: str) -> Optional[str]:
        match = self._PATTERNS["address"].search(text)
        return match.group(0) if match else None
    
    def _extract_email(self, soup: BeautifulSoup, text: str) -> Optional[str]:
        for selector in self._EMAIL_SELECTORS:
            element = soup.select_one(selector)
            if not element:
                continue
                
            href = element.get('href', '')
            if href.startswith('mailto:'):
                email = href.replace('mailto:', '').strip()
                if email:
                    return email
                    
            email_text = element.get_text(strip=True)
            if email_text:
                return email_text
        
        match = self._PATTERNS["email"].search(text)
        return match.group(0) if match else None
                
    def _extract_license_text(self, text: str) -> Optional[str]:
        match = self._license_text_pattern.search(text)
        return match.group(0) if match else None

    def _parse_bond(self, text: str) -> Optional[int]:
        match = self._PATTERNS["bond"].search(text)
        if not match:
            return None

        amount = float(match.group(1).replace(',', ''))
        multiplier = self._BOND_MULTIPLIERS.get(match.group(2), 1)
        return int(amount * multiplier)

    def _detect_union_status(self, text: str) -> Optional[str]:
        text_lower = text.lower()
        if 'non-union' in text_lower:
            return "Non-Union"
        if 'union' in text_lower:
            return "Union"
        return None

    def _extract_projects(self, text: str) -> List[str]:
        sentences = re.split(r'(?<=[.!?])\s+', text)
        
        project_snippets = []
        count = 0
        
        for s in sentences[:100]: 
            s_stripped = s.strip()
            if 20 < len(s_stripped) < 500:
                keyword_count = sum(1 for kw in self._PROJECT_KEYWORDS if kw in s_stripped.lower())
                if keyword_count >= 2:
                    project_snippets.append(s_stripped)
                    count += 1
                    if count >= 5:
                        break
        
        return project_snippets

    def _extract_evidence(self, soup: BeautifulSoup, text: str) -> str:
        sections = []
        for selector in self._EVIDENCE_SELECTORS:
            section = soup.select_one(selector)
            if section:
                sections.append(section.get_text(" ", strip=True)[:300])
        
        if sections:
            return " [...] ".join(sections)
        else:
            return text[:500]  

    def _validate_profile(self, profile: Dict) -> bool:
        if not profile:
            logger.warning("[Extractor] Empty profile received for validation")
            return False
            
        valid = bool(profile.get("business_name")) and bool(profile.get("website"))
        
        if not valid:
            logger.warning("[Extractor] Profile validation failed: missing business_name or website")
        
        return valid
        
    def _extract_phone(self, soup: BeautifulSoup, text: str) -> Optional[str]:
        for selector in self._PHONE_SELECTORS:
            element = soup.select_one(selector)
            if not element:
                continue
                
            href = element.get('href', '')
            if href.startswith('tel:'):
                phone = href.replace('tel:', '').strip()
                if phone:
                    return self._normalize_phone(phone)
                    
            phone_text = element.get_text(strip=True)
            if phone_text:
                return self._normalize_phone(phone_text)
        
        match = self._PATTERNS["phone"].search(text)
        if match:
            return self._normalize_phone(match.group(0))
            
        return None
        
    @staticmethod
    def _normalize_phone(phone: str) -> str:
        digits = re.sub(r'\D', '', phone)
        
        if len(digits) == 10:
            return f"({digits[0:3]}) {digits[3:6]}-{digits[6:10]}"
        elif len(digits) == 11 and digits[0] == '1':
            return f"({digits[1:4]}) {digits[4:7]}-{digits[7:11]}"
        
        return phone

    def _create_minimal_profile(self, url: str) -> Dict[str, Any]:
        return {
            "business_name": self._extract_domain_name(url),
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
        
    @staticmethod
    @lru_cache(maxsize=128)
    def _extract_domain_name(url: str) -> str:
        try:
            domain = urlparse(url).netloc
            name = re.sub(r'^www\.', '', domain)
            name = re.sub(r'\.(com|org|net|io|co|us|gov)$', '', name)
            name = name.replace('-', ' ').replace('_', ' ')
            return name.title()
        except Exception:
            return "Unknown Business"
    
    @staticmethod
    def _extract_location_from_address(profile: Dict[str, Any], address: str) -> None:
        address_parts = address.split(",")
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
    
    @staticmethod
    def _generate_cache_key(url: str) -> str:
        normalized_url = url.rstrip('/')
        if '://' not in normalized_url:
            normalized_url = 'http://' + normalized_url
        return normalized_url
        
    @classmethod
    def clear_cache(cls) -> None:
        cls._result_cache.clear()
        
    @classmethod
    def get_cache_stats(cls) -> Dict[str, int]:
        return {
            "size": len(cls._result_cache),
            "max_size": cls._result_cache.maxsize
        }