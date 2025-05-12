import asyncio
import os
import re
import json
import httpx
import logging
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)


class SubcontractorExtractor:
    def __init__(self):
        # Only try to load transformers if needed
        self._nlp_initialized = False
        self._classifier = None
        self._tokenizer = None

        # Field extraction patterns
        self.patterns = {
            "email": r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b",
            "phone": r"(?:\+?\d{1,2}\s?)?(?:\(?\d{3}\)?[\s.-]?)?\d{3}[\s.-]?\d{4}\b",
            "license": r"(?:lic(?:ense)?|reg(?:istration)?)[#:]?\s*([A-Z0-9-]{8,15})",
            "bond": r"\$?(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s?(million|M|thousand|K)?\s+bond",
            "address": r"\d+\s+[\w\s]+\s(?:Ave|St|Rd|Blvd|Dr)[\w\s,]+[A-Z]{2}\s\d{5}"
        }

    def _init_nlp(self):
        """Lazy initialize NLP components"""
        if self._nlp_initialized:
            return

        try:
            # Try to import and initialize transformers
            from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification

            self._tokenizer = AutoTokenizer.from_pretrained("bert-base-uncased")
            self._classifier = pipeline(
                "text-classification",
                model="distilbert-base-uncased",
                tokenizer=self._tokenizer
            )
            self._nlp_initialized = True
            logger.info("NLP models initialized successfully")
        except Exception as e:
            logger.warning(f"Could not initialize NLP models: {str(e)}")
            # We'll use fallback methods if NLP isn't available
            self._nlp_initialized = False

    async def extract_profiles(self, urls: List[str]) -> List[Dict]:
        """Orchestrate the extraction pipeline"""
        if not urls:
            logger.warning("No URLs provided for extraction")
            return []

        # Ensure we only have valid strings as URLs
        valid_urls = [str(url) for url in urls if url]

        if not valid_urls:
            logger.warning("No valid URLs after filtering")
            return []

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                tasks = [self._extract_single(client, url) for url in valid_urls]
                return await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"Error in batch extraction: {str(e)}")
            return []

    async def _extract_single(self, client: httpx.AsyncClient, url: str) -> Dict:
        """Process a single subcontractor website"""
        try:
            # Ensure URL is valid
            if not url or not isinstance(url, str) or not url.startswith('http'):
                logger.warning(f"Invalid URL: {url}")
                return self._create_minimal_profile(url)

            response = await client.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')
            text = soup.get_text(" ", strip=True) if soup else ""

            # Base profile with direct scraping
            profile = {
                "name": self._extract_name(soup),
                "website": url,
                "email": self._extract_field(text, "email"),
                "phone_number": self._extract_field(text, "phone"),
                "lic_number": self._extract_field(text, "license"),
                "bond_amount": self._parse_bond(text),
                "evidence_url": url,
                "evidence_text": text[:1000] if text else "",
                "last_checked": datetime.utcnow().isoformat(),
                "score": 0
            }

            # Enhanced extraction with NLP if available
            address = self._extract_address(text)
            if address:
                address_data = self._parse_address(address)
                profile.update(address_data)

            # Extract projects (using simpler method if NLP not available)
            profile["projects"] = self._extract_projects(text)

            # Add legacy fields for compatibility
            self._add_compatibility_fields(profile)

            return profile

        except Exception as e:
            logger.error(f"Error processing {url}: {str(e)}")
            return self._create_minimal_profile(url)

    def _add_compatibility_fields(self, profile: Dict) -> None:
        """Add legacy fields for compatibility with existing code"""
        # Add license field to maintain compatibility
        if profile.get("lic_number") and not profile.get("license"):
            profile["license"] = profile["lic_number"]

        # Add source_url field
        if profile.get("website") and not profile.get("source_url"):
            profile["source_url"] = profile["website"]

        # Add raw_text field if not present
        if profile.get("evidence_text") and not profile.get("raw_text"):
            profile["raw_text"] = profile["evidence_text"]

        # Phone number
        if profile.get("phone_number") and not profile.get("phone"):
            profile["phone"] = profile["phone_number"]

    def _extract_name(self, soup) -> str:
        """Extract company name from meta tags or title"""
        if not soup:
            return "Unknown"

        try:
            # Try meta tags first
            for meta in soup.find_all("meta"):
                if meta.get("property") in ["og:site_name", "og:title"]:
                    content = meta.get("content")
                    if content:
                        return str(content)

            # Fallback to title
            if soup.title and soup.title.string:
                return str(soup.title.string).strip()

            # Final fallback to URL domain
            if getattr(soup, 'url', None):
                return urlparse(soup.url).netloc
        except Exception as e:
            logger.error(f"Error extracting name: {str(e)}")

        return "Unknown"

    def _extract_field(self, text: str, field: str) -> Optional[str]:
        """Generic field extraction with regex"""
        if not text or not isinstance(text, str) or field not in self.patterns:
            return None

        try:
            match = re.search(self.patterns[field], text, re.IGNORECASE)
            return match.group(0) if match else None
        except Exception as e:
            logger.error(f"Error extracting field {field}: {str(e)}")
            return None

    def _parse_bond(self, text: str) -> Optional[int]:
        """Convert bond text to numeric value"""
        if not text or not isinstance(text, str):
            return None

        try:
            match = re.search(self.patterns["bond"], text, re.IGNORECASE)
            if not match:
                return None

            amount_str = match.group(1).replace(",", "") if match.group(1) else "0"

            try:
                amount = float(amount_str)
            except ValueError:
                return None

            multiplier = {
                "million": 1000000,
                "M": 1000000,
                "thousand": 1000,
                "K": 1000
            }.get(match.group(2), 1) if match.group(2) else 1

            return int(amount * multiplier)
        except Exception as e:
            logger.error(f"Error parsing bond amount: {str(e)}")
            return None

    def _extract_address(self, text: str) -> Optional[str]:
        """Find physical address in text"""
        if not text or not isinstance(text, str):
            return None

        try:
            matches = re.finditer(self.patterns["address"], text)
            for match in matches:
                if any(kw in match.group(0).lower() for kw in ["ave", "st", "road"]):
                    return match.group(0)
        except Exception as e:
            logger.error(f"Error extracting address: {str(e)}")
        return None

    def _parse_address(self, address: str) -> Dict:
        """Split address into components"""
        try:
            parts = [p.strip() for p in address.split(",")]
            return {
                "address": parts[0] if parts else "",
                "city": parts[-2] if len(parts) > 2 else "",
                "state": parts[-1][:2].strip() if len(parts) > 1 else ""
            }
        except Exception as e:
            logger.error(f"Error parsing address: {str(e)}")
            return {"address": "", "city": "", "state": ""}

    def _extract_projects(self, text: str) -> List[str]:
        """Identify project examples using NLP or fallback to keyword matching"""
        if not text or not isinstance(text, str):
            return []

        try:
            # Only initialize NLP models when needed
            if not self._nlp_initialized:
                self._init_nlp()

            # If NLP is available, use it
            if self._nlp_initialized and self._classifier:
                return self._extract_projects_with_nlp(text)
            else:
                # Fallback to keyword matching
                return self._extract_projects_with_keywords(text)
        except Exception as e:
            logger.error(f"Error extracting projects: {str(e)}")
            return []

    def _extract_projects_with_nlp(self, text: str) -> List[str]:
        """Extract projects using NLP classification"""
        sentences = [s.strip() for s in re.split(r'[.!?]', text) if s.strip()]

        # Classify sentences as project-related
        project_sentences = []
        for sentence in sentences[:50]:  # Limit to first 50 sentences
            try:
                result = self._classifier(sentence)
                if result[0]["label"] == "PROJECT" and result[0]["score"] > 0.7:
                    project_sentences.append(sentence)
            except Exception:
                continue

        return project_sentences[:3]  # Return top 3

    def _extract_projects_with_keywords(self, text: str) -> List[str]:
        """Extract projects using simple keyword matching"""
        project_keywords = [
            "project", "portfolio", "completed", "construction",
            "built", "developed", "work", "client"
        ]

        sentences = [s.strip() for s in re.split(r'[.!?]', text) if s.strip()]
        project_sentences = []

        for sentence in sentences[:50]:
            lower_sentence = sentence.lower()
            if any(keyword in lower_sentence for keyword in project_keywords):
                if 20 < len(sentence) < 200:  # Reasonable length
                    project_sentences.append(sentence)

        return project_sentences[:3]  # Return top 3

    def _create_minimal_profile(self, url: str) -> Dict:
        """Fallback profile when extraction fails"""
        domain = urlparse(url).netloc if url else "unknown"
        return {
            "name": domain,
            "website": url,
            "source_url": url,
            "evidence_url": url,
            "license": "",
            "lic_number": "",
            "phone": "",
            "phone_number": "",
            "address": "",
            "bond_amount": 0,
            "city": "",
            "state": "",
            "lic_active": False,
            "projects": [],
            "evidence_text": "",
            "raw_text": "",
            "score": 0,
            "last_checked": datetime.utcnow().isoformat()
        }