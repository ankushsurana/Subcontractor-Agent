from pydantic import BaseModel
from typing import List, Optional

class DiscoveryRequest(BaseModel):
    q: str

class ExtractRequest(BaseModel):
    url: str

class LicenseRequest(BaseModel):
    url: str

class HistoryRequest(BaseModel):
    url: str

class ScoreRequest(BaseModel):
    url: str

class Project(BaseModel):
    url: str
    license: str | None = None
    score: int | None = None

class ResearchResult(BaseModel):
    name: str
    website: str
    city: str
    state: str
    lic_active: bool
    lic_number: str
    bond_amount: int
    tx_projects_past_5yrs: int
    score: int
    evidence_url: str
    evidence_text: str
    last_checked: str
