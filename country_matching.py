"""
Utilities for matching education backgrounds and email domains with country codes
using the world_universities_and_domains.json database and TLD inference.
"""

import json
import csv
import re
import math
from typing import List, Set, Dict, Optional

# Common email domains to ignore
COMMON_EMAIL_DOMAINS = {
    'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'icloud.com',
    'aol.com', 'live.com', 'msn.com', 'protonmail.com', 'yandex.com',
    'mail.ru', 'qq.com', '163.com', 'sina.com', 'sohu.com'
}

# Country code mapping from TLD (Top Level Domain)
TLD_TO_COUNTRY = {
    '.cn': 'CN', '.in': 'IN', '.uk': 'GB', '.ca': 'CA', '.au': 'AU',
    '.de': 'DE', '.fr': 'FR', '.jp': 'JP', '.kr': 'KR', '.sg': 'SG',
    '.hk': 'HK', '.tw': 'TW', '.my': 'MY', '.th': 'TH', '.ph': 'PH',
    '.id': 'ID', '.vn': 'VN', '.bd': 'BD', '.pk': 'PK', '.lk': 'LK',
    '.np': 'NP', '.mm': 'MM', '.kh': 'KH', '.la': 'LA', '.bn': 'BN',
    '.mx': 'MX', '.br': 'BR', '.ar': 'AR', '.cl': 'CL', '.co': 'CO',
    '.pe': 'PE', '.ve': 'VE', '.ec': 'EC', '.uy': 'UY', '.py': 'PY',
    '.bo': 'BO', '.za': 'ZA', '.ng': 'NG', '.ke': 'KE', '.eg': 'EG',
    '.ma': 'MA', '.tn': 'TN', '.dz': 'DZ', '.ru': 'RU', '.ua': 'UA',
    '.by': 'BY', '.kz': 'KZ', '.uz': 'UZ', '.kg': 'KG', '.tj': 'TJ',
    '.tm': 'TM', '.af': 'AF', '.ir': 'IR', '.iq': 'IQ', '.sy': 'SY',
    '.lb': 'LB', '.jo': 'JO', '.il': 'IL', '.ps': 'PS', '.sa': 'SA',
    '.ae': 'AE', '.qa': 'QA', '.kw': 'KW', '.bh': 'BH', '.om': 'OM',
    '.ye': 'YE', '.tr': 'TR', '.cy': 'CY', '.gr': 'GR', '.bg': 'BG',
    '.ro': 'RO', '.md': 'MD', '.hu': 'HU', '.sk': 'SK', '.cz': 'CZ',
    '.pl': 'PL', '.lt': 'LT', '.lv': 'LV', '.ee': 'EE', '.fi': 'FI',
    '.se': 'SE', '.no': 'NO', '.dk': 'DK', '.is': 'IS', '.ie': 'IE',
    '.pt': 'PT', '.es': 'ES', '.it': 'IT', '.ch': 'CH', '.at': 'AT',
    '.be': 'BE', '.nl': 'NL', '.lu': 'LU', '.nz': 'NZ',
}


def load_universities_data(file_path: str) -> Dict:
    """Load the world universities data and create lookup dictionaries."""
    print(f"Loading universities data from {file_path}...")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        universities = json.load(f)
    
    domain_to_country = {}
    institution_to_country = {}
    code_to_country_name = {}
    
    for uni in universities:
        country_code = uni.get('alpha_two_code', '')
        country_name = uni.get('country', '')
        institution_name = uni.get('name', '')
        domains = uni.get('domains', [])
        
        for domain in domains:
            domain_to_country[domain.lower()] = country_code
        
        if institution_name:
            normalized_name = normalize_institution_name(institution_name)
            if normalized_name:
                institution_to_country[normalized_name] = country_code
        
        if country_code and country_name and country_code not in code_to_country_name:
            code_to_country_name[country_code] = country_name
    
    print(f"Loaded {len(universities)} universities")
    print(f"Created {len(domain_to_country)} domain mappings")
    print(f"Created {len(institution_to_country)} institution mappings")
    
    return {
        'domain_to_country': domain_to_country,
        'institution_to_country': institution_to_country,
        'universities': universities,
        'code_to_country_name': code_to_country_name
    }


def load_toefl_requirements(csv_path: str) -> Dict[str, str]:
    """Load TOEFL requirements, keyed by lowercase country name."""
    requirements: Dict[str, str] = {}
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            country = (row.get('Country') or '').strip()
            requirement = (row.get('TOEFL requirement') or '').strip()
            if country:
                requirements[country.lower()] = requirement
    return requirements


def normalize_institution_name(name: str) -> str:
    """Normalize institution name for better matching."""
    normalized = name.lower()
    
    words_to_remove = [
        'university', 'college', 'institute', 'school', 'of', 'the', 'and',
        'technology', 'science', 'engineering', 'medical', 'state', 'national',
        'international', 'private', 'public', 'community'
    ]
    
    for word in words_to_remove:
        normalized = normalized.replace(word, '')
    
    normalized = re.sub(r'[^\w\s]', '', normalized)
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    
    return normalized


def extract_institutions_from_education(education_background: str) -> List[str]:
    """Extract institution names from education background JSON string."""
    if education_background is None:
        return []
    
    if isinstance(education_background, float):
        if math.isnan(education_background):
            return []
        education_background = str(education_background)
    
    if not isinstance(education_background, str):
        education_background = str(education_background)
    
    if not education_background or education_background.strip() == '':
        return []
    
    try:
        education_data = json.loads(education_background)
        institutions = []
        
        for entry in education_data:
            institution = entry.get('institution', '')
            if institution:
                institutions.append(institution)
        
        return institutions
    except (json.JSONDecodeError, TypeError, ValueError):
        return []


def filter_email_domains(domains_str: str) -> List[str]:
    """Filter out common email domains and return only institutional domains."""
    if not domains_str or domains_str.strip() == '':
        return []
    
    domains = [d.strip() for d in domains_str.split(';')]
    filtered_domains = []
    
    for domain in domains:
        if domain and domain.lower() not in COMMON_EMAIL_DOMAINS:
            filtered_domains.append(domain.lower())
    
    return filtered_domains


def infer_country_from_tld(domain: str) -> Optional[str]:
    """Infer country code from domain TLD."""
    parts = domain.split('.')
    if len(parts) >= 2:
        tld = '.' + parts[-1]
        return TLD_TO_COUNTRY.get(tld)
    return None


def find_country_codes(university_data: Dict, institutions: List[str], email_domains: List[str]) -> Set[str]:
    """Find country codes for given institutions and email domains."""
    country_codes = set()
    
    # Match by institution names
    for institution in institutions:
        normalized_name = normalize_institution_name(institution)
        
        if not normalized_name:
            continue
        
        if normalized_name in university_data['institution_to_country']:
            country_code = university_data['institution_to_country'][normalized_name]
            country_codes.add(country_code)
            continue
        
        if len(normalized_name) >= 3:
            for uni_name, country_code in university_data['institution_to_country'].items():
                if not uni_name or len(uni_name) < 3:
                    continue
                
                if (normalized_name in uni_name and len(normalized_name) >= 4) or \
                   (uni_name in normalized_name and len(uni_name) >= 4):
                    country_codes.add(country_code)
                    break
    
    # Match by email domains
    for domain in email_domains:
        if domain in university_data['domain_to_country']:
            country_code = university_data['domain_to_country'][domain]
            country_codes.add(country_code)
        else:
            tld_country = infer_country_from_tld(domain)
            if tld_country:
                country_codes.add(tld_country)
    
    return country_codes

