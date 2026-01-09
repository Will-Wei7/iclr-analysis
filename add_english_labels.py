"""
Add English speaker labels to author profiles based on education and email domain data
"""

import pandas as pd
import re
import math
from pathlib import Path
from config import (
    AUTHOR_PROFILES_FILE, 
    AUTHOR_PROFILES_WITH_LANGUAGE_FILE,
    UNIVERSITIES_FILE,
    TOEFL_FILE
)
from country_matching import (
    load_universities_data,
    load_toefl_requirements,
    filter_email_domains,
    extract_institutions_from_education,
    find_country_codes,
    infer_country_from_tld
)


def extract_email_domains(email_string: str) -> str:
    """Extract domains from email string."""
    if pd.isna(email_string) or email_string == '':
        return ''
    
    email_pattern = r'@([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})'
    domains = re.findall(email_pattern, email_string)
    
    unique_domains = list(dict.fromkeys(domains))
    return '; '.join(unique_domains)


def clean_label(val):
    """Clean and validate English speaker label."""
    if pd.isna(val):
        return None
    try:
        num_val = float(val)
        if math.isnan(num_val):
            return None
        int_val = int(num_val)
        if int_val in [0, 1, -1]:
            return int_val
        return None
    except (ValueError, TypeError):
        return None


def process_author_profiles():
    """Process author profiles and add English speaker labels"""
    print("="*60)
    print("Adding English Speaker Labels to Author Profiles")
    print("="*60)
    
    if not AUTHOR_PROFILES_FILE.exists():
        print(f"Error: Input file not found: {AUTHOR_PROFILES_FILE}")
        print("Please run fetch_profiles.py first")
        return None
    
    print(f"\n1. Loading author profiles from {AUTHOR_PROFILES_FILE}...")
    df = pd.read_csv(AUTHOR_PROFILES_FILE)
    print(f"   Loaded {len(df)} author profiles")
    
    # Check if authors already have english_speaker labels
    has_labels = 'english_speaker' in df.columns and df['english_speaker'].notna().any()
    if has_labels:
        print(f"   Found {df['english_speaker'].notna().sum()} authors with existing labels")
        print(f"   Will preserve existing labels and only process authors without labels")
    
    # Load supporting data
    print(f"\n2. Loading supporting data...")
    print(f"   Loading universities data from {UNIVERSITIES_FILE}...")
    university_data = load_universities_data(str(UNIVERSITIES_FILE))
    
    print(f"   Loading TOEFL requirements from {TOEFL_FILE}...")
    toefl_requirements = load_toefl_requirements(str(TOEFL_FILE))
    print(f"   Loaded TOEFL requirements for {len(toefl_requirements)} countries")
    
    # Extract email domains
    print(f"\n3. Extracting email domains...")
    df['email_primary_domains'] = df['email_primary'].apply(extract_email_domains)
    df['all_emails_domains'] = df['all_emails'].apply(extract_email_domains)
    
    # Filter email domains
    print(f"\n4. Filtering email domains (removing common providers)...")
    df['filtered_email_domains'] = df['all_emails_domains'].apply(
        lambda x: '; '.join(filter_email_domains(x)) if pd.notna(x) and x else ''
    )
    
    # Extract institutions from education background
    print(f"\n5. Extracting institutions from education background...")
    df['institutions_from_education'] = df['education_background'].apply(
        lambda x: '; '.join(extract_institutions_from_education(x)) if pd.notna(x) and x else ''
    )
    
    # Find country codes and determine English speaker status
    print(f"\n6. Matching domains and institutions to countries...")
    print(f"   Determining English speaker status...")
    
    # Clean existing labels if present
    if 'english_speaker' in df.columns:
        df['english_speaker'] = df['english_speaker'].apply(clean_label)
    
    authors_with_labels = set()
    if 'english_speaker' in df.columns:
        valid_mask = df['english_speaker'].notna() & (df['english_speaker'].isin([0, 1]))
        authors_with_labels = set(df[valid_mask]['author_name'].values)
        if len(authors_with_labels) > 0:
            print(f"   Found {len(authors_with_labels)} authors with existing valid labels (0 or 1)")
    
    education_countries_list = []
    english_speaker_list = []
    
    for idx, row in df.iterrows():
        if (idx + 1) % 1000 == 0:
            print(f"   Processed {idx + 1}/{len(df)} authors...")
        
        author_name = row.get('author_name', '')
        
        # Preserve existing labels if present
        if author_name in authors_with_labels:
            existing_countries = row.get('education_countries', '')
            has_country_data = False
            if pd.notna(existing_countries):
                countries_str = str(existing_countries).strip()
                if countries_str and countries_str not in ['nan', 'None', ''] and not countries_str.startswith('{'):
                    has_country_data = True
            
            existing_label = row.get('english_speaker', -1)
            try:
                if pd.notna(existing_label):
                    existing_label = int(float(existing_label))
                    if existing_label not in [0, 1, -1]:
                        existing_label = -1
                else:
                    existing_label = -1
            except (ValueError, TypeError):
                existing_label = -1
            
            if not has_country_data:
                existing_label = -1
                existing_countries = None
            
            education_countries_list.append(existing_countries if has_country_data else None)
            english_speaker_list.append(existing_label)
            continue
        
        # Process authors without labels
        institutions_str = row.get('institutions_from_education', '')
        institutions = [i.strip() for i in institutions_str.split(';')] if institutions_str else []
        
        domains_str = row.get('filtered_email_domains', '')
        email_domains = [d.strip() for d in domains_str.split(';')] if domains_str else []
        
        # Find country codes
        country_codes = find_country_codes(university_data, institutions, email_domains)
        
        # Try TLD inference if no countries found
        if not country_codes and email_domains:
            for domain in email_domains:
                tld_country = infer_country_from_tld(domain)
                if tld_country:
                    country_codes.add(tld_country)
        
        # Use current_country as fallback
        current_country_raw = row.get('current_country', '')
        current_country = None
        has_valid_current_country = False
        if pd.notna(current_country_raw):
            current_country_str = str(current_country_raw).strip()
            if current_country_str and current_country_str.lower() not in ['nan', 'none', '']:
                current_country = current_country_str
                has_valid_current_country = True
        
        countries_from_processing = len(country_codes) > 0
        
        if not country_codes and current_country:
            country_codes.add(current_country)
        elif current_country and current_country not in country_codes:
            country_codes.add(current_country)
        
        # If no country data, set to -1
        if not countries_from_processing and not has_valid_current_country:
            education_countries_list.append(None)
            english_speaker_list.append(-1)
            continue
        
        # Store education countries
        if country_codes:
            education_countries_list.append('; '.join(sorted(country_codes)))
        else:
            education_countries_list.append(None)
        
        # Determine English speaker flag
        if not country_codes:
            english_flag = -1
        else:
            english_flag = 0
            for code in country_codes:
                country_name = university_data['code_to_country_name'].get(code, '')
                if country_name and toefl_requirements.get(country_name.lower(), '') == 'Exempt':
                    english_flag = 1
                    break
        
        english_speaker_list.append(english_flag)
    
    # Update columns
    df['education_countries'] = education_countries_list
    df['english_speaker'] = english_speaker_list
    
    # Final validation
    print(f"\n7. Final validation: Ensuring consistency...")
    no_country_mask = df['education_countries'].isna() | (df['education_countries'] == '') | (df['education_countries'].astype(str).str.strip() == '')
    invalid_labels = (no_country_mask & (df['english_speaker'] != -1)).sum()
    if invalid_labels > 0:
        print(f"   Found {invalid_labels} authors with no country data but invalid labels, fixing...")
        df.loc[no_country_mask, 'english_speaker'] = -1
        df.loc[no_country_mask, 'education_countries'] = None
    
    # Print statistics
    print(f"\n" + "="*60)
    print("Processing Complete!")
    print("="*60)
    
    total = len(df)
    with_countries = (df['education_countries'].notna() & (df['education_countries'] != '')).sum()
    english_speakers = (df['english_speaker'] == 1).sum()
    non_english_speakers = (df['english_speaker'] == 0).sum()
    unknown_speakers = (df['english_speaker'] == -1).sum()
    
    print(f"\nStatistics:")
    print(f"  Total authors: {total}")
    print(f"  Authors with country data: {with_countries} ({with_countries/total*100:.1f}%)")
    print(f"  English speakers (1): {english_speakers} ({english_speakers/total*100:.1f}%)")
    print(f"  Non-English speakers (0): {non_english_speakers} ({non_english_speakers/total*100:.1f}%)")
    print(f"  Unknown (-1, no country data): {unknown_speakers} ({unknown_speakers/total*100:.1f}%)")
    
    # Save results
    print(f"\n8. Saving results to {AUTHOR_PROFILES_WITH_LANGUAGE_FILE}...")
    df.to_csv(AUTHOR_PROFILES_WITH_LANGUAGE_FILE, index=False)
    print(f"   Saved successfully!")
    
    return df


if __name__ == "__main__":
    process_author_profiles()

