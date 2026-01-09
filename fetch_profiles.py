"""
Fetch author profiles from OpenReview API for all unique first authors (2018-2025)
"""

import pandas as pd
import openreview
import time
import json
from tqdm import tqdm
from pathlib import Path
from config import UNIQUE_AUTHORS_FILE, AUTHOR_PROFILES_FILE, OUTPUT_DIR, API_BATCH_SIZE, API_SAVE_INTERVAL, API_RATE_LIMIT_DELAY


def _initialize_client():
    """Initialize OpenReview client with fallback options."""
    try:
        client = openreview.Client(baseurl='https://api.openreview.net')
        print("OpenReview client initialized")
        return client
    except Exception:
        try:
            client = openreview.api.OpenReviewClient(baseurl='https://api2.openreview.net')
            print("OpenReview API client initialized")
            return client
        except Exception as e:
            print(f"Both clients failed: {e}")
            return None


def _search_profiles(client, author_name):
    """Search for author profiles using different API methods."""
    name_parts = author_name.split()
    first_name = name_parts[0] if name_parts else author_name
    last_name = name_parts[-1] if len(name_parts) >= 2 else ""
    
    try:
        if hasattr(client, 'search_profiles'):
            return client.search_profiles(first=first_name, last=last_name)
        elif hasattr(client, 'get_profiles'):
            return client.get_profiles([f"{first_name} {last_name}".strip()])
        return []
    except:
        return []


def _get_current_position(history):
    """Extract current position information from history."""
    for entry in history:
        if entry.get('end') is None or entry.get('end') == 'Present':
            position = entry.get('position', '')
            institution_info = entry.get('institution', {})
            institution = institution_info.get('name', '')
            country = institution_info.get('country', '')
            return position, institution, country
    return '', '', ''


def _build_education_background(history):
    """Build education background from all history entries."""
    education = []
    for entry in history:
        position = entry.get('position', '')
        if position:
            institution_info = entry.get('institution', {})
            year = entry.get('end', entry.get('start', ''))
            
            education.append({
                'position': position,
                'institution': institution_info.get('name', ''),
                'year': year if year else None
            })
    return education


def _extract_profile_data(profile, author_name):
    """Extract relevant data from a profile object."""
    profile_name = (profile.get_preferred_name(pretty=True) 
                   if hasattr(profile, 'get_preferred_name') else author_name)
    
    emails = profile.content.get('emailsConfirmed', []) or profile.content.get('emails', [])
    
    history = profile.content.get('history', [])
    current_position, current_institution, current_country = _get_current_position(history)
    
    education = _build_education_background(history)
    
    return {
        'author_name': author_name,
        'profile_name': profile_name,
        'profile_id': getattr(profile, 'id', ''),
        'email_primary': emails[0] if emails else '',
        'all_emails': '; '.join(emails) if emails else '',
        'current_position': current_position,
        'current_institution': current_institution,
        'current_country': current_country,
        'education_background': json.dumps(education) if education else '',
        'total_positions': len(history)
    }


def _create_empty_profile(author_name):
    """Create an empty profile record for authors without profiles."""
    return {
        'author_name': author_name,
        'profile_name': author_name,
        'profile_id': '',
        'email_primary': '',
        'all_emails': '',
        'current_position': '',
        'current_institution': '',
        'current_country': '',
        'education_background': '',
        'total_positions': 0
    }


def _process_author(client, author_name):
    """Process a single author and extract profile information."""
    try:
        profiles = _search_profiles(client, author_name)
        
        if profiles:
            return _extract_profile_data(profiles[0], author_name)
        else:
            return _create_empty_profile(author_name)
            
    except Exception as e:
        print(f"Error with {author_name}: {e}")
        return _create_empty_profile(author_name)


def fetch_author_profiles(unique_authors_df=None, batch_size=None, save_interval=None):
    """
    Fetch author profiles using OpenReview API.
    
    Args:
        unique_authors_df: DataFrame with unique authors (if None, loads from file)
        batch_size: Number of authors to process in each batch
        save_interval: Save intermediate results every N batches
    """
    if unique_authors_df is None:
        if not UNIQUE_AUTHORS_FILE.exists():
            print(f"Error: Input file not found: {UNIQUE_AUTHORS_FILE}")
            print("Please run extract_unique_authors.py first")
            return None
        unique_authors_df = pd.read_csv(UNIQUE_AUTHORS_FILE)
    
    if batch_size is None:
        batch_size = API_BATCH_SIZE
    if save_interval is None:
        save_interval = API_SAVE_INTERVAL
    
    print(f"\nFetching author profiles for {len(unique_authors_df):,} authors...")
    print("This may take several hours depending on API response times.")
    
    client = _initialize_client()
    if not client:
        print("Failed to initialize OpenReview client")
        return None
    
    enhanced_data = []
    total_batches = (len(unique_authors_df) + batch_size - 1) // batch_size
    
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min((batch_num + 1) * batch_size, len(unique_authors_df))
        batch_authors = unique_authors_df.iloc[start_idx:end_idx]
        
        print(f"\nProcessing batch {batch_num + 1}/{total_batches} (authors {start_idx + 1}-{end_idx})")
        
        for idx, row in tqdm(batch_authors.iterrows(), total=len(batch_authors), desc=f"Batch {batch_num + 1}"):
            author_name = row['author_name']
            profile_data = _process_author(client, author_name)
            enhanced_data.append(profile_data)
            time.sleep(API_RATE_LIMIT_DELAY)
        
        # Save intermediate results
        if (batch_num + 1) % save_interval == 0:
            temp_df = pd.DataFrame(enhanced_data)
            temp_file = OUTPUT_DIR / f'author_profiles_temp_batch_{batch_num + 1}.csv'
            temp_df.to_csv(temp_file, index=False)
            print(f"Intermediate save: {temp_file} ({len(temp_df)} authors processed)")
    
    enhanced_df = pd.DataFrame(enhanced_data)
    
    print(f"\n" + "="*60)
    print("Profile extraction completed:")
    print(f"  Authors processed: {len(enhanced_df)}")
    print(f"  With emails: {len(enhanced_df[enhanced_df['email_primary'] != ''])}")
    print(f"  With positions: {len(enhanced_df[enhanced_df['current_position'] != ''])}")
    print(f"  With education: {len(enhanced_df[enhanced_df['education_background'] != ''])}")
    print("="*60)
    
    return enhanced_df


def main():
    """Main function"""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    if not UNIQUE_AUTHORS_FILE.exists():
        print(f"Error: Input file not found: {UNIQUE_AUTHORS_FILE}")
        print("Please run extract_unique_authors.py first")
        return
    
    print(f"Loading unique authors from {UNIQUE_AUTHORS_FILE}...")
    unique_authors_df = pd.read_csv(UNIQUE_AUTHORS_FILE)
    print(f"Loaded {len(unique_authors_df):,} unique authors")
    
    enhanced_df = fetch_author_profiles(unique_authors_df)
    
    if enhanced_df is not None:
        enhanced_df.to_csv(AUTHOR_PROFILES_FILE, index=False)
        print(f"\nSaved author profiles to: {AUTHOR_PROFILES_FILE}")
        
        total_authors = len(enhanced_df)
        email_count = len(enhanced_df[enhanced_df['email_primary'] != ''])
        position_count = len(enhanced_df[enhanced_df['current_position'] != ''])
        education_count = len(enhanced_df[enhanced_df['education_background'] != ''])
        
        print(f"\nSuccess Rates:")
        print(f"  Email found: {email_count}/{total_authors} ({email_count/total_authors*100:.1f}%)")
        print(f"  Position found: {position_count}/{total_authors} ({position_count/total_authors*100:.1f}%)")
        print(f"  Education found: {education_count}/{total_authors} ({education_count/total_authors*100:.1f}%)")


if __name__ == "__main__":
    main()

