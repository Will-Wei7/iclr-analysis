"""
Merge ICLR paper data with author language labels
Creates files with paper data and English speaker status for each paper's first author
"""

import pandas as pd
from pathlib import Path
from config import (
    ICLR_DATA_FILES,
    AUTHOR_PROFILES_WITH_LANGUAGE_FILE,
    OUTPUT_DIR,
    YEARS
)


def extract_first_author(authors_str: str) -> str:
    """Extract first author name from authors string"""
    if pd.isna(authors_str) or not authors_str:
        return ""
    
    authors_str = str(authors_str).strip()
    if not authors_str:
        return ""
    
    authors = authors_str.split(',')
    if authors:
        return authors[0].strip()
    return ""


def merge_papers_with_language():
    """
    Merge paper data with author language labels
    Creates files: iclr_{year}_with_language.csv for each year
    """
    print("="*60)
    print("Merging ICLR Paper Data with Author Language Labels")
    print("="*60)
    
    # Load author profiles with language labels
    if not AUTHOR_PROFILES_WITH_LANGUAGE_FILE.exists():
        print(f"Error: Author profiles file not found: {AUTHOR_PROFILES_WITH_LANGUAGE_FILE}")
        print("Please run add_english_labels.py first")
        return None
    
    print(f"\n1. Loading author profiles from {AUTHOR_PROFILES_WITH_LANGUAGE_FILE}...")
    author_profiles_df = pd.read_csv(AUTHOR_PROFILES_WITH_LANGUAGE_FILE)
    print(f"   Loaded {len(author_profiles_df)} author profiles")
    
    # Create lookup dictionary: author_name -> english_speaker
    author_language_map = {}
    for _, row in author_profiles_df.iterrows():
        author_name = row.get('author_name', '')
        english_speaker = row.get('english_speaker', -1)
        if author_name:
            author_language_map[author_name] = english_speaker
    
    print(f"   Created language map for {len(author_language_map)} authors")
    
    # Process each year
    all_merged_data = []
    
    for year in YEARS:
        data_file = ICLR_DATA_FILES.get(year)
        if not data_file or not data_file.exists():
            print(f"\nWarning: File not found for year {year}: {data_file}")
            continue
        
        print(f"\n2. Processing year {year}...")
        
        # Load paper data
        if data_file.suffix == '.parquet':
            df = pd.read_parquet(data_file)
        else:
            df = pd.read_csv(data_file)
        
        print(f"   Loaded {len(df)} papers")
        
        # Extract first author if not present
        if 'first_author' not in df.columns:
            if 'authors' in df.columns:
                df['first_author'] = df['authors'].apply(extract_first_author)
            else:
                print(f"   Warning: No 'authors' column found for year {year}")
                continue
        
        # Match with author language labels
        df['english_speaker'] = df['first_author'].apply(
            lambda x: author_language_map.get(x, -1) if x else -1
        )
        
        # Statistics
        total = len(df)
        matched = (df['english_speaker'] != -1).sum()
        english = (df['english_speaker'] == 1).sum()
        non_english = (df['english_speaker'] == 0).sum()
        unknown = (df['english_speaker'] == -1).sum()
        
        print(f"   First authors matched: {matched}/{total} ({matched/total*100:.1f}%)")
        print(f"   English speakers: {english} ({english/total*100:.1f}%)")
        print(f"   Non-English speakers: {non_english} ({non_english/total*100:.1f}%)")
        print(f"   Unknown: {unknown} ({unknown/total*100:.1f}%)")
        
        # Save merged file
        output_file = OUTPUT_DIR / f"iclr_{year}_with_language.csv"
        df.to_csv(output_file, index=False)
        print(f"   Saved to {output_file}")
        
        all_merged_data.append(df)
    
    # Create combined file
    if all_merged_data:
        combined_df = pd.concat(all_merged_data, ignore_index=True)
        combined_file = OUTPUT_DIR / "iclr_2018_2025_with_language.csv"
        combined_df.to_csv(combined_file, index=False)
        print(f"\n3. Saved combined file: {combined_file}")
        print(f"   Total papers: {len(combined_df)}")
    
    print("\n" + "="*60)
    print("Merge complete!")
    print("="*60)
    
    return all_merged_data


if __name__ == "__main__":
    merge_papers_with_language()

