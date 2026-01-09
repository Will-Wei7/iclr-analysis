"""
Extract unique first authors from ICLR papers (2018-2025)
"""

import pandas as pd
from pathlib import Path
from config import ICLR_DATA_FILES, UNIQUE_AUTHORS_FILE, YEARS


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


def extract_unique_authors() -> pd.DataFrame:
    """
    Extract unique first authors from ICLR papers (2018-2025)
    
    Returns:
        DataFrame with unique author names
    """
    print("="*60)
    print("Extracting unique first authors from ICLR papers (2018-2025)")
    print("="*60)
    
    all_authors = []
    
    for year in YEARS:
        data_file = ICLR_DATA_FILES.get(year)
        if not data_file or not data_file.exists():
            print(f"Warning: File not found for year {year}: {data_file}")
            continue
        
        print(f"\nProcessing year {year}...")
        
        # Handle parquet files vs CSV files
        if data_file.suffix == '.parquet':
            df = pd.read_parquet(data_file)
        else:
            df = pd.read_csv(data_file)
        
        print(f"  Loaded {len(df)} papers")
        
        # Extract first authors
        if 'authors' in df.columns:
            df['first_author'] = df['authors'].apply(extract_first_author)
        elif 'first_author' in df.columns:
            pass  # Already has first_author column
        else:
            print(f"  Warning: No 'authors' column found for year {year}")
            continue
        
        # Filter out empty names
        first_authors = df[df['first_author'] != '']['first_author'].unique()
        print(f"  Found {len(first_authors)} unique first authors")
        
        # Add to list with year info
        for author in first_authors:
            all_authors.append({
                'author_name': author,
                'year': year
            })
    
    if not all_authors:
        print("\nERROR: No authors found. Please check that the data files exist.")
        return pd.DataFrame(columns=['author_name'])
    
    authors_df = pd.DataFrame(all_authors)
    
    # Get unique authors (keep first occurrence)
    unique_authors_df = authors_df.drop_duplicates(subset=['author_name'], keep='first')
    
    print(f"\n" + "="*60)
    print(f"Summary:")
    print(f"  Total first author entries: {len(authors_df)}")
    print(f"  Unique first authors: {len(unique_authors_df)}")
    print("="*60)
    
    # Save to CSV
    if len(unique_authors_df) > 0:
        unique_authors_df = unique_authors_df[['author_name']]
        unique_authors_df.to_csv(UNIQUE_AUTHORS_FILE, index=False)
        print(f"\nSaved unique authors to: {UNIQUE_AUTHORS_FILE}")
    else:
        print("\nWARNING: No unique authors to save!")
    
    return unique_authors_df


if __name__ == "__main__":
    extract_unique_authors()

