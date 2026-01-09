"""
Tokenize abstracts following Liang et al. method
Creates tokenized parquet files by year and English/non-English speaker groups
Following the approach from: Quantifying large language model usage in scientific papers
"""

import pandas as pd
import spacy
from tqdm import tqdm
from typing import List
from pathlib import Path
from config import OUTPUT_DIR, YEARS

# Tokenized output directory
TOKENIZED_DIR = OUTPUT_DIR / "tokenized_data"
TOKENIZED_DIR.mkdir(parents=True, exist_ok=True)

# Load spaCy model
print("Loading spaCy model...")
try:
    nlp = spacy.load("en_core_web_sm", disable=["ner", "tagger", "attribute_ruler", "lemmatizer"])
    print("spaCy model loaded successfully")
except OSError:
    print("Error: spaCy model 'en_core_web_sm' not found.")
    print("Please install it with: python -m spacy download en_core_web_sm")
    raise


def tokenize_abstract(abstract: str) -> List[List[str]]:
    """
    Tokenize an abstract into sentences, where each sentence is a list of tokens
    Following Liang et al. method: extract sentences and tokenize each one
    """
    if pd.isna(abstract) or not abstract or len(str(abstract).strip()) < 50:
        return []
    
    abstract_str = str(abstract).strip()
    doc = nlp(abstract_str)
    
    sentences = []
    for sent in doc.sents:
        # Extract tokens (excluding spaces)
        tokens = [token.text for token in sent if not token.is_space]
        # Filter out very short sentences (same as original: len(tokens) > 5)
        if len(tokens) > 5:
            sentences.append(tokens)
    
    return sentences


def process_year_tokenization(year: int, language_group: str = None):
    """
    Process tokenization for a specific year and optionally language group
    
    Args:
        year: Year to process
        language_group: 'english', 'non_english', or None for all
    """
    input_file = OUTPUT_DIR / f"iclr_{year}_with_language.csv"
    
    if not input_file.exists():
        print(f"Warning: File not found: {input_file}")
        print("Please run merge_papers_with_language.py first")
        return
    
    print(f"\nProcessing tokenization for year {year}" + 
          (f" ({language_group})" if language_group else ""))
    
    df = pd.read_csv(input_file)
    
    # Filter by language group if specified
    if language_group == 'english':
        df = df[df['english_speaker'] == 1]
        suffix = "_english"
    elif language_group == 'non_english':
        df = df[df['english_speaker'] == 0]
        suffix = "_non_english"
    else:
        suffix = ""
    
    print(f"  Processing {len(df)} papers...")
    
    # Filter out papers with missing abstracts
    df = df.dropna(subset=['abstract'])
    df = df[df['abstract'].str.len() > 50]
    print(f"  After filtering: {len(df)} papers with valid abstracts")
    
    if len(df) == 0:
        print(f"  No valid abstracts found, skipping...")
        return
    
    # Tokenize all abstracts
    all_sentences = []
    paper_ids = []
    first_authors = []
    
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Tokenizing"):
        abstract = row.get('abstract', '')
        sentences = tokenize_abstract(abstract)
        
        for sent in sentences:
            all_sentences.append(sent)
            paper_ids.append(row.get('id', ''))
            first_authors.append(row.get('first_author', ''))
    
    # Create DataFrame following Liang et al. format
    parquet_df = pd.DataFrame({
        'sentence': all_sentences,
        'paper_id': paper_ids,
        'first_author': first_authors
    })
    
    # Save to parquet
    output_file = TOKENIZED_DIR / f"{year}_1{suffix}.parquet"
    parquet_df.to_parquet(output_file, engine='pyarrow')
    
    print(f"  Saved {len(parquet_df)} tokenized sentences to {output_file}")
    if len(df) > 0:
        print(f"  Average sentences per paper: {len(parquet_df) / len(df):.1f}")


def main():
    """Main function to tokenize all years"""
    print("="*60)
    print("Tokenizing ICLR papers following Liang et al. method")
    print("="*60)
    
    # Check if merged files exist
    missing_files = []
    for year in YEARS:
        merged_file = OUTPUT_DIR / f"iclr_{year}_with_language.csv"
        if not merged_file.exists():
            missing_files.append(year)
    
    if missing_files:
        print(f"\nError: Missing merged files for years: {missing_files}")
        print("Please run merge_papers_with_language.py first")
        return
    
    # Process each year
    for year in YEARS:
        # Process all papers
        process_year_tokenization(year, language_group=None)
        
        # Process English speakers
        process_year_tokenization(year, language_group='english')
        
        # Process non-English speakers
        process_year_tokenization(year, language_group='non_english')
    
    print("\n" + "="*60)
    print("Tokenization complete!")
    print("="*60)
    print(f"\nOutput files saved in: {TOKENIZED_DIR}/")
    print("\nFile naming convention:")
    print("  {year}_1.parquet - All papers")
    print("  {year}_1_english.parquet - English speaker first authors")
    print("  {year}_1_non_english.parquet - Non-English speaker first authors")


if __name__ == "__main__":
    main()

