"""
Main pipeline script to run the complete ICLR author profile building process
"""

import sys
from pathlib import Path
from extract_unique_authors import extract_unique_authors
from fetch_profiles import fetch_author_profiles
from add_english_labels import process_author_profiles
from merge_papers_with_language import merge_papers_with_language
from tokenize_data import main as tokenize_all_data
from config import (
    UNIQUE_AUTHORS_FILE, 
    AUTHOR_PROFILES_FILE, 
    AUTHOR_PROFILES_WITH_LANGUAGE_FILE,
    TOKENIZED_DIR
)


def main():
    """Run the complete author profile building pipeline"""
    print("="*70)
    print("ICLR Author Profile Building Pipeline (2018-2025)")
    print("="*70)
    print("NOTE: Assumes paper data files already exist in data/iclr_data/")
    print("      If you need to collect data, run collect_paper_data.py separately.")
    
    # Step 1: Extract unique first authors
    print("\n" + "="*70)
    print("STEP 1: Extracting unique first authors from ICLR papers (2018-2025)")
    print("="*70)
    
    try:
        unique_authors_df = extract_unique_authors()
        if unique_authors_df is None or len(unique_authors_df) == 0:
            print("ERROR: Failed to extract unique authors")
            return
    except Exception as e:
        print(f"ERROR in Step 1: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Step 2: Fetch author profiles from OpenReview API
    print("\n" + "="*70)
    print("STEP 2: Fetching author profiles from OpenReview API")
    print("="*70)
    print("NOTE: This step may take several hours depending on the number of authors")
    print("      and API response times. Intermediate results will be saved periodically.")
    
    user_input = input("\nProceed with API calls? This may take hours. (yes/no): ")
    if user_input.lower() not in ['yes', 'y']:
        print("Skipping API calls. You can run fetch_profiles.py separately later.")
        return
    
    try:
        profiles_df = fetch_author_profiles(unique_authors_df)
        if profiles_df is None:
            print("ERROR: Failed to fetch author profiles")
            return
    except Exception as e:
        print(f"ERROR in Step 2: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Step 3: Add English speaker labels (example analysis)
    print("\n" + "="*70)
    print("STEP 3: Adding English speaker labels (Example Analysis)")
    print("="*70)
    print("NOTE: This is an example analysis. You can skip this and use")
    print("      author_profiles_2018_2025.csv for your own analyses.")
    
    user_input = input("\nAdd English speaker labels? (yes/no): ")
    if user_input.lower() not in ['yes', 'y']:
        print("Skipping English speaker labels. You can run add_english_labels.py separately later.")
        print("\n" + "="*70)
        print("Pipeline Complete!")
        print("="*70)
        print("\nOutput files:")
        print(f"  - {UNIQUE_AUTHORS_FILE}")
        print(f"  - {AUTHOR_PROFILES_FILE} (MAIN OUTPUT - use this for your analyses)")
        print("\nYou can now use the author profiles file for your own analyses.")
        return
    
    try:
        final_df = process_author_profiles()
        if final_df is None:
            print("ERROR: Failed to add English speaker labels")
            return
    except Exception as e:
        print(f"ERROR in Step 3: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Step 4: Merge papers with language labels
    print("\n" + "="*70)
    print("STEP 4: Merging papers with language labels")
    print("="*70)
    
    try:
        merged_data = merge_papers_with_language()
        if merged_data is None:
            print("ERROR: Failed to merge papers with language labels")
            return
    except Exception as e:
        print(f"ERROR in Step 4: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Step 5: Tokenize abstracts
    print("\n" + "="*70)
    print("STEP 5: Tokenizing abstracts (Liang et al. method)")
    print("="*70)
    print("NOTE: This step tokenizes abstracts for LLM usage analysis.")
    
    user_input = input("\nTokenize abstracts? (yes/no): ")
    if user_input.lower() in ['yes', 'y']:
        try:
            tokenize_all_data()
        except Exception as e:
            print(f"ERROR in Step 5: {e}")
            import traceback
            traceback.print_exc()
            return
    else:
        print("Skipping tokenization. You can run tokenize_data.py separately later.")
    
    print("\n" + "="*70)
    print("Pipeline Complete!")
    print("="*70)
    print("\nOutput files:")
    print(f"  - {UNIQUE_AUTHORS_FILE}")
    print(f"  - {AUTHOR_PROFILES_FILE}")
    print(f"  - {AUTHOR_PROFILES_WITH_LANGUAGE_FILE}")
    print(f"  - output/iclr_*_with_language.csv (Merged paper data with language labels)")
    print(f"  - {TOKENIZED_DIR}/ (Tokenized abstracts for LLM analysis)")
    print("\nYou can now use these files for your analyses.")


if __name__ == "__main__":
    main()

