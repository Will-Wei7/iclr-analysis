# ICLR Author Profile Pipeline

A clean, reusable pipeline for collecting ICLR paper data (2018-2025), extracting unique first authors, fetching their profiles from OpenReview API, and inferring English speaker status based on education and email domain data.

## Overview

This pipeline performs the following steps:

1. **Extract Unique Authors**: Extracts unique first authors from ICLR papers (2018-2025)
2. **Fetch Profiles**: Fetches author profiles from OpenReview API (may take several hours)
3. **Add English Labels**: Infers English speaker status based on education background and email domains
4. **Merge Papers with Language**: Merges paper data with author language labels
5. **Tokenize Abstracts**: Tokenizes abstracts following Liang et al. method for LLM usage analysis

## Requirements

- Python 3.7+
- Required packages (see `requirements.txt`):
- pandas
- openreview-py
- tqdm
- pyarrow
- spacy

## Installation

1. Clone or download this pipeline folder
2. Install dependencies:
```bash
pip install -r requirements.txt
```
3. Download spaCy model (required for tokenization):
```bash
python -m spacy download en_core_web_sm
```

## Configuration

Before running the pipeline, you need to configure the paths in `config.py`:

1. **Set PROJECT_ROOT**: Modify the `PROJECT_ROOT` path to point to your project directory
2. **Set ICLR_DATA_DIR**: Path to your ICLR data files (CSV for 2018-2023, parquet for 2024-2025)
3. **Set Supporting Files**: Paths to:
   - `world_universities_and_domains.json` - Universities database
   - `country_region_toefl.csv` - TOEFL requirements by country

Example configuration:
```python
PROJECT_ROOT = Path("/path/to/your/project")
ICLR_DATA_DIR = PROJECT_ROOT / "data" / "iclr_data"
UNIVERSITIES_FILE = PROJECT_ROOT / "world_universities_and_domains.json"
TOEFL_FILE = PROJECT_ROOT / "country_region_toefl.csv"
```

## Data File Structure

The pipeline expects ICLR data files in the following format:

- **2018-2023**: CSV files named `iclr_YYYY_detailed.csv` with an `authors` column
- **2024-2025**: Parquet files named `iclr24.parquet` and `iclr25.parquet` with an `authors` column

Each file should contain paper data with at least an `authors` column (comma-separated author names).

## Usage

### Option 1: Run Complete Pipeline

Run all steps in sequence:
```bash
python run_pipeline.py
```

This will:
1. Extract unique first authors from all years
2. Prompt you to confirm before making API calls (which may take hours)
3. Fetch author profiles from OpenReview
4. Add English speaker labels
5. Merge papers with language labels
6. Optionally tokenize abstracts for LLM usage analysis

### Option 2: Run Steps Individually

You can also run each step separately:

#### Step 1: Extract Unique Authors
```bash
python extract_unique_authors.py
```
Output: `output/unique_first_authors_2018_2025.csv`

#### Step 2: Fetch Author Profiles
```bash
python fetch_profiles.py
```
Input: `output/unique_first_authors_2018_2025.csv`  
Output: `output/author_profiles_2018_2025.csv`

**Note**: This step makes API calls to OpenReview and may take several hours. Intermediate results are saved periodically.

#### Step 3: Add English Speaker Labels
```bash
python add_english_labels.py
```
Input: `output/author_profiles_2018_2025.csv`  
Output: `output/author_profiles_2018_2025_with_language.csv`

#### Step 4: Merge Papers with Language Labels
```bash
python merge_papers_with_language.py
```
Input: Paper data files + `output/author_profiles_2018_2025_with_language.csv`  
Output: `output/iclr_{year}_with_language.csv` for each year

**Note**: This step merges paper data with author language labels, creating files needed for tokenization.

#### Step 5: Tokenize Abstracts
```bash
python tokenize_data.py
```
Input: `output/iclr_{year}_with_language.csv` files  
Output: `output/tokenized_data/{year}_1.parquet`, `{year}_1_english.parquet`, `{year}_1_non_english.parquet`

**Note**: This step tokenizes abstracts following Liang et al. method for LLM usage analysis. Requires spaCy model (`python -m spacy download en_core_web_sm`).

## Output Files

All output files are saved in the `output/` directory:

- `unique_first_authors_2018_2025.csv`: List of unique first authors
- `author_profiles_2018_2025.csv`: Author profiles with education and email data
- `author_profiles_2018_2025_with_language.csv`: Author profiles with English speaker labels
- `iclr_{year}_with_language.csv`: Paper data merged with author language labels (one file per year)
- `tokenized_data/`: Directory containing tokenized abstracts:
  - `{year}_1.parquet`: All papers
  - `{year}_1_english.parquet`: English speaker first authors
  - `{year}_1_non_english.parquet`: Non-English speaker first authors

## Output Format

The final output file (`author_profiles_2018_2025_with_language.csv`) contains:

- `author_name`: Author name
- `profile_name`: Profile name from OpenReview
- `profile_id`: OpenReview profile ID
- `email_primary`: Primary email address
- `all_emails`: All email addresses (semicolon-separated)
- `current_position`: Current position
- `current_institution`: Current institution
- `current_country`: Current country
- `education_background`: JSON string of education history
- `education_countries`: Inferred countries from education (semicolon-separated)
- `english_speaker`: English speaker label
  - `1`: English speaker (from TOEFL-exempt country)
  - `0`: Non-English speaker
  - `-1`: Unknown (no country data available)

## English Speaker Inference Method

The pipeline infers English speaker status using:

1. **Education Background**: Extracts institutions from author's education history
2. **Email Domains**: Extracts institutional email domains (filters out common providers like gmail.com)
3. **Country Matching**: Matches institutions and domains to countries using:
   - World universities database
   - TLD (Top Level Domain) inference for email domains
4. **TOEFL Requirements**: Checks if any matched country is TOEFL-exempt (English-speaking)

## Tokenization (Liang et al. Method)

The pipeline includes tokenization functionality following the method from Liang et al. (2025) "Quantifying large language model usage in scientific papers":

1. **Merge Papers with Language**: Combines paper data with author language labels
2. **Tokenize Abstracts**: Uses spaCy to tokenize abstracts into sentences
3. **Group by Language**: Creates separate tokenized files for English and non-English speaker first authors
4. **Output Format**: Parquet files with columns:
   - `sentence`: List of tokens for each sentence
   - `paper_id`: ICLR paper ID
   - `first_author`: First author name

These tokenized files can be used for word frequency shift analysis to estimate LLM usage prevalence.

## Troubleshooting

### File Not Found Errors

- Check that all paths in `config.py` are correct
- Ensure ICLR data files exist in the specified directory
- Verify supporting files (`world_universities_and_domains.json`, `country_region_toefl.csv`) exist

### API Errors

- The OpenReview API may have rate limits - the script includes delays between requests
- If API calls fail, intermediate results are saved periodically
- You can resume by running `fetch_profiles.py` again (it will continue from where it left off)

### Missing Data

- Some authors may not have OpenReview profiles - they will have empty fields
- Authors without country data will have `english_speaker = -1`

## License

This pipeline is provided as-is for research purposes.

## Citation

If you use this pipeline in your research, please cite appropriately.

