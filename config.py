"""
Configuration file for ICLR Author Profile Pipeline
Modify paths here to match your directory structure.
"""

from pathlib import Path

# Base directory - modify this to match your project root
PROJECT_ROOT = Path(__file__).parent.parent

# Input data directories
ICLR_DATA_DIR = PROJECT_ROOT / "data" / "iclr_data"

# Supporting data files (modify paths as needed)
UNIVERSITIES_FILE = PROJECT_ROOT / "world_universities_and_domains.json"
TOEFL_FILE = PROJECT_ROOT / "country_region_toefl.csv"

# Output directory
OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

# Output files
UNIQUE_AUTHORS_FILE = OUTPUT_DIR / "unique_first_authors_2018_2025.csv"
AUTHOR_PROFILES_FILE = OUTPUT_DIR / "author_profiles_2018_2025.csv"
AUTHOR_PROFILES_WITH_LANGUAGE_FILE = OUTPUT_DIR / "author_profiles_2018_2025_with_language.csv"

# Tokenized data directory
TOKENIZED_DIR = OUTPUT_DIR / "tokenized_data"
TOKENIZED_DIR.mkdir(exist_ok=True)

# ICLR data file mapping (modify if your files are named differently)
ICLR_DATA_FILES = {
    2018: ICLR_DATA_DIR / "iclr_2018_detailed.csv",
    2019: ICLR_DATA_DIR / "iclr_2019_detailed.csv",
    2020: ICLR_DATA_DIR / "iclr_2020_detailed.csv",
    2021: ICLR_DATA_DIR / "iclr_2021_detailed.csv",
    2022: ICLR_DATA_DIR / "iclr_2022_detailed.csv",
    2023: ICLR_DATA_DIR / "iclr_2023_detailed.csv",
    2024: ICLR_DATA_DIR / "iclr24.parquet",
    2025: ICLR_DATA_DIR / "iclr25.parquet",
}

# Years to process
YEARS = list(range(2018, 2026))  # 2018-2025

# API settings
API_BATCH_SIZE = 100
API_SAVE_INTERVAL = 10
API_RATE_LIMIT_DELAY = 0.05  # seconds between API calls

