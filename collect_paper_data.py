"""
Collect ICLR paper data from OpenReview API (2018-2025)
Supports both API v1 (2018-2023) and API v2 (2024-2025)
"""

import openreview
import requests
import csv
import statistics
import time
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Optional
from tqdm import tqdm
from config import ICLR_DATA_DIR, YEARS

ICLR_DATA_DIR.mkdir(parents=True, exist_ok=True)

# API Configuration
API_CONFIG = {
    "2018": {
        "api": "v1",
        "invitation": "ICLR.cc/2018/Conference/-/Blind_Submission",
        "decision_inv": "Acceptance_Decision"
    },
    "2019": {
        "api": "v1",
        "invitation": "ICLR.cc/2019/Conference/-/Blind_Submission",
        "decision_inv": "Decision"
    },
    "2020": {
        "api": "v1",
        "invitation": "ICLR.cc/2020/Conference/-/Blind_Submission",
        "decision_inv": "Decision"
    },
    "2021": {
        "api": "v1",
        "invitation": "ICLR.cc/2021/Conference/-/Blind_Submission",
        "decision_inv": "Decision"
    },
    "2022": {
        "api": "v1",
        "invitation": "ICLR.cc/2022/Conference/-/Blind_Submission",
        "decision_inv": "Decision"
    },
    "2023": {
        "api": "v1",
        "invitation": "ICLR.cc/2023/Conference/-/Blind_Submission",
        "decision_inv": "Decision"
    },
    # "2024": {
    #     "api": "v2",
    #     "venueid": "ICLR.cc/2024/Conference"
    # },
    # "2025": {
    #     "api": "v2",
    #     "venueid": "ICLR.cc/2025/Conference"
    # }
}


def safe_get_json(url: str, params: Optional[Dict] = None, max_attempts: int = 5) -> Optional[Dict]:
    """Safely fetch JSON data with retry logic and rate limit handling."""
    for attempt in range(max_attempts):
        try:
            resp = requests.get(url, params=params, timeout=60)
            
            if resp.status_code == 429 or resp.status_code >= 500:
                print(f"  [Server Busy] Status {resp.status_code}. Sleeping 10s...")
                time.sleep(10)
                continue
            
            try:
                data = resp.json()
            except:
                print("  [Error] Response was not valid JSON. Retrying...")
                time.sleep(5)
                continue

            if 'name' in data and data['name'] == 'RateLimitError':
                print("  [Rate Limit] API told us to wait. Sleeping 30s...")
                time.sleep(30)
                continue
                
            return data
            
        except requests.exceptions.RequestException as e:
            print(f"  [Connection Error] {e}. Sleeping 5s...")
            time.sleep(5)
    
    print(f"!!! Failed to fetch data after {max_attempts} attempts !!!")
    return None


def get_data_v1(client, year: int, config: Dict[str, str]) -> List[Dict[str, Any]]:
    """Retrieve data for ICLR 2018-2023 using API v1"""
    print(f"Fetching ICLR {year} (API v1)...")
    
    submissions = openreview.tools.iterget_notes(
        client,
        invitation=config['invitation'],
        details='directReplies'
    )
    
    rows = []
    for note in tqdm(submissions, desc=f"Processing {year}"):
        row = {
            "year": year,
            "id": note.id,
            "title": note.content.get('title', ''),
            "abstract": note.content.get('abstract', ''),
            "authors": ", ".join(note.content.get('authors', [])),
            "first_author": note.content.get('authors', [''])[0] if note.content.get('authors') else '',
            "decision": "Reject/Unknown",
            "score": "N/A",
            "soundness_score": "N/A",
            "presentation_score": "N/A",
            "contribution_score": "N/A",
            "contributions": note.content.get('main_contributions', note.content.get('abstract', '')),
            "introduction": "PDF Parsing Required",
            "conclusion": "PDF Parsing Required"
        }
        
        replies = note.details.get('directReplies', [])
        
        # Extract decision
        decisions = [r for r in replies if config['decision_inv'] in r['invitation']]
        if decisions:
            d_content = decisions[0]['content']
            row["decision"] = d_content.get('decision', d_content.get('title', 'Unknown'))
        
        # Extract reviews and scores
        ratings = []
        soundness_ratings = []
        presentation_ratings = []
        contribution_ratings = []
        
        reviews = [r for r in replies if 'Official_Review' in r['invitation'] or 'Review' in r['invitation']]
        
        for r in reviews:
            c = r['content']
            
            if 'rating' in c:
                try:
                    ratings.append(int(str(c['rating']).split(':')[0]))
                except Exception:
                    pass
            
            for field_name, collector in [
                ('soundness', soundness_ratings),
                ('presentation', presentation_ratings),
                ('contribution', contribution_ratings),
            ]:
                if field_name in c and c[field_name] is not None:
                    try:
                        collector.append(int(str(c[field_name]).split(':')[0]))
                    except Exception:
                        pass
        
        if ratings:
            row["score"] = round(statistics.mean(ratings), 2)
        if soundness_ratings:
            row["soundness_score"] = round(statistics.mean(soundness_ratings), 2)
        if presentation_ratings:
            row["presentation_score"] = round(statistics.mean(presentation_ratings), 2)
        if contribution_ratings:
            row["contribution_score"] = round(statistics.mean(contribution_ratings), 2)
        
        rows.append(row)
    
    return rows


def get_data_v2(year: int, config: Dict[str, str]) -> List[Dict[str, Any]]:
    """Retrieve data for ICLR 2024-2025 using API v2"""
    print(f"Fetching ICLR {year} (API v2)...")
    
    queries = ['', '/Withdrawn_Submission', '/Rejected_Submission', '/Desk_Rejected_Submission']
    all_rows = []
    seen_ids = set()
    batch_size = 1000
    
    for query in queries:
        venue_param = f"{config['venueid']}{query}"
        url = "https://api2.openreview.net/notes"
        
        for offset in tqdm(range(0, 50000, batch_size), desc=f"Fetching {query or 'Main'}"):
            params = {
                'content.venueid': venue_param,
                'offset': offset,
                'limit': batch_size,
                'details': 'basic'
            }
            
            data = safe_get_json(url, params)
            
            if not data or 'notes' not in data or len(data['notes']) == 0:
                break
            
            batch = data['notes']
            
            for note in batch:
                if note['id'] in seen_ids:
                    continue
                seen_ids.add(note['id'])
                
                c = note.get('content', {})
                
                # Determine decision
                decision = "Pending/Unknown"
                if 'Withdrawn' in query:
                    decision = "Withdrawn"
                elif 'Desk_Rejected' in query:
                    decision = "Desk Reject"
                elif 'Rejected' in query:
                    decision = "Reject"
                
                # Extract data
                authors_list = c.get('authors', {}).get('value', [])
                authors_str = ", ".join(authors_list)
                first_author = authors_list[0] if authors_list else ""
                
                row = {
                    'year': year,
                    'id': note['id'],
                    'title': c.get('title', {}).get('value', ''),
                    'abstract': c.get('abstract', {}).get('value', ''),
                    'authors': authors_str,
                    'first_author': first_author,
                    'decision': decision,
                    'score': "N/A",
                    'soundness_score': "N/A",
                    'presentation_score': "N/A",
                    'contribution_score': "N/A",
                    'contributions': str(c.get('keywords', {}).get('value', [])),
                    'introduction': "PDF Parsing Required",
                    'conclusion': "PDF Parsing Required"
                }
                
                all_rows.append(row)
            
            if len(batch) < batch_size:
                break
    
    # Enrich with review scores (optional, can be slow)
    print(f"Enriching {len(all_rows)} papers with review scores...")
    for i, row in enumerate(tqdm(all_rows[:100], desc="Fetching reviews")):  # Limit to first 100 for speed
        forum_id = row['id']
        url = f"https://api2.openreview.net/notes?forum={forum_id}"
        forum_data = safe_get_json(url)
        
        if forum_data and 'notes' in forum_data:
            notes = forum_data['notes']
            
            # Update decision if found
            if row['decision'] in ["Pending/Unknown", ""]:
                for n in notes:
                    if 'decision' in n.get('content', {}):
                        row['decision'] = n['content']['decision'].get('value', row['decision'])
                        break
            
            # Extract review scores
            ratings = []
            soundness_ratings = []
            presentation_ratings = []
            contribution_ratings = []
            
            for n in notes:
                if 'invitation' in n and 'Official_Review' in n['invitation']:
                    c = n.get('content', {})
                    
                    def get_int_score(field_name):
                        if field_name in c:
                            val = c[field_name].get('value') if isinstance(c[field_name], dict) else c[field_name]
                            if isinstance(val, int):
                                return val
                            if isinstance(val, str) and ':' in val:
                                try:
                                    return int(val.split(':')[0])
                                except:
                                    pass
                            if isinstance(val, str) and val.isdigit():
                                return int(val)
                        return None
                    
                    rating = get_int_score('rating')
                    if rating is not None:
                        ratings.append(rating)
                    
                    snd = get_int_score('soundness')
                    if snd is not None:
                        soundness_ratings.append(snd)
                    
                    pres = get_int_score('presentation')
                    if pres is not None:
                        presentation_ratings.append(pres)
                    
                    contrib = get_int_score('contribution')
                    if contrib is not None:
                        contribution_ratings.append(contrib)
            
            if ratings:
                row['score'] = round(statistics.mean(ratings), 2)
            if soundness_ratings:
                row['soundness_score'] = round(statistics.mean(soundness_ratings), 2)
            if presentation_ratings:
                row['presentation_score'] = round(statistics.mean(presentation_ratings), 2)
            if contribution_ratings:
                row['contribution_score'] = round(statistics.mean(contribution_ratings), 2)
    
    return all_rows


def collect_all_data(save_individual_files: bool = True) -> Dict[int, pd.DataFrame]:
    """
    Collect ICLR paper data for all years (2018-2025)
    
    Args:
        save_individual_files: If True, save individual CSV/parquet files per year
        
    Returns:
        Dictionary mapping year to DataFrame
    """
    print("="*70)
    print("ICLR Paper Data Collection Pipeline (2018-2025)")
    print("="*70)
    
    # Initialize clients
    client_v1 = openreview.Client(baseurl='https://api.openreview.net')
    
    all_dataframes = {}
    fieldnames = [
        "year", "id", "title", "abstract", "authors", "decision", "score",
        "soundness_score", "presentation_score", "contribution_score",
        "contributions", "first_author", "introduction", "conclusion"
    ]
    
    for year in YEARS:
        year_str = str(year)
        if year_str not in API_CONFIG:
            print(f"Warning: No configuration for year {year}, skipping...")
            continue
        
        config = API_CONFIG[year_str]
        try:
            if config['api'] == 'v1':
                data = get_data_v1(client_v1, year, config)
            else:
                data = get_data_v2(year, config)
            
            print(f"  -> Retrieved {len(data)} papers for {year}")
            
            # Create DataFrame
            df = pd.DataFrame(data)
            all_dataframes[year] = df
            
            # Save individual file
            if save_individual_files:
                if year >= 2024:
                    # Save as parquet for 2024-2025
                    output_file = ICLR_DATA_DIR / f"iclr{year}.parquet"
                    df.to_parquet(output_file, index=False)
                    print(f"  -> Saved to {output_file}")
                else:
                    # Save as CSV for 2018-2023
                    output_file = ICLR_DATA_DIR / f"iclr_{year}_detailed.csv"
                    df.to_csv(output_file, index=False)
                    print(f"  -> Saved to {output_file}")
            
        except Exception as e:
            print(f"Error fetching {year}: {e}")
            import traceback
            traceback.print_exc()
    
    # Save combined file
    all_data = []
    for year in sorted(all_dataframes.keys()):
        all_data.extend(all_dataframes[year].to_dict('records'))
    
    combined_file = ICLR_DATA_DIR / "iclr_papers_2018_2025.csv"
    print(f"\nWriting combined data to {combined_file}...")
    with open(combined_file, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_data)
    
    print(f"Done. Collected {len(all_data)} papers total.")
    return all_dataframes


if __name__ == "__main__":
    collect_all_data()

