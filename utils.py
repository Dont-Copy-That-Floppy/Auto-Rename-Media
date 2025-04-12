# utils.py

import os
import re
from rapidfuzz import fuzz


def clean_filename(name):
    name = os.path.splitext(name)[0]  # Remove extension
    name = re.sub(r'[._\-]', ' ', name)  # Replace symbols with space
    name = re.sub(r'\(\d{4}\)', '', name)  # Remove year in parentheses
    name = re.sub(r'\d{3,4}p', '', name, flags=re.IGNORECASE)  # Remove resolutions
    return name.strip().lower()


def fuzzy_match(query, candidates, threshold=85):
    best_match = None
    highest_score = 0
    for candidate in candidates:
        score = fuzz.ratio(query, candidate.lower())
        if score > highest_score and score >= threshold:
            best_match = candidate
            highest_score = score
    return best_match, highest_score


def tsv_to_documents(tsv_path):
    documents = []
    try:
        with open(tsv_path, encoding='utf-8') as f:
            header = f.readline().strip().split('\t')
            for line in f:
                values = line.strip().split('\t')
                if len(values) == len(header):
                    doc = {header[i]: values[i] for i in range(len(header))}
                    doc['_id'] = doc.get('tconst') or doc.get('nconst') or None
                    documents.append(doc)
    except Exception as e:
        print(f"Error reading {tsv_path}: {e}")
    return documents
