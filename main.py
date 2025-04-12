# main.py

from dataset_manager import DatasetManager
from media_scanner import MediaScanner
from media_matcher import MediaMatcher


def run():
    print("\n--- IMDb Sync & Match ---\n")

    # Step 1: Sync IMDb dataset
    print("[1] Updating IMDb datasets...")
    dataset = DatasetManager()
    dataset.update_all()

    # Step 2: Scan media directory
    print("\n[2] Scanning local media files...")
    scanner = MediaScanner()
    scanned = scanner.scan()
    print(f"Found {len(scanned['movies'])} movies and {len(scanned['shows'])} shows.")

    # Step 3: Match media files to IMDb
    print("\n[3] Matching files to IMDb titles...")
    matcher = MediaMatcher()

    all_matches = []
    print("\nMovies:")
    for match in matcher.match_batch(scanned['movies']):
        print(f"{match['file']} => {match['matched_title']} (Score: {match['score']})")
        all_matches.append(match)

    print("\nTV Shows:")
    for match in matcher.match_batch(scanned['shows']):
        print(f"{match['file']} => {match['matched_title']} (Score: {match['score']})")
        all_matches.append(match)


if __name__ == "__main__":
    run()
