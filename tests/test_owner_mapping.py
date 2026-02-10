"""
Test script to show which companies will be sent to which owner.

Usage:
    python tests/test_owner_mapping.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.email_client import load_owner_mapping, load_json_files


def main():
    mapping = load_owner_mapping()
    if mapping is None:
        print("No owner_mapping.json found. Run salesforce.py first.")
        return

    owner_to_companies = mapping.get("owner_to_companies", {})
    unmapped = mapping.get("unmapped_companies", [])

    # Check which companies have scraped data
    scraped = {cd.get("company") for cd in load_json_files()}

    print(f"{'='*60}")
    print(f"Owner Digest Distribution")
    print(f"{'='*60}")

    total = 0
    for owner_email, company_names in sorted(owner_to_companies.items()):
        print(f"\n{owner_email} ({len(company_names)} companies)")
        print(f"{'-'*40}")
        for name in sorted(company_names):
            status = "ready" if name in scraped else "not scraped"
            print(f"  {name:<35} [{status}]")
        total += len(company_names)

    if unmapped:
        print(f"\nUNMAPPED - fallback recipients ({len(unmapped)} companies)")
        print(f"{'-'*40}")
        for name in sorted(unmapped):
            status = "ready" if name in scraped else "not scraped"
            print(f"  {name:<35} [{status}]")
        total += len(unmapped)

    print(f"\n{'='*60}")
    print(f"Total: {len(owner_to_companies)} owners, {total} companies, {len(unmapped)} unmapped")
    print(f"Scraped data available: {len(scraped)} companies")


if __name__ == "__main__":
    main()
