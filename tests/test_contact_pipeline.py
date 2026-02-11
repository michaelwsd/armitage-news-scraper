"""
End-to-end test for the contact LinkedIn activity pipeline.

Tests the full flow:
  1. Salesforce  -> get primary contact names for sample companies
  2. SerpAPI     -> Google search for contact's LinkedIn profile URL
  3. BrightData  -> scrape contact's LinkedIn posts (past 30 days)
  4. OpenAI      -> summarize posts into dot-point summaries
  5. Salesforce  -> push formatted HTML to Profile Activity field

Usage:
    python tests/test_contact_pipeline.py                     # test all steps
    python tests/test_contact_pipeline.py --step salesforce    # test single step
    python tests/test_contact_pipeline.py --step search
    python tests/test_contact_pipeline.py --step scrape
    python tests/test_contact_pipeline.py --step summarize
    python tests/test_contact_pipeline.py --step push
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv

load_dotenv()

# ── Logging ──────────────────────────────────────────────────────────────────

LOG_FORMAT = (
    "%(asctime)s | %(levelname)-8s | %(name)-30s | %(message)s"
)
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("test_contact_pipeline")

# ── Sample companies ─────────────────────────────────────────────────────────
# These are real Opportunity names from the Salesforce instance.
# Edit this list to test with different companies.

SAMPLE_COMPANIES = [
    "OnQ Software",
    "Axcelerate",
]

# ── Helpers ──────────────────────────────────────────────────────────────────

def banner(title):
    width = 70
    logger.info("=" * width)
    logger.info(f"  {title}")
    logger.info("=" * width)


def step_result(step_name, success, detail=""):
    status = "PASS" if success else "FAIL"
    msg = f"[{status}] {step_name}"
    if detail:
        msg += f" — {detail}"
    if success:
        logger.info(msg)
    else:
        logger.error(msg)
    return success


# ── Step 1: Salesforce — get primary contacts ────────────────────────────────

def test_salesforce(company_names):
    banner("Step 1: Salesforce — Get Primary Contacts")

    from salesforce import get_access_token, sf_get, get_primary_contacts

    logger.info("Authenticating with Salesforce...")
    try:
        token = get_access_token()
    except Exception as e:
        step_result("Salesforce auth", False, str(e))
        return None
    step_result("Salesforce auth", True, "token obtained")

    logger.info(f"Querying OpportunityContactRole for {len(company_names)} companies...")
    company_to_contact = get_primary_contacts(token, company_names)

    for company, contact in company_to_contact.items():
        if contact:
            logger.info(f"  {company:<30} -> {contact}")
        else:
            logger.warning(f"  {company:<30} -> (no primary contact)")

    mapped = sum(1 for v in company_to_contact.values() if v is not None)
    step_result(
        "Salesforce contacts",
        mapped > 0,
        f"{mapped}/{len(company_names)} companies have a primary contact",
    )

    return company_to_contact


# ── Step 2: SerpAPI — find LinkedIn profile URL ──────────────────────────────

def test_search(company_to_contact):
    banner("Step 2: SerpAPI — Google Search for LinkedIn Profile URL")

    from company.serp_contact_url import get_contact_linkedin_url

    contact_urls = {}  # company -> (contact_name, linkedin_url)

    for company, contact_name in company_to_contact.items():
        if not contact_name:
            logger.info(f"  Skipping {company} (no contact name)")
            continue

        logger.info(f"  Searching: \"{contact_name} {company} LinkedIn\"")
        url = get_contact_linkedin_url(contact_name, company)

        if url:
            logger.info(f"    Found: {url}")
            contact_urls[company] = (contact_name, url)
        else:
            logger.warning(f"    No LinkedIn profile URL found for {contact_name}")

    found = len(contact_urls)
    total = sum(1 for v in company_to_contact.values() if v is not None)
    step_result(
        "SerpAPI search",
        found > 0,
        f"{found}/{total} contacts have a LinkedIn URL",
    )

    return contact_urls


# ── Step 3: BrightData — scrape contact's LinkedIn posts ────────────────────

def test_scrape(contact_urls):
    banner("Step 3: BrightData — Scrape Contact LinkedIn Posts")

    from scrapers.linkedin_contact_scraper import scrape_contact_linkedin

    scraped = {}  # company -> filepath

    for company, (contact_name, linkedin_url) in contact_urls.items():
        logger.info(f"  Scraping posts for {contact_name} ({company})...")
        logger.info(f"    URL: {linkedin_url}")

        start_time = time.time()
        filepath = scrape_contact_linkedin(contact_name, linkedin_url, company)
        elapsed = time.time() - start_time

        if filepath and os.path.exists(filepath):
            with open(filepath, "r") as f:
                posts = json.load(f)
            post_count = len(posts) if isinstance(posts, list) else 1
            logger.info(f"    Scraped {post_count} posts in {elapsed:.0f}s -> {filepath}")
            scraped[company] = (contact_name, filepath, post_count)
        else:
            logger.warning(f"    No posts returned after {elapsed:.0f}s")

    step_result(
        "BrightData scrape",
        len(scraped) > 0,
        f"{len(scraped)}/{len(contact_urls)} contacts returned posts",
    )

    return scraped


# ── Step 4: OpenAI — summarize contact posts ────────────────────────────────

def test_summarize(scraped):
    banner("Step 4: OpenAI — Summarize Contact Posts")

    from utils.summarizer import summarize_contact_posts

    summaries = {}  # company -> (contact_name, summaries_list)

    for company, (contact_name, filepath, post_count) in scraped.items():
        logger.info(f"  Summarizing {post_count} posts for {contact_name} ({company})...")

        result = summarize_contact_posts(filepath, contact_name)

        if result is not None:
            logger.info(f"    Generated {len(result)} summaries:")
            for i, s in enumerate(result):
                logger.info(f"      [{i+1}] ({s.get('topic', '?')}) {s.get('summary', '')[:80]}")
                logger.info(f"           Date: {s.get('date', '?')}")
            summaries[company] = (contact_name, result)
        else:
            logger.warning(f"    Summarization returned None for {contact_name}")

    step_result(
        "OpenAI summarization",
        len(summaries) > 0,
        f"{len(summaries)}/{len(scraped)} contacts summarized",
    )

    return summaries


# ── Step 5: Salesforce — push to Profile Activity field ──────────────────────

def test_push(company_to_contact, summaries):
    """
    Push contact activity to Salesforce for ALL contacts from Step 1.
    Contacts with summaries get their posts pushed.
    Contacts without posts get a "no recent activity" message pushed.
    """
    banner("Step 5: Salesforce — Push to Profile Activity Field (P__c)")

    from salesforce import (
        get_access_token,
        sf_patch,
        _format_contact_activity_html,
        _get_opportunity_ids,
    )

    logger.info("Authenticating with Salesforce...")
    token = get_access_token()

    company_names = list(company_to_contact.keys())
    name_to_id = _get_opportunity_ids(token, company_names)
    logger.info(f"  Matched {len(name_to_id)}/{len(company_names)} companies to Opportunity IDs")

    pushed = 0
    for company, contact_name in company_to_contact.items():
        opp_id = name_to_id.get(company)
        if not opp_id:
            logger.warning(f"  No Opportunity ID for {company}, skipping push")
            continue

        # Use summaries if available, otherwise push empty (triggers "no activity" message)
        if company in summaries:
            _, summary_list = summaries[company]
        else:
            summary_list = []

        data = {
            "contact_name": contact_name,
            "contact_posts": summary_list,
        }
        html = _format_contact_activity_html(data)

        post_count = len(summary_list)
        label = f"{post_count} posts" if post_count > 0 else "no activity message"
        logger.info(f"  Pushing to {company} (Opp ID: {opp_id}) — {label}")
        logger.info(f"    Contact: {contact_name or '(none)'}, HTML: {len(html)} chars")

        resp = sf_patch(f"sobjects/Opportunity/{opp_id}", token, {"P__c": html})

        if resp.status_code == 204:
            logger.info(f"    Push successful for {company}")
            pushed += 1
        else:
            logger.error(f"    Push failed for {company}: {resp.status_code} {resp.text[:200]}")

    step_result(
        "Salesforce push",
        pushed > 0,
        f"{pushed}/{len(company_to_contact)} companies updated",
    )

    return pushed


# ── Cleanup ──────────────────────────────────────────────────────────────────

def cleanup(scraped):
    """Remove intermediate Contact Posts JSON files."""
    if not scraped:
        return
    for company, (contact_name, filepath, _) in scraped.items():
        try:
            if os.path.exists(filepath):
                os.remove(filepath)
                logger.info(f"  Cleaned up: {filepath}")
        except Exception as e:
            logger.warning(f"  Could not delete {filepath}: {e}")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Test contact LinkedIn activity pipeline")
    parser.add_argument(
        "--step",
        choices=["salesforce", "search", "scrape", "summarize", "push"],
        help="Run only a specific step (default: run all steps end-to-end)",
    )
    parser.add_argument(
        "--companies",
        nargs="+",
        help="Override sample companies (space-separated Opportunity names)",
    )
    parser.add_argument(
        "--no-cleanup",
        action="store_true",
        help="Keep intermediate files after test",
    )
    args = parser.parse_args()

    companies = args.companies or SAMPLE_COMPANIES

    banner("Contact Pipeline End-to-End Test")
    logger.info(f"Sample companies: {companies}")
    logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    results = {}
    scraped = {}
    summaries = {}
    contact_urls = {}
    company_to_contact = {}

    # Step 1: Salesforce
    if args.step in (None, "salesforce"):
        try:
            company_to_contact = test_salesforce(companies) or {}
            results["salesforce"] = any(company_to_contact.values())
        except Exception as e:
            logger.error(f"Step 1 crashed: {e}")
            results["salesforce"] = False
    else:
        company_to_contact = {"OnQ Software": "Nick Gannoulis"}
        logger.info(f"Skipping Salesforce step, using mock: {company_to_contact}")

    # Step 2: SerpAPI search
    if args.step in (None, "search"):
        try:
            contact_urls = test_search(company_to_contact)
            results["search"] = len(contact_urls) > 0
        except Exception as e:
            logger.error(f"Step 2 crashed: {e}")
            results["search"] = False
    else:
        if args.step != "salesforce":
            contact_urls = {
                "OnQ Software": ("Nick Gannoulis", "https://www.linkedin.com/in/nick-gannoulis-2a94991/")
            }
            logger.info(f"Skipping search step, using mock URL")

    # Step 3: BrightData scrape
    if args.step in (None, "scrape"):
        try:
            scraped = test_scrape(contact_urls)
            results["scrape"] = len(scraped) > 0
        except Exception as e:
            logger.error(f"Step 3 crashed: {e}")
            results["scrape"] = False
    else:
        if args.step not in ("salesforce", "search"):
            test_file = PROJECT_ROOT / "data" / "output" / "OnQ Software Contact Posts.json"
            if test_file.exists():
                try:
                    with open(test_file) as f:
                        post_count = len(json.load(f))
                    scraped = {"OnQ Software": ("Nick Gannoulis", str(test_file), post_count)}
                    logger.info(f"Skipping scrape step, using existing file: {test_file}")
                except Exception:
                    logger.info("Skipping scrape step, existing file is corrupt")
            else:
                logger.info("Skipping scrape step, no existing file found")

    # Step 4: OpenAI summarization
    if args.step in (None, "summarize"):
        try:
            summaries = test_summarize(scraped)
            results["summarize"] = len(summaries) > 0
        except Exception as e:
            logger.error(f"Step 4 crashed: {e}")
            results["summarize"] = False

    # Step 5: Salesforce push (always runs — pushes "no activity" for contacts without posts)
    if args.step in (None, "push"):
        try:
            if company_to_contact:
                pushed = test_push(company_to_contact, summaries)
                results["push"] = pushed > 0
            else:
                logger.warning("No contacts to push — skipping push step")
                results["push"] = False
        except Exception as e:
            logger.error(f"Step 5 crashed: {e}")
            results["push"] = False

    # Cleanup
    if not args.no_cleanup and scraped:
        logger.info("")
        logger.info("Cleaning up intermediate files...")
        cleanup(scraped)

    # Final summary
    banner("Test Summary")
    all_pass = True
    for step_name, passed in results.items():
        status = "PASS" if passed else "FAIL"
        icon = "+" if passed else "-"
        logger.info(f"  [{icon}] {step_name:<20} {status}")
        if not passed:
            all_pass = False

    logger.info("")
    if all_pass:
        logger.info("All steps passed.")
    else:
        logger.warning("Some steps failed. Check logs above for details.")

    logger.info(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return 0 if all_pass else 1


if __name__ == "__main__":
    sys.exit(main())
