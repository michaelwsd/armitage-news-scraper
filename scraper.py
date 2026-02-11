import asyncio
import csv
import json
import logging
import os
import random
from company.get_company_info import get_info
from scrapers.linkedin_scraper_api import scrape_news_linkedin as scrape_linkedin_api
from scrapers.linkedin_scraper_requests import scrape_news_linkedin as scrape_linkedin_requests
from scrapers.linkedin_scraper_playwright import scrape_news_linkedin as scrape_linkedin_playwright
from utils.summarizer import summarize_posts, generate_reachout_message, generate_potential_actions, add_posts_to_news_file, summarize_contact_posts
from scrapers.perplexity_scraper import scrape_news_perplexity
from company.serp_contact_url import get_contact_linkedin_url
from scrapers.linkedin_contact_scraper import scrape_contact_linkedin

logging.basicConfig(
    level=logging.INFO,  # change to DEBUG for more verbosity
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


def load_contact_mapping():
    """Load the company -> contact_name mapping from JSON."""
    mapping_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "input", "contact_mapping.json")
    if not os.path.exists(mapping_path):
        logger.warning("Contact mapping not found")
        return {}
    try:
        with open(mapping_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load contact mapping: {e}")
        return {}


def _add_contact_data_to_output(news_filepath, contact_name, contact_summaries):
    """Add contact name and contact post summaries to the company output JSON."""
    try:
        with open(news_filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)

        data['contact_name'] = contact_name
        data['contact_posts'] = contact_summaries if contact_summaries else []

        with open(news_filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)

        post_count = len(data['contact_posts'])
        logger.info(f"Added contact data to {news_filepath}: {contact_name}, {post_count} posts")
    except Exception as e:
        logger.warning(f"Could not add contact data to {news_filepath}: {e}")


def ensure_posts_field(news_filepath):
    """
    Ensure the JSON file has a 'posts' field, adding an empty array if missing.
    """
    if not news_filepath or not os.path.exists(news_filepath):
        return False

    try:
        with open(news_filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)

        if 'posts' not in data:
            data['posts'] = []
            with open(news_filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
            logger.info(f"Added empty posts array to {news_filepath}")

        return True
    except Exception as e:
        logger.warning(f"Could not ensure posts field in {news_filepath}: {e}")
        return False


def add_linkedin_url(news_filepath, company_info):
    """
    Add linkedin_url field to the JSON file.
    """
    if not news_filepath or not os.path.exists(news_filepath):
        return False

    try:
        with open(news_filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)

        linkedin_id = company_info.get('linkedin') if company_info else None
        if linkedin_id:
            data['linkedin_url'] = f"https://www.linkedin.com/company/{linkedin_id}/posts/"
        else:
            data['linkedin_url'] = None

        with open(news_filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)

        logger.info(f"Added linkedin_url to {news_filepath}")
        return True
    except Exception as e:
        logger.warning(f"Could not add linkedin_url to {news_filepath}: {e}")
        return False

async def scrape(company, location):
    """
    Scrape news and LinkedIn posts for a single company.

    This function handles failures gracefully - if one step fails,
    it will continue with subsequent steps where possible.

    Returns:
        dict: Results summary with success/failure status for each step
    """
    results = {
        'company': company,
        'location': location,
        'company_info': False,
        'news_scrape': False,
        'linkedin_scrape': False,
        'contact_scrape': False,
        'summarization': False,
        'errors': []
    }

    # Step 1: Get company info
    logger.info(f"Starting scrape for {company} in {location}")
    try:
        company_info = get_info(company, location)
    except Exception as e:
        logger.exception(f"Unexpected error getting company info for {company}: {e}")
        company_info = None
        results['errors'].append(f"Company info: {e}")

    if not company_info:
        logger.error(f"Could not retrieve company info for {company}, skipping this company")
        return results

    results['company_info'] = True
    logger.debug("Retrieved company info: %s", company_info)

    # Step 2: Scrape news from Perplexity
    news_filepath = None
    try:
        news_filepath = await scrape_news_perplexity(company_info, "month")
        if news_filepath:
            results['news_scrape'] = True
            logger.info(f"News scrape successful for {company}")
        else:
            logger.warning(f"News scrape returned no results for {company}")
            results['errors'].append("News scrape returned None")
    except Exception as e:
        logger.exception(f"Unexpected error in news scrape for {company}: {e}")
        results['errors'].append(f"News scrape: {e}")

    # Step 3: Scrape LinkedIn posts (try API -> Requests -> Playwright)
    posts_filepath = None
    scraper_used = None

    # Try API scraper first
    try:
        logger.info(f"Attempting LinkedIn scrape via API for {company}")
        posts_filepath = scrape_linkedin_api(company_info)
        if posts_filepath:
            results['linkedin_scrape'] = True
            scraper_used = 'API'
            logger.info(f"LinkedIn API scrape successful for {company}")
        else:
            logger.warning(f"LinkedIn API scrape returned no results for {company}")
    except Exception as e:
        logger.warning(f"LinkedIn API scrape failed for {company}: {e}")
        results['errors'].append(f"LinkedIn API scrape: {e}")

    # Fall back to requests-based scraper if API failed (only if explicitly enabled)
    use_requests_fallback = os.getenv('USE_REQUESTS_FALLBACK', 'true').lower() == 'true'

    if not posts_filepath and use_requests_fallback:
        try:
            logger.info(f"Falling back to requests-based scraper for {company}")
            posts_filepath = scrape_linkedin_requests(company_info)
            if posts_filepath:
                results['linkedin_scrape'] = True
                scraper_used = 'Requests'
                logger.info(f"LinkedIn requests scrape successful for {company}")
            else:
                logger.warning(f"LinkedIn requests scrape returned no results for {company}")
        except Exception as e:
            logger.warning(f"LinkedIn requests scrape failed for {company}: {e}")
            results['errors'].append(f"LinkedIn requests scrape: {e}")
    elif not posts_filepath and not use_requests_fallback:
        logger.info(f"Requests fallback disabled. Set USE_REQUESTS_FALLBACK=true to enable.")

    # Fall back to Playwright if both API and Requests failed (only if explicitly enabled)
    use_playwright_fallback = os.getenv('USE_PLAYWRIGHT_FALLBACK', 'false').lower() == 'true'

    if not posts_filepath and use_playwright_fallback:
        try:
            logger.info(f"Falling back to Playwright scraper for {company}")
            posts_filepath = await scrape_linkedin_playwright(company_info)
            if posts_filepath:
                results['linkedin_scrape'] = True
                scraper_used = 'Playwright'
                logger.info(f"LinkedIn Playwright scrape successful for {company}")
            else:
                logger.warning(f"LinkedIn Playwright scrape returned no results for {company}")
                results['errors'].append("All scrapers returned None")
        except Exception as e:
            logger.exception(f"Unexpected error in LinkedIn Playwright scrape for {company}: {e}")
            results['errors'].append(f"LinkedIn Playwright scrape: {e}")
    elif not posts_filepath and not use_playwright_fallback:
        logger.info(f"Playwright fallback disabled. Set USE_PLAYWRIGHT_FALLBACK=true to enable.")
        if not scraper_used:
            results['errors'].append("All enabled LinkedIn scrapers failed")

    if scraper_used:
        logger.info(f"LinkedIn scrape completed using: {scraper_used}")

    # Step 3.5: Scrape contact's LinkedIn posts
    contact_posts_filepath = None
    contact_summaries = None
    contact_name = None

    try:
        contact_mapping = load_contact_mapping()
        contact_name = contact_mapping.get(company)

        if contact_name:
            logger.info(f"Found primary contact for {company}: {contact_name}")

            contact_linkedin_url = get_contact_linkedin_url(contact_name, company)

            if contact_linkedin_url:
                contact_posts_filepath = scrape_contact_linkedin(contact_name, contact_linkedin_url, company)

                if contact_posts_filepath:
                    contact_summaries = summarize_contact_posts(contact_posts_filepath, contact_name)
                    if contact_summaries is not None:
                        results['contact_scrape'] = True
                        logger.info(f"Contact scrape successful for {contact_name} ({company}): {len(contact_summaries)} posts")
                    else:
                        logger.warning(f"Contact post summarization returned None for {contact_name}")
                else:
                    logger.warning(f"No LinkedIn posts found for contact {contact_name}")
            else:
                logger.warning(f"Could not find LinkedIn URL for contact {contact_name}")
        else:
            logger.info(f"No primary contact mapped for {company}")
    except Exception as e:
        logger.warning(f"Contact scrape failed for {company}: {e}")
        results['errors'].append(f"Contact scrape: {e}")

    # Step 4: Summarize and merge data (only if we have both files)
    if news_filepath and posts_filepath:
        try:
            summary_result = summarize_posts(news_filepath, posts_filepath)
            if summary_result is not None:
                results['summarization'] = True
                logger.info(f"Summarization successful for {company}")
            else:
                logger.warning(f"Summarization returned no results for {company}")
                results['errors'].append("Summarization returned None")
        except Exception as e:
            logger.exception(f"Unexpected error in summarization for {company}: {e}")
            results['errors'].append(f"Summarization: {e}")
    elif news_filepath:
        # No LinkedIn posts, but still generate reachout message and actions from news alone
        logger.info(f"No LinkedIn posts for {company} - generating actions from news only")
        try:
            with open(news_filepath, 'r', encoding='utf-8') as f:
                company_data = json.load(f)
            company_name = company_data.get('company', company)

            message = generate_reachout_message(company_name, [], company_data)
            potential_actions = generate_potential_actions(company_name, [], company_data)
            add_posts_to_news_file(news_filepath, [], message, potential_actions)
            results['summarization'] = True
        except Exception as e:
            logger.warning(f"Failed to generate actions from news for {company}: {e}")
            results['errors'].append(f"News-only actions: {e}")
    else:
        logger.info(f"Skipping summarization for {company} - no news data available")

    # Step 5: Ensure posts field exists in JSON (even if empty) and add linkedin_url
    if news_filepath:
        ensure_posts_field(news_filepath)
        add_linkedin_url(news_filepath, company_info)
        _add_contact_data_to_output(news_filepath, contact_name, contact_summaries)

    # Cleanup: Delete LinkedIn posts file after summarization
    try:
        if posts_filepath and os.path.exists(posts_filepath):
            os.remove(posts_filepath)
            logger.info(f"Deleted posts file: {posts_filepath}")
    except Exception as e:
        logger.warning(f"Error deleting {posts_filepath}: {e}")

    try:
        if contact_posts_filepath and os.path.exists(contact_posts_filepath):
            os.remove(contact_posts_filepath)
            logger.info(f"Deleted contact posts file: {contact_posts_filepath}")
    except Exception as e:
        logger.warning(f"Error deleting {contact_posts_filepath}: {e}")

    # Log summary for this company
    success_count = sum([results['company_info'], results['news_scrape'],
                         results['linkedin_scrape'], results['contact_scrape'], results['summarization']])
    logger.info(f"Completed scrape for {company}: {success_count}/5 steps successful")

    return results


def read_companies_from_csv(csv_path="data/input/companies.csv"):
    """
    Reads companies from a CSV file and returns a list of tuples.

    Args:
        csv_path: Path to the CSV file (relative to project root)

    Returns:
        List of tuples in format [(company_name, location), ...]
    """
    # Get absolute path to ensure it works from anywhere
    script_dir = os.path.dirname(os.path.abspath(__file__))
    full_path = os.path.join(script_dir, csv_path)

    companies = []

    try:
        with open(full_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                company = row.get('company', '').strip()
                location = row.get('location', '').strip()
                if company and location:
                    companies.append((company, location))

        logger.info(f"Loaded {len(companies)} companies from {csv_path}")
        return companies

    except FileNotFoundError:
        logger.error(f"CSV file not found at {full_path}")
        raise
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        raise

async def scrape_companies(companies_list, inter_delay=True):
    """
    Scrape a specific subset of companies with random 5-15 min delays between them.

    Args:
        companies_list: List of (company_name, location) tuples to scrape
        inter_delay: Whether to add random delays between companies

    Returns:
        list: Results for each company
    """
    all_results = []

    for idx, (company, location) in enumerate(companies_list):
        logger.info(f"{'=' * 50}")
        logger.info(f"Processing company {idx + 1}/{len(companies_list)}: {company}")
        logger.info(f"{'=' * 50}")

        try:
            result = await scrape(company, location)
            all_results.append(result)
        except Exception as e:
            logger.exception(f"Critical error processing {company}: {e}")
            all_results.append({
                'company': company,
                'location': location,
                'company_info': False,
                'news_scrape': False,
                'linkedin_scrape': False,
                'contact_scrape': False,
                'summarization': False,
                'errors': [f"Critical error: {e}"]
            })

        # Inter-company delay (skip after last company)
        if inter_delay and idx < len(companies_list) - 1:
            delay = random.randint(300, 900)
            logger.info(f"Waiting {delay // 60}m {delay % 60}s before next company...")
            await asyncio.sleep(delay)

    # Log summary
    logger.info("=" * 50)
    logger.info("SESSION SUMMARY")
    logger.info("=" * 50)
    successful = sum(1 for r in all_results if r['news_scrape'] or r['linkedin_scrape'])
    logger.info(f"Companies processed: {len(all_results)}, Successful: {successful}")

    return all_results


async def scrape_all_companies():
    """
    Scrape all companies in the list, continuing even if individual companies fail.

    Args:
        companies_list: List of (company_name, location) tuples

    Returns:
        list: Results for each company
    """
    all_results = []
    companies_list = read_companies_from_csv()

    for idx, (company, location) in enumerate(companies_list):
        logger.info(f"=" * 50)
        logger.info(f"Processing company {idx + 1}/{len(companies_list)}: {company}")
        logger.info(f"=" * 50)

        try:
            result = await scrape(company, location)
            all_results.append(result)
        except Exception as e:
            logger.exception(f"Critical error processing {company}, moving to next company: {e}")
            all_results.append({
                'company': company,
                'location': location,
                'company_info': False,
                'news_scrape': False,
                'linkedin_scrape': False,
                'contact_scrape': False,
                'summarization': False,
                'errors': [f"Critical error: {e}"]
            })

        # Inter-company delay to avoid API rate limits (skip after last company)
        if idx < len(companies_list) - 1:
            delay = 300 # delay by 5 minutes between each scrape
            logger.info(f"Waiting {delay}s before next company to avoid rate limits...")
            await asyncio.sleep(delay)

    # Print final summary
    logger.info("=" * 50)
    logger.info("FINAL SUMMARY")
    logger.info("=" * 50)

    successful = sum(1 for r in all_results if r['news_scrape'] or r['linkedin_scrape'])
    failed = len(all_results) - successful

    logger.info(f"Total companies processed: {len(all_results)}")
    logger.info(f"Successful (at least partial data): {successful}")
    logger.info(f"Failed (no data): {failed}")

    for result in all_results:
        status = "OK" if result['news_scrape'] or result['linkedin_scrape'] else "FAILED"
        logger.info(f"  - {result['company']}: {status}")
        if result['errors']:
            for error in result['errors']:
                logger.debug(f"      Error: {error}")

    return all_results


if __name__ == "__main__":
    # Read companies from CSV file
    """
    0 OnQ Software,Melbourne
    1 Axcelerate,Brisbane
    2 Pharmako Biotechnologies,Sydney
    """
    companies_list = read_companies_from_csv()

    # To scrape a single company (for testing):
    company, location = companies_list[0]
    asyncio.run(scrape(company, location))
    