import asyncio
import csv
import json
import logging
import os
from company.get_company_info import get_info
from scrapers.linkedin_scraper import scrape_news_linkedin
from summarizer import summarize_csv
from scrapers.perplexity_scraper import scrape_news_perplexity

logging.basicConfig(
    level=logging.INFO,  # change to DEBUG for more verbosity
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


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
        news_filepath = await scrape_news_perplexity(company_info, "year")
        if news_filepath:
            results['news_scrape'] = True
            logger.info(f"News scrape successful for {company}")
        else:
            logger.warning(f"News scrape returned no results for {company}")
            results['errors'].append("News scrape returned None")
    except Exception as e:
        logger.exception(f"Unexpected error in news scrape for {company}: {e}")
        results['errors'].append(f"News scrape: {e}")

    # Step 3: Scrape LinkedIn posts
    posts_filepath = None
    try:
        posts_filepath = await scrape_news_linkedin(company_info)
        if posts_filepath:
            results['linkedin_scrape'] = True
            logger.info(f"LinkedIn scrape successful for {company}")
        else:
            logger.warning(f"LinkedIn scrape returned no results for {company}")
            results['errors'].append("LinkedIn scrape returned None")
    except Exception as e:
        logger.exception(f"Unexpected error in LinkedIn scrape for {company}: {e}")
        results['errors'].append(f"LinkedIn scrape: {e}")

    # Step 4: Summarize and merge data (only if we have both files)
    if news_filepath and posts_filepath:
        try:
            summary_result = summarize_csv(news_filepath, posts_filepath)
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
        logger.info(f"Skipping summarization for {company} - no LinkedIn posts available")
    else:
        logger.info(f"Skipping summarization for {company} - no news data available")

    # Step 5: Ensure posts field exists in JSON (even if empty) and add linkedin_url
    if news_filepath:
        ensure_posts_field(news_filepath)
        add_linkedin_url(news_filepath, company_info)

    # Cleanup: Delete CSV files after summarization
    try:
        if posts_filepath and os.path.exists(posts_filepath):
            os.remove(posts_filepath)
            logger.info(f"Deleted CSV file: {posts_filepath}")
    except Exception as e:
        logger.warning(f"Error deleting {posts_filepath}: {e}")

    # Log summary for this company
    success_count = sum([results['company_info'], results['news_scrape'],
                         results['linkedin_scrape'], results['summarization']])
    logger.info(f"Completed scrape for {company}: {success_count}/4 steps successful")

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
                'summarization': False,
                'errors': [f"Critical error: {e}"]
            })

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
    