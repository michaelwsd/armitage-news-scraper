import asyncio
import csv
import logging
import os
from company.get_company_info import get_info
from scrapers.linkedin_scraper import scrape_news_linkedin
from scrapers.perplexity_scraper import scrape_news_perplexity

logging.basicConfig(
    level=logging.INFO,  # change to DEBUG for more verbosity
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

async def scrape(company, location):
    company_info = get_info(company, location)

    logger.debug("Retrieved company info: %s", company_info)
    
    await scrape_news_perplexity(company_info, "year")
    await scrape_news_linkedin(company_info)

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

if __name__ == "__main__":
    # Read companies from CSV file
    companies_list = read_companies_from_csv()
    company, location = companies_list[3]

    asyncio.run(scrape(company, location))

    