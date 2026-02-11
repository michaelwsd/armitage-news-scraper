import argparse
import asyncio
import logging
from pathlib import Path
from scraper import scrape_all_companies, scrape_companies, read_companies_from_csv
from salesforce import import_companies_from_salesforce, push_to_salesforce
from utils.email_client import send_all_reports, send_owner_digests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

def run(recipients: list[str] = None, send_digest: bool = True, company: str = None):
    """
    Run the full scraping and email pipeline.

    Args:
        recipients: List of email addresses to send reports to.
                   Falls back to EMAIL_RECIPIENTS env var (comma-separated).
        send_digest: If True, send one digest email. If False, send individual emails per company.
        company: If provided, only process this single company (skips Salesforce import, uses CSV lookup).
    """
    if company:
        # Single-company test mode: look up from CSV, skip full Salesforce import
        logger.info(f"Single-company mode: {company}")
        import_companies_from_salesforce()
        companies = read_companies_from_csv()
        match = [(name, loc) for name, loc in companies if name.lower() == company.lower()]
        if not match:
            logger.error(f"Company '{company}' not found in companies.csv")
            return
        logger.info(f"Found: {match[0][0]} in {match[0][1]}")
        asyncio.run(scrape_companies(match, inter_delay=False))
    else:
        # 1. get the list of companies from salesforce
        import_companies_from_salesforce()
        # 2. run scrape function to scrape all companies
        asyncio.run(scrape_all_companies())

    # 3. push result json to salesforce
    push_to_salesforce()

    # 4. send emails — per-owner digests with fallback to recipients
    if send_digest:
        send_owner_digests(fallback_recipients=recipients)
    elif recipients:
        send_all_reports(recipients)
    else:
        logger.warning("No recipients configured, pass recipients to run().")

    # clean up files
    cleanup()


def cleanup(input_dir: str = "data/input", output_dir: str = "data/output"):
    """Delete all files from data/input and data/output directories."""
    base = Path(__file__).parent
    for dir_path in (base / input_dir, base / output_dir):
        if not dir_path.exists():
            continue
        for file in dir_path.iterdir():
            if file.is_file():
                file.unlink()
                logger.info(f"Deleted {file}")
    logger.info("Cleanup complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Armitage automation pipeline")
    parser.add_argument(
        "--company",
        type=str,
        help="Run pipeline for a single company (must match a name in companies.csv)",
    )
    parser.add_argument(
        "--no-email",
        action="store_true",
        help="Skip sending emails (useful for testing)",
    )
    args = parser.parse_args()

    if args.no_email:
        run(company=args.company, send_digest=False)
    else:
        run(["mwan0165@student.monash.edu"], company=args.company)