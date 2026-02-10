import asyncio
import logging
from pathlib import Path
from scraper import scrape_all_companies
from salesforce import import_companies_from_salesforce
from utils.email_client import send_all_reports, send_owner_digests

logging.basicConfig(
    level=logging.INFO,  
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

def run(recipients: list[str] = None, send_digest: bool = True):
    """
    Run the full scraping and email pipeline.

    Args:
        recipients: List of email addresses to send reports to.
                   Falls back to EMAIL_RECIPIENTS env var (comma-separated).
        send_digest: If True, send one digest email. If False, send individual emails per company.
    """
    # 1. get the list of companies from salesforce
    import_companies_from_salesforce()

    # 2. run scrape function to scrape all companies
    asyncio.run(scrape_all_companies())

    # 3. push result json to salesforce dashboard

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
    run(["mwan0165@student.monash.edu"])