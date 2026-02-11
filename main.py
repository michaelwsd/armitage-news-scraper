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

def run(
    recipients: list[str] = None,
    send_digest: bool = True,
    company: str = None,
    scrape_only: bool = False,
    deliver_only: bool = False,
    batch: str = None,
    limit: int = None,
):
    """
    Run the full scraping and email pipeline.

    Args:
        recipients: List of email addresses to send reports to.
        send_digest: If True, send one digest email. If False, send individual emails per company.
        company: If provided, only process this single company.
        scrape_only: If True, import + scrape only — skip push, email, cleanup.
        deliver_only: If True, push + email + cleanup only — skip import + scrape.
        batch: Batch spec like "1/4" meaning "batch 1 of 4".
        limit: If provided, only process the first N companies from the list.
    """
    # ── Scrape phase ──
    if not deliver_only:
        if company:
            logger.info(f"Single-company mode: {company}")
            import_companies_from_salesforce()
            companies = read_companies_from_csv()
            match = [(name, loc) for name, loc in companies if name.lower() == company.lower()]
            if not match:
                logger.error(f"Company '{company}' not found in companies.csv")
                return
            logger.info(f"Found: {match[0][0]} in {match[0][1]}")
            asyncio.run(scrape_companies(match, inter_delay=False))
        elif batch:
            batch_num, total_batches = _parse_batch(batch)
            if not scrape_only:
                import_companies_from_salesforce()
            companies = read_companies_from_csv()
            if limit:
                companies = companies[:limit]
                logger.info(f"Limited to first {limit} companies")
            chunk = _get_batch_slice(companies, batch_num, total_batches)
            logger.info(f"Batch {batch_num}/{total_batches}: processing {len(chunk)} of {len(companies)} companies")
            for name, loc in chunk:
                logger.info(f"  - {name}")
            asyncio.run(scrape_companies(chunk))
        else:
            import_companies_from_salesforce()
            companies = read_companies_from_csv()
            if limit:
                companies = companies[:limit]
                logger.info(f"Limited to first {limit} companies")
            asyncio.run(scrape_companies(companies))

    if scrape_only:
        logger.info("Scrape-only mode: skipping push, email, and cleanup")
        return

    # ── Deliver phase ──
    push_to_salesforce()

    if send_digest:
        send_owner_digests(fallback_recipients=recipients)
    elif recipients:
        send_all_reports(recipients)
    else:
        logger.warning("No recipients configured, pass recipients to run().")

    cleanup()


def _parse_batch(batch_str: str) -> tuple[int, int]:
    """Parse '1/4' into (1, 4)."""
    parts = batch_str.split("/")
    if len(parts) != 2:
        raise ValueError(f"Invalid batch format '{batch_str}', expected 'N/M' (e.g. '1/4')")
    batch_num, total = int(parts[0]), int(parts[1])
    if batch_num < 1 or batch_num > total:
        raise ValueError(f"Batch number must be between 1 and {total}, got {batch_num}")
    return batch_num, total


def _get_batch_slice(companies: list, batch_num: int, total_batches: int, batch_size: int = 5) -> list:
    """Return a fixed-size slice of companies for the given batch number (1-indexed).

    Each batch contains exactly `batch_size` companies, except the last batch
    which may contain fewer.
    """
    start = (batch_num - 1) * batch_size
    end = start + batch_size
    return companies[start:end]


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
        "--batch",
        type=str,
        help="Process a batch of companies, e.g. '1/4' for batch 1 of 4",
    )
    parser.add_argument(
        "--scrape-only",
        action="store_true",
        help="Import and scrape only — skip push, email, and cleanup. Output stays in data/output/",
    )
    parser.add_argument(
        "--deliver-only",
        action="store_true",
        help="Push + email + cleanup only — use existing output files, skip scraping",
    )
    parser.add_argument(
        "--no-email",
        action="store_true",
        help="Skip sending emails (useful for testing)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Only process the first N companies (useful for testing)",
    )
    args = parser.parse_args()

    if args.scrape_only and args.deliver_only:
        parser.error("Cannot use --scrape-only and --deliver-only together")

    if args.no_email:
        run(
            company=args.company,
            send_digest=False,
            scrape_only=args.scrape_only,
            deliver_only=args.deliver_only,
            batch=args.batch,
            limit=args.limit,
        )
    else:
        run(
            ["mwan0165@student.monash.edu"],
            company=args.company,
            scrape_only=args.scrape_only,
            deliver_only=args.deliver_only,
            batch=args.batch,
            limit=args.limit,
        )
