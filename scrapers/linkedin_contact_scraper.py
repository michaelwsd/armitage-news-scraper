import os
import json
import logging
import time
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


def scrape_contact_linkedin(contact_name, linkedin_url, company_name):
    """
    Scrape LinkedIn posts for an individual contact using BrightData's API.

    Args:
        contact_name: Name of the contact person
        linkedin_url: Full LinkedIn profile URL (e.g. "https://www.linkedin.com/in/nick-gannoulis-2a94991/")
        company_name: Company name (used for output file naming)

    Returns:
        str: Path to output JSON file on success
        None: On any failure
    """
    if not linkedin_url:
        logger.warning(f"No LinkedIn URL for contact {contact_name}, skipping")
        return None

    api_key = os.getenv('BRIGHTDATA_API_KEY')
    if not api_key:
        logger.error("BRIGHTDATA_API_KEY not found in environment variables")
        return None

    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    start_date_str = start_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    end_date_str = end_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    logger.info(f"Scraping contact LinkedIn posts for {contact_name} ({company_name}) from {start_date_str} to {end_date_str}")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    output_dir = os.path.join(project_root, "data", "output")
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"{company_name} Contact Posts.json")

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    data = json.dumps({
        "input": [{
            "url": linkedin_url,
            "start_date": start_date_str,
            "end_date": end_date_str,
        }],
    })

    try:
        # Step 1: Trigger the scrape
        logger.info(f"Triggering BrightData profile scrape for {contact_name}...")
        response = requests.post(
            "https://api.brightdata.com/datasets/v3/trigger"
            "?dataset_id=gd_lyy3tktm25m4avu764"
            "&custom_output_fields=title%2Cpost_text%2Cdate_posted"
            "&notify=false&type=discover_new&discover_by=profile_url",
            headers=headers,
            data=data,
        )

        if not response.ok:
            logger.error(f"API error {response.status_code}: {response.text[:500]}")
            response.raise_for_status()

        snapshot_id = response.json().get("snapshot_id")
        if not snapshot_id:
            logger.error(f"No snapshot_id in trigger response: {response.text[:300]}")
            return None

        logger.info(f"Scrape triggered, snapshot_id: {snapshot_id}")

        # Step 2: Poll for completion
        poll_url = f"https://api.brightdata.com/datasets/v3/progress/{snapshot_id}"
        max_wait = 1800
        poll_interval = 60
        elapsed = 0

        while elapsed < max_wait:
            time.sleep(poll_interval)
            elapsed += poll_interval

            progress_resp = requests.get(poll_url, headers={"Authorization": f"Bearer {api_key}"})
            if not progress_resp.ok:
                logger.warning(f"Progress check failed ({progress_resp.status_code}): {progress_resp.text[:200]}")
                continue

            status = progress_resp.json().get("status")
            logger.info(f"Snapshot {snapshot_id} status: {status} (waited {elapsed}s)")

            if status == "ready":
                break
            elif status == "failed":
                logger.error(f"Snapshot failed: {progress_resp.text[:300]}")
                return None
        else:
            logger.error(f"Snapshot {snapshot_id} did not complete within {max_wait}s")
            return None

        # Step 3: Download the snapshot
        logger.info(f"Downloading snapshot {snapshot_id}...")
        download_resp = requests.get(
            f"https://api.brightdata.com/datasets/v3/snapshot/{snapshot_id}?format=json",
            headers={"Authorization": f"Bearer {api_key}"},
        )

        if not download_resp.ok:
            logger.error(f"Download failed ({download_resp.status_code}): {download_resp.text[:500]}")
            return None

        response_text = download_resp.text.strip()
        logger.info(f"Download status: {download_resp.status_code}, length: {len(response_text)} characters")

        parsed = json.loads(response_text)
        if isinstance(parsed, list):
            posts_data = [obj for obj in parsed if isinstance(obj, dict) and 'post_text' in obj]
        elif isinstance(parsed, dict) and 'post_text' in parsed:
            posts_data = [parsed]
        else:
            posts_data = []

        if not posts_data:
            logger.warning(f"No posts found for contact {contact_name}")
            return None

        logger.info(f"Collected {len(posts_data)} posts for {contact_name}")

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(posts_data, f, indent=2, ensure_ascii=False)

        logger.info(f"Successfully saved {len(posts_data)} contact posts to {output_file}")
        return output_file

    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed for contact {contact_name}: {e}")
        return None
    except Exception as e:
        logger.exception(f"Contact LinkedIn scraper failed for {contact_name}: {e}")
        return None


if __name__ == "__main__":
    result = scrape_contact_linkedin(
        contact_name="Nick Gannoulis",
        linkedin_url="https://www.linkedin.com/in/nick-gannoulis-2a94991/",
        company_name="OnQ Software",
    )
    if result:
        print(f"Successfully scraped contact posts to: {result}")
    else:
        print("Scraping failed")
