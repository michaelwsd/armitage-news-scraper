import os
import logging
import serpapi
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

load_dotenv()
API_KEY = os.getenv("SERP_API_KEY")


def get_contact_linkedin_url(contact_name, company_name):
    """
    Search Google for a person's LinkedIn profile URL.

    Args:
        contact_name: Full name of the contact (e.g. "Nick Gannoulis")
        company_name: Company name for disambiguation (e.g. "OnQ Software")

    Returns:
        str: LinkedIn profile URL on success
        None: On any failure
    """
    params = {
        "engine": "google",
        "location": "Australia",
        "google_domain": "google.com.au",
        "hl": "en",
        "gl": "au",
        "q": f"{contact_name} {company_name} LinkedIn",
        "api_key": API_KEY,
    }

    try:
        client = serpapi.Client(api_key=params["api_key"])
        results = client.search(params)

        if not results.get("organic_results"):
            logger.warning(f"No search results for contact {contact_name} at {company_name}")
            return None

        for result in results["organic_results"][:5]:
            link = result.get("link", "")
            if "linkedin.com/in/" in link:
                logger.info(f"Found LinkedIn URL for {contact_name}: {link}")
                return link

        logger.warning(f"No LinkedIn profile URL found in top results for {contact_name}")
        return None

    except Exception as e:
        logger.exception(f"SERP API error searching for {contact_name}: {e}")
        return None


if __name__ == "__main__":
    print(get_contact_linkedin_url("Nick Gannoulis", "OnQ Software"))
