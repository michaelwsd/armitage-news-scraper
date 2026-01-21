import os
import json
import asyncio
import logging
from dotenv import load_dotenv
from perplexity import Perplexity
from datetime import datetime, timedelta

# -------------------------------------------------------------------
# Logging configuration
# -------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,  # change to DEBUG for more verbosity
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# Setup
# -------------------------------------------------------------------
load_dotenv()

client = Perplexity()

article_schema = {
    "type": "json_schema",
    "json_schema": {
        "schema": {
            "type": "object",
            "properties": {
                "company": {"type": "string"},
                "articles": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "headline": {"type": "string"},
                            "date": {
                                "type": "string",
                                "description": "Publish date of the article strictly in 'DD/MM/YYYY' format"
                            },
                            "summary": {"type": "string"},
                            "growth_type": {"type": "string"},
                            "source_url": {"type": "string"}
                        },
                        "required": ["headline", "date", "summary", "growth_type", "source_url"]
                    }
                }
            },
            "required": ["company", "articles"]
        }
    }
}

def parse_date(article):
    """
    Parses the date string in DD/MM/YYYY format. 
    If parsing fails, returns datetime.min so the article is sorted last.
    """
    date_str = article.get("date", "")
    try:
        # Changed from "%d %B %Y" to "%d/%m/%Y"
        return datetime.strptime(date_str, "%d/%m/%Y")
    except (ValueError, TypeError):
        logger.warning(f"Could not parse date: '{date_str}'. Sorting to end.")
        return datetime.min

async def scrape_news_perplexity(company_info, timeframe):
    company_name = company_info['name']
    company_city = company_info['city']
    company_hq_location = company_info['hq_location']
    company_website = company_info['website']
    company_industry = company_info['industry']

    start_date = None 
    now = datetime.now()

    if timeframe == "year":
        start_date = (now - timedelta(days=365)).strftime("%-m/%-d/%Y")
    elif timeframe == "month":
        start_date = (now - timedelta(days=30)).strftime("%-m/%-d/%Y")
    elif timeframe == "week":
        start_date = (now - timedelta(days=7)).strftime("%-m/%-d/%Y")
    elif timeframe == "day":
        start_date = (now - timedelta(days=1)).strftime("%-m/%-d/%Y")

    logger.info(f"Starting news pull for company={company_name}, location={company_city} after {start_date}")

    try:
        hq_sentence = (
                        f"{company_name} is currently headquartered at {company_hq_location}. "
                        if company_info.get("hq_location")
                        else ""
                    )

        user_prompt = (
            f"The company you will be finding news articles for is {company_name} located in {company_city}. "
            f"{hq_sentence}"
            f"They are primarily in the {company_industry.lower()} industries. "
            f"Find news articles indicating growth (awards, expansion, new hires, "
            f"partnerships, patents, financial success, etc) for {company_name}. "
            "Only return news for this specific company and location, do not confuse it with other companies with similar names."
        )

        logger.info(f"User prompt: {user_prompt}")

        logger.info("Sending request to Perplexity model")
        domains = [company_website, 
                   "afr.com", 
                   "insidesmallbusiness.com.au", 
                   "dynamicbusiness.com",
                   "smartcompany.com.au",
                   "startupdaily.net",
                   "businessnews.com.au",
                  ]
        
        for domain in domains:
            logger.info(f"Scraping {domain}")

        response = client.chat.completions.create(
                        messages=[
                            {
                                "role": "user",
                                "content": user_prompt
                            }
                        ],
                        model="sonar-pro",
                        web_search_options={
                            "search_domain_filter": domains,
                            "search_after_date": start_date,
                            "user_location": {
                                                "country": "AU",
                                                "city": company_city,
                                             }
                        },
                        response_format=article_schema
                    )

        content = response.choices[0].message.content
        data = json.loads(content)

        data["articles"] = sorted(
            data["articles"],
            key=parse_date,
            reverse=True
        )

        logger.info(
            "Successfully retrieved %d articles for %s",
            len(data["articles"]),
            company_name
        )

        if data:
            # 1. Get the project root directory (parent of 'scrapers' folder)
            script_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(script_dir)
            output_dir = os.path.join(project_root, "data", "output")

            # 2. Create the 'data/output' directory if it doesn't exist
            os.makedirs(output_dir, exist_ok=True)

            # 3. Construct the filename (e.g., "data/output/LAB Group.json")
            # Using .get("company") ensures we use the exact name returned by the AI
            filename = os.path.join(output_dir, f"{data.get('company', company_name)}.json")

            # 4. Save the result
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)

            logger.info(f"Result saved to {filename}")

        return data

    except Exception:
        logger.exception("Failed to pull news for %s", company_name)
        raise  # re-raise so callers can handle it

# -------------------------------------------------------------------
# Entrypoint
# -------------------------------------------------------------------
if __name__ == "__main__":
    # single scrape
    company_info = {
        'hq_location': '201 Kent St, Level 14, Sydney, NSW, 2000, AU', 
        'linkedin': 'grc-solutions-pty-ltd', 
        'industry': 'E-learning and online education', 
        'website': 'grc-solutions.com', 
        'name': 'GRC Solutions', 
        'city': 'Sydney'
        }
    
    data = asyncio.run(scrape_news_perplexity(company_info, "year"))
    print(json.dumps(data, indent=2))