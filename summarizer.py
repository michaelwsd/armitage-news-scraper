import os
import csv
import json
import logging
from dotenv import load_dotenv
from openai import OpenAI
from datetime import datetime, timedelta
import re

# -------------------------------------------------------------------
# Logging configuration
# -------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# Setup
# -------------------------------------------------------------------
load_dotenv()
client = OpenAI()

# Define the schema for batch LinkedIn post analysis
posts_batch_schema = {
    "type": "json_schema",
    "json_schema": {
        "name": "linkedin_posts_batch_analysis",
        "schema": {
            "type": "object",
            "properties": {
                "posts": {
                    "type": "array",
                    "description": "Array of analyzed posts",
                    "items": {
                        "type": "object",
                        "properties": {
                            "post_index": {
                                "type": "integer",
                                "description": "Index of the post in the original list (0-based)"
                            },
                            "is_growth_indicator": {
                                "type": "boolean",
                                "description": "Whether this post indicates company growth"
                            },
                            "summary": {
                                "type": "string",
                                "description": "Brief summary of the post content"
                            },
                            "growth_type": {
                                "type": "string",
                                "description": "Type of growth: awards, expansion, new hires, partnerships, patents, financial success, product launch, etc. Empty string if not a growth indicator."
                            },
                            "date": {
                                "type": "string",
                                "description": "Date from the post"
                            }
                        },
                        "required": ["post_index", "is_growth_indicator", "summary", "growth_type", "date"],
                        "additionalProperties": False
                    }
                }
            },
            "required": ["posts"],
            "additionalProperties": False
        },
        "strict": True
    }
}

def parse_csv(filepath):
    "parse the csv content in data/output/filename"

    if not os.path.exists(filepath):
        raise FileNotFoundError(f"CSV file not found: {filepath}")

    with open(filepath, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        data = list(reader)

    return data


def analyze_posts_batch_with_openai(posts):
    """
    Analyze multiple LinkedIn posts at once using OpenAI to determine which indicate growth.
    Returns a list of structured JSON objects with summary, growth_type, and date.
    """
    try:
        # Build the batch prompt with all posts
        posts_text = ""
        for i, post in enumerate(posts):
            posts_text += f"""
                            Post #{i}:
                            - Date: {post['Date']}
                            - Likes: {post['Likes']}
                            - Content: {post['Content']}

                            """

        user_prompt = f"""
        Analyze these LinkedIn posts and determine which ones indicate company growth.

        Growth indicators include:
        - Awards and recognition
        - Business expansion
        - New hires or team growth
        - Partnerships or collaborations
        - Patents or innovations
        - Financial success or funding
        - Product launches or major updates
        - Market expansion
        - Client acquisitions

        For each post, determine if it indicates growth.
        Provide a brief summary, identify the growth type, and extract the date.

        {posts_text}

        Analyze all {len(posts)} posts above.
        """

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "You are an expert business analyst who identifies company growth indicators from social media posts."
                },
                {
                    "role": "user",
                    "content": user_prompt
                }
            ],
            response_format=posts_batch_schema
        )

        result = json.loads(response.choices[0].message.content)
        logger.info(f"Analyzed {len(result['posts'])} posts in batch")

        return result['posts']

    except Exception as e:
        logger.exception(f"Failed to analyze posts batch: {e}")
        raise


def convert_relative_date_to_absolute(relative_date):
    """
    Convert relative date strings (e.g., '1d', '2w', '3mo') to absolute dates in DD/MM/YYYY format.
    """
    today = datetime.now()

    # Pattern to match relative dates like '1d', '2w', '3mo', '4y'
    match = re.match(r'(\d+)(d|w|mo|y)', relative_date.lower().strip())

    if not match:
        logger.warning(f"Could not parse relative date: {relative_date}")
        return relative_date  # Return as-is if can't parse

    amount = int(match.group(1))
    unit = match.group(2)

    if unit == 'd':
        target_date = today - timedelta(days=amount)
    elif unit == 'w':
        target_date = today - timedelta(weeks=amount)
    elif unit == 'mo':
        # Approximate months as 30 days
        target_date = today - timedelta(days=amount * 30)
    elif unit == 'y':
        target_date = today - timedelta(days=amount * 365)
    else:
        logger.warning(f"Unknown unit in relative date: {relative_date}")
        return relative_date

    # Return in DD/MM/YYYY format to match the perplexity scraper format
    return target_date.strftime("%d/%m/%Y")


def parse_date_for_sorting(date_str):
    """
    Parse date string in DD/MM/YYYY format for sorting.
    Returns datetime object, or datetime.min if parsing fails.
    """
    try:
        return datetime.strptime(date_str, "%d/%m/%Y")
    except (ValueError, TypeError):
        logger.warning(f"Could not parse date for sorting: '{date_str}'. Sorting to end.")
        return datetime.min


def add_posts_to_news_file(news_filepath, posts_data):
    """
    Add the analyzed posts to the news JSON file under a 'posts' field.
    """
    try:
        # Read existing news file
        with open(news_filepath, 'r', encoding='utf-8') as f:
            news_data = json.load(f)

        # Add posts field
        news_data['posts'] = posts_data

        # Write back to file
        with open(news_filepath, 'w', encoding='utf-8') as f:
            json.dump(news_data, f, indent=2)

        logger.info(f"Successfully added {len(posts_data)} posts to {news_filepath}")

    except Exception as e:
        logger.exception(f"Failed to add posts to news file: {e}")
        raise


def summarize_csv(news_filepath, posts_filepath):
    """
    Main function to process LinkedIn posts CSV and add growth indicators to news file.

    Args:
        news_filepath: Path to the company news JSON file (e.g., "data/output/OnQ Software.json")
        posts_filepath: Path to the LinkedIn posts CSV file (e.g., "data/output/OnQ Software Linkedin Posts.csv")
    """
    logger.info(f"Processing posts from {posts_filepath}")

    # Parse CSV
    posts = parse_csv(posts_filepath)
    logger.info(f"Found {len(posts)} posts in CSV")

    # Analyze all posts in one batch API call
    logger.info(f"Analyzing all {len(posts)} posts in a single API call")
    analyzed_posts = analyze_posts_batch_with_openai(posts)

    # Filter for growth indicators only and convert dates
    growth_posts = []
    for analysis in analyzed_posts:
        if analysis['is_growth_indicator']:
            relative_date = analysis['date']
            absolute_date = convert_relative_date_to_absolute(relative_date)

            growth_posts.append({
                "summary": analysis['summary'],
                "growth_type": analysis['growth_type'] + " - " + relative_date,
                "date": absolute_date
            })
            logger.info(f"Growth indicator found: {analysis['growth_type']} - {relative_date} -> {absolute_date}")

    logger.info(f"Found {len(growth_posts)} growth indicator posts out of {len(posts)} total posts")

    # Sort posts chronologically (latest first)
    growth_posts.sort(key=lambda x: parse_date_for_sorting(x['date']), reverse=True)
    logger.info("Sorted posts chronologically (latest first)")

    # Add to news file
    add_posts_to_news_file(news_filepath, growth_posts)

    logger.info("Processing complete!")
    return growth_posts


if __name__ == "__main__":
    # Example usage
    summarize_csv(
        news_filepath="data/output/OnQ Software.json",
        posts_filepath="data/output/OnQ Software Linkedin Posts.csv"
    )
