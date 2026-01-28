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
        return []  # Return empty list to allow workflow to continue


def convert_relative_date_to_absolute(relative_date):
    """
    Convert relative date strings (e.g., '1h', '1d', '2w', '3mo') to absolute dates in DD/MM/YYYY format.
    """
    today = datetime.now()

    # Pattern to match relative dates like '1h', '1d', '2w', '3mo', '4y'
    match = re.match(r'(\d+)(h|d|w|mo|y)', relative_date.lower().strip())

    if not match:
        logger.warning(f"Could not parse relative date: {relative_date}")
        return relative_date  # Return as-is if can't parse

    amount = int(match.group(1))
    unit = match.group(2)

    if unit == 'h':
        target_date = today - timedelta(hours=amount)
    elif unit == 'd':
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
    Handles dates with relative time tags (e.g., '20/01/2026 - 3d').
    Returns datetime object, or datetime.min if parsing fails.
    """
    try:
        # Extract just the date part before the " - " separator
        date_part = date_str.split(' - ')[0].strip()
        return datetime.strptime(date_part, "%d/%m/%Y")
    except (ValueError, TypeError, IndexError):
        logger.warning(f"Could not parse date for sorting: '{date_str}'. Sorting to end.")
        return datetime.min


def generate_reachout_message(company_name, growth_posts):
    """
    Generate a short professional LinkedIn reachout message based on filtered growth posts.
    Returns the message string, or an empty string on failure.
    """
    logger.info(f"Generating LinkedIn reachout message for {company_name} based on {len(growth_posts)} growth posts")

    if not growth_posts:
        logger.warning(f"No growth posts available for {company_name}, skipping reachout message")
        return ""

    posts_summary = "\n".join(
        f"- [{p.get('growth_type', 'growth')}] {p.get('summary', '')}"
        for p in growth_posts
    )

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a professional business development specialist. "
                        "Write short, warm LinkedIn reachout messages that reference "
                        "specific company achievements. Keep it under 100 words, "
                        "conversational, and end with a soft call-to-action."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Write a short LinkedIn reachout message to someone at {company_name}. "
                        f"Reference these recent growth signals:\n{posts_summary}\n\n"
                        "The message should congratulate them on their growth, briefly mention "
                        "one or two specific achievements, and suggest a conversation. "
                        "Do not include a subject line. Just the message body."
                    ),
                },
            ],
        )
        message = response.choices[0].message.content.strip()
        logger.info(f"Generated reachout message for {company_name}")
        return message
    except Exception as e:
        logger.exception(f"Failed to generate reachout message: {e}")
        return ""


def add_posts_to_news_file(news_filepath, posts_data, message=""):
    """
    Add the analyzed posts to the news JSON file under a 'posts' field.
    """
    try:
        # Read existing news file
        with open(news_filepath, 'r', encoding='utf-8') as f:
            news_data = json.load(f)

        # Add posts and message fields
        news_data['posts'] = posts_data
        news_data['message'] = message

        # Write back to file
        with open(news_filepath, 'w', encoding='utf-8') as f:
            json.dump(news_data, f, indent=2)

        logger.info(f"Successfully added {len(posts_data)} posts to {news_filepath}")

    except Exception as e:
        logger.exception(f"Failed to add posts to news file: {e}")
        return False
    return True


def summarize_csv(news_filepath, posts_filepath):
    """
    Main function to process LinkedIn posts CSV and add growth indicators to news file.

    Args:
        news_filepath: Path to the company news JSON file (e.g., "data/output/OnQ Software.json")
        posts_filepath: Path to the LinkedIn posts CSV file (e.g., "data/output/OnQ Software Linkedin Posts.csv")

    Returns:
        list: Growth posts on success
        None: On failure or if inputs are missing
    """
    # Handle missing inputs gracefully
    if not news_filepath:
        logger.warning("No news filepath provided, skipping summarization")
        return None

    if not posts_filepath:
        logger.warning("No posts filepath provided, skipping LinkedIn post analysis")
        return None

    if not os.path.exists(posts_filepath):
        logger.warning(f"Posts CSV file not found: {posts_filepath}, skipping LinkedIn post analysis")
        return None

    if not os.path.exists(news_filepath):
        logger.warning(f"News JSON file not found: {news_filepath}, cannot add posts")
        return None

    logger.info(f"Processing posts from {posts_filepath}")

    try:
        # Parse CSV
        posts = parse_csv(posts_filepath)
        logger.info(f"Found {len(posts)} posts in CSV")

        if not posts:
            logger.warning("No posts found in CSV, skipping analysis")
            return []

        # Analyze all posts in one batch API call
        logger.info(f"Analyzing all {len(posts)} posts in a single API call")
        analyzed_posts = analyze_posts_batch_with_openai(posts)

        if not analyzed_posts:
            logger.warning("Post analysis returned no results")
            return []

        # Filter for growth indicators only and convert dates
        growth_posts = []
        for analysis in analyzed_posts:
            if analysis.get('is_growth_indicator'):
                relative_date = analysis.get('date', 'Unknown')
                absolute_date = convert_relative_date_to_absolute(relative_date)

                growth_posts.append({
                    "summary": analysis.get('summary', ''),
                    "growth_type": analysis.get('growth_type', ''),
                    "date": absolute_date + " - " + relative_date
                })
                logger.info(f"Growth indicator found: {analysis.get('growth_type')} - {relative_date} -> {absolute_date}")

        logger.info(f"Found {len(growth_posts)} growth indicator posts out of {len(posts)} total posts")

        # Sort posts chronologically (latest first)
        growth_posts.sort(key=lambda x: parse_date_for_sorting(x['date']), reverse=True)
        logger.info("Sorted posts chronologically (latest first)")

        # Generate LinkedIn reachout message from growth posts
        with open(news_filepath, 'r', encoding='utf-8') as f:
            company_name = json.load(f).get('company', 'the company')
        message = generate_reachout_message(company_name, growth_posts)

        # Add to news file
        add_posts_to_news_file(news_filepath, growth_posts, message)

        logger.info("Processing complete!")
        return growth_posts

    except FileNotFoundError as e:
        logger.error(f"File not found during summarization: {e}")
        return None
    except Exception as e:
        logger.exception(f"Error during summarization: {e}")
        return None


if __name__ == "__main__":
    # Example usage
    summarize_csv(
        news_filepath="data/output/GRC Solutions.json",
        posts_filepath="data/output/GRC Solutions Linkedin Posts.csv"
    )
