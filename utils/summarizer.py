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

contact_posts_schema = {
    "type": "json_schema",
    "json_schema": {
        "name": "contact_posts_analysis",
        "schema": {
            "type": "object",
            "properties": {
                "posts": {
                    "type": "array",
                    "description": "Array of summarized contact posts",
                    "items": {
                        "type": "object",
                        "properties": {
                            "summary": {
                                "type": "string",
                                "description": "Brief one-sentence summary of what the person posted about"
                            },
                            "date": {
                                "type": "string",
                                "description": "Date of the post in DD/MM/YYYY format"
                            },
                            "topic": {
                                "type": "string",
                                "description": "Topic category: industry insight, company update, personal achievement, thought leadership, event, hiring, other"
                            }
                        },
                        "required": ["summary", "date", "topic"],
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


def summarize_contact_posts(contact_posts_filepath, contact_name):
    """
    Summarize a contact's LinkedIn posts into dot-point summaries.

    Args:
        contact_posts_filepath: Path to the contact posts JSON file
        contact_name: Name of the contact person

    Returns:
        list: List of dicts with 'summary', 'date', 'topic' keys
        None: On failure
    """
    if not contact_posts_filepath or not os.path.exists(contact_posts_filepath):
        logger.warning(f"Contact posts file not found: {contact_posts_filepath}")
        return None

    try:
        posts = parse_posts_file(contact_posts_filepath)
        if not posts:
            logger.warning(f"No contact posts found for {contact_name}")
            return []

        logger.info(f"Summarizing {len(posts)} contact posts for {contact_name}")

        posts_text = ""
        for i, post in enumerate(posts):
            posts_text += f"Post #{i}:\n- Date: {post['Date']}\n- Content: {post['Content']}\n\n"

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are an analyst summarizing a person's LinkedIn activity. "
                        "For each post, provide a brief one-sentence summary of what they posted about, "
                        "the date in DD/MM/YYYY format, and a topic category."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Summarize these LinkedIn posts by {contact_name}. "
                        f"Provide a brief dot-point summary for each post.\n\n{posts_text}"
                    ),
                },
            ],
            response_format=contact_posts_schema,
        )

        result = json.loads(response.choices[0].message.content)
        summaries = result.get("posts", [])

        for s in summaries:
            date_str = s.get("date", "Unknown")
            if date_str and '/' in date_str and len(date_str) == 10:
                absolute_date = date_str
                relative_date = calculate_relative_date(absolute_date)
            else:
                relative_date = date_str
                absolute_date = convert_relative_date_to_absolute(relative_date)
            s["date"] = absolute_date + " - " + relative_date

        summaries.sort(key=lambda x: parse_date_for_sorting(x['date']), reverse=True)

        logger.info(f"Summarized {len(summaries)} contact posts for {contact_name}")
        return summaries

    except Exception as e:
        logger.exception(f"Failed to summarize contact posts for {contact_name}: {e}")
        return None


def parse_posts_file(filepath):
    """
    Parse posts from either JSON or CSV format.
    Returns a list of dicts with keys: Date, Likes, Content
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Posts file not found: {filepath}")

    # Determine file type by extension
    file_ext = os.path.splitext(filepath)[1].lower()

    if file_ext == '.json':
        # Parse JSON format (from API scraper)
        logger.info(f"Parsing JSON posts file: {filepath}")
        with open(filepath, 'r', encoding='utf-8') as file:
            json_data = json.load(file)

        # Convert JSON format to expected format
        data = []
        for post in json_data:
            # Extract date from date_posted field (ISO format)
            date_posted = post.get('date_posted', 'Unknown')
            if date_posted and date_posted != 'Unknown':
                try:
                    # Parse ISO date and format as DD/MM/YYYY (matching Perplexity format)
                    dt = datetime.fromisoformat(date_posted.replace('Z', '+00:00'))
                    formatted_date = dt.strftime("%d/%m/%Y")
                except Exception:
                    formatted_date = date_posted
            else:
                formatted_date = 'Unknown'

            data.append({
                'Date': formatted_date,
                'Likes': '0',  # API doesn't provide likes
                'Content': post.get('post_text', 'No Text')
            })

        return data

    elif file_ext == '.csv':
        # Parse CSV format (from Playwright scraper)
        logger.info(f"Parsing CSV posts file: {filepath}")
        with open(filepath, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            data = list(reader)
        return data

    else:
        raise ValueError(f"Unsupported file format: {file_ext}. Expected .json or .csv")


# Keep backward compatibility
def parse_csv(filepath):
    """Backward compatibility wrapper for parse_posts_file"""
    return parse_posts_file(filepath)


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


def calculate_relative_date(absolute_date_str):
    """
    Calculate relative date format (e.g., "2w", "1mo") from absolute date.

    Args:
        absolute_date_str: Date string in YYYY-MM-DD or DD/MM/YYYY format

    Returns:
        Relative date string like "1d", "2w", "1mo", "3y" or the original if parsing fails
    """
    try:
        # Try parsing YYYY-MM-DD format first
        try:
            date_obj = datetime.strptime(absolute_date_str, "%Y-%m-%d")
        except ValueError:
            # Try DD/MM/YYYY format
            date_obj = datetime.strptime(absolute_date_str, "%d/%m/%Y")

        # Calculate difference from today
        today = datetime.now()
        delta = today - date_obj

        days = delta.days

        if days < 0:
            # Future date
            return absolute_date_str
        elif days == 0:
            return "today"
        elif days == 1:
            return "1d"
        elif days < 7:
            return f"{days}d"
        elif days < 30:
            weeks = days // 7
            return f"{weeks}w"
        elif days < 365:
            months = days // 30
            return f"{months}mo"
        else:
            years = days // 365
            return f"{years}y"
    except (ValueError, TypeError):
        return absolute_date_str


def parse_date_for_sorting(date_str):
    """
    Parse date string for sorting.
    Handles multiple formats:
    - DD/MM/YYYY format (from Playwright CSV)
    - YYYY-MM-DD format (from API JSON)
    - Dates with relative time tags (e.g., '20/01/2026 - 3d' or '2026-01-20 - 2026-01-20')
    Returns datetime object, or datetime.min if parsing fails.
    """
    try:
        # Extract just the date part before the " - " separator
        date_part = date_str.split(' - ')[0].strip()

        # Try DD/MM/YYYY format first (Playwright CSV)
        try:
            return datetime.strptime(date_part, "%d/%m/%Y")
        except ValueError:
            # Try YYYY-MM-DD format (API JSON)
            return datetime.strptime(date_part, "%Y-%m-%d")
    except (ValueError, TypeError, IndexError):
        logger.warning(f"Could not parse date for sorting: '{date_str}'. Sorting to end.")
        return datetime.min


def generate_potential_actions(company_name, growth_posts, company_data=None):
    """
    Generate potential actions for investment analysts based on company growth signals.
    Returns a list of actionable items from a private equity perspective.
    """
    logger.info(f"Generating potential actions for {company_name}")

    if not growth_posts and not company_data:
        logger.warning(f"No growth posts or company data for {company_name}, returning default actions")
        return ["Schedule introductory call with founders", "Research competitive landscape"]

    # Build context from growth posts
    posts_summary = ""
    if growth_posts:
        posts_summary = "\n".join(
            f"- [{p.get('growth_type', 'growth')}] {p.get('summary', '')}"
            for p in growth_posts
        )

    # Build context from articles if available
    articles_summary = ""
    if company_data and company_data.get("articles"):
        articles_summary = "\n".join(
            f"- {a.get('headline', '')} ({a.get('growth_type', '')})"
            for a in company_data.get("articles", [])[:5]
        )

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a relationship-focused advisor for a private equity firm. "
                        "Generate creative, specific ways for investment analysts to build genuine "
                        "relationships with company founders and executives. Focus on physical meetings, "
                        "social activities, and proactive outreach - NOT research or due diligence. "
                        "Think: coffee meetings, golf, tennis, dinners, industry events, introductions, "
                        "sending gifts, attending their events, inviting them to exclusive gatherings."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Based on these signals about {company_name}, suggest 4-6 creative ways to "
                        f"build a relationship with the founders/executives:\n\n"
                        f"Recent Company Signals:\n{posts_summary}\n\n"
                        f"News & Articles:\n{articles_summary}\n\n"
                        "Focus on PHYSICAL and SOCIAL relationship-building activities like:\n"
                        "- Inviting them to play golf, tennis, or other sports\n"
                        "- Coffee or lunch meetings at specific venues\n"
                        "- Attending or inviting them to industry events\n"
                        "- Sending personalized gifts related to their interests\n"
                        "- Making warm introductions to valuable contacts\n"
                        "- Congratulating them in person on milestones\n\n"
                        "Format as a simple numbered list. Each action should be a single clear sentence "
                        "with specific details tailored to this company. No markdown, no bold, no sub-bullets."
                    ),
                },
            ],
        )
        actions_text = response.choices[0].message.content.strip()

        # Parse numbered list into array
        actions = []
        for line in actions_text.split('\n'):
            line = line.strip()
            # Skip empty lines and sub-bullets
            if not line or line.startswith('-'):
                continue
            # Remove numbering (1., 2., etc.) and clean up
            if line and line[0].isdigit():
                # Remove leading number and punctuation
                clean_line = line.lstrip('0123456789.-) ').strip()
                # Remove markdown formatting (**, *, etc.)
                clean_line = clean_line.replace('**', '').replace('*', '')
                # Remove trailing colons from header-style lines
                if clean_line.endswith(':'):
                    continue  # Skip header-only lines
                if clean_line and len(clean_line) > 15:
                    actions.append(clean_line)
            elif line and not line[0].isdigit() and len(line) > 20:
                # Include non-numbered substantial lines, clean markdown
                clean_line = line.replace('**', '').replace('*', '')
                if not clean_line.endswith(':'):
                    actions.append(clean_line)

        # Ensure we have at least some actions
        if not actions:
            actions = [actions_text]  # Fall back to full text if parsing fails

        logger.info(f"Generated {len(actions)} potential actions for {company_name}")
        return actions

    except Exception as e:
        logger.exception(f"Failed to generate potential actions: {e}")
        return ["Schedule introductory call with founders", "Research competitive landscape"]


def generate_reachout_message(company_name, growth_posts, company_data=None):
    """
    Generate a short professional LinkedIn reachout message from Armitage Associates.
    Uses growth posts when available, falls back to news articles.
    Returns the message string, or an empty string on failure.
    """
    logger.info(f"Generating LinkedIn reachout message for {company_name} based on {len(growth_posts)} growth posts")

    # Build context from growth posts
    posts_summary = ""
    if growth_posts:
        posts_summary = "\n".join(
            f"- [{p.get('growth_type', 'growth')}] {p.get('summary', '')}"
            for p in growth_posts
        )

    # Build context from news articles as fallback or supplement
    articles_summary = ""
    if company_data and company_data.get("articles"):
        articles_summary = "\n".join(
            f"- {a.get('headline', '')} ({a.get('growth_type', '')})"
            for a in company_data.get("articles", [])[:5]
        )

    if not posts_summary and not articles_summary:
        logger.warning(f"No growth posts or articles for {company_name}, skipping reachout message")
        return ""

    # Build the signals section
    signals = ""
    if posts_summary:
        signals += f"LinkedIn growth signals:\n{posts_summary}\n"
    if articles_summary:
        signals += f"Recent news:\n{articles_summary}\n"

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are writing a LinkedIn message on behalf of a partner at Armitage Associates, "
                        "a private equity firm that backs founder-led software and technology businesses in "
                        "Australia and New Zealand. "
                        "Write like a real person, not a chatbot. Vary your sentence length. "
                        "Use casual but professional language - the way a senior investor would actually "
                        "type a LinkedIn message on their phone. Short sentences. No filler. No corporate "
                        "buzzwords like 'synergy', 'leverage', 'ecosystem', or 'value proposition'. "
                        "Never start with 'I hope this message finds you well' or 'I came across your company'. "
                        "Sound like someone who genuinely follows the space and noticed something specific. "
                        "Keep it under 80 words. No emojis. No subject line. Just the message body."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Write a LinkedIn message to a founder/executive at {company_name}.\n\n"
                        f"{signals}\n"
                        "Rules:\n"
                        "- Open with something specific you noticed about their business - not a generic compliment\n"
                        "- Mention Armitage Associates naturally, not as a pitch\n"
                        "- Keep it conversational - one founder talking to another\n"
                        "- End with a low-pressure suggestion (coffee, quick call, or grabbing a beer)\n"
                        "- Do NOT use phrases like 'impressive growth', 'exciting trajectory', or 'caught my eye'\n"
                        "- Write like a text you'd actually send, not a template\n"
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


def add_posts_to_news_file(news_filepath, posts_data, message="", potential_actions=None):
    """
    Add the analyzed posts to the news JSON file under a 'posts' field.
    Also adds potential_actions field (required).
    """
    try:
        # Read existing news file
        with open(news_filepath, 'r', encoding='utf-8') as f:
            news_data = json.load(f)

        # Add posts, message, and potential_actions fields
        news_data['posts'] = posts_data
        news_data['message'] = message
        # Ensure potential_actions always exists
        news_data['potential_actions'] = potential_actions if potential_actions else []

        # Write back to file
        with open(news_filepath, 'w', encoding='utf-8') as f:
            json.dump(news_data, f, indent=2)

        logger.info(f"Successfully added {len(posts_data)} posts and {len(news_data['potential_actions'])} actions to {news_filepath}")

    except Exception as e:
        logger.exception(f"Failed to add posts to news file: {e}")
        return False
    return True


def summarize_posts(news_filepath, posts_filepath):
    """
    Main function to process LinkedIn posts (JSON or CSV) and add growth indicators to news file.

    Args:
        news_filepath: Path to the company news JSON file (e.g., "data/output/OnQ Software.json")
        posts_filepath: Path to the LinkedIn posts file (e.g., "data/output/OnQ Software Linkedin Posts.json" or .csv)

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
        logger.warning(f"Posts file not found: {posts_filepath}, skipping LinkedIn post analysis")
        return None

    if not os.path.exists(news_filepath):
        logger.warning(f"News JSON file not found: {news_filepath}, cannot add posts")
        return None

    logger.info(f"Processing posts from {posts_filepath}")

    try:
        # Parse posts file (handles both JSON and CSV)
        posts = parse_posts_file(posts_filepath)
        logger.info(f"Found {len(posts)} posts")

        if not posts:
            logger.warning("No posts found, skipping analysis")
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
                date_from_analysis = analysis.get('date', 'Unknown')

                # Check if date is already absolute (DD/MM/YYYY format) or relative (e.g., "2w")
                if date_from_analysis and '/' in date_from_analysis and len(date_from_analysis) == 10:
                    # Already absolute format (DD/MM/YYYY) - from API or Perplexity
                    absolute_date = date_from_analysis
                    relative_date = calculate_relative_date(absolute_date)
                else:
                    # Relative format (e.g., "2w") - from Playwright CSV
                    relative_date = date_from_analysis
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

        # Load company data for generating actions and message
        with open(news_filepath, 'r', encoding='utf-8') as f:
            company_data = json.load(f)
        company_name = company_data.get('company', 'the company')

        # Generate LinkedIn reachout message from growth posts and news
        message = generate_reachout_message(company_name, growth_posts, company_data)

        # Generate potential actions for investment analysts
        potential_actions = generate_potential_actions(company_name, growth_posts, company_data)

        # Add to news file
        add_posts_to_news_file(news_filepath, growth_posts, message, potential_actions)

        logger.info("Processing complete!")
        return growth_posts

    except FileNotFoundError as e:
        logger.error(f"File not found during summarization: {e}")
        return None
    except Exception as e:
        logger.exception(f"Error during summarization: {e}")
        return None


# Backward compatibility wrapper
def summarize_csv(news_filepath, posts_filepath):
    """
    Backward compatibility wrapper for summarize_posts.
    Deprecated: Use summarize_posts instead.
    """
    return summarize_posts(news_filepath, posts_filepath)


if __name__ == "__main__":
    # Example usage
    summarize_csv(
        news_filepath="data/output/GRC Solutions.json",
        posts_filepath="data/output/GRC Solutions Linkedin Posts.csv"
    )
