import os
import asyncio
import json
import random
import csv
import logging
from dotenv import load_dotenv
from playwright.async_api import async_playwright

# -------------------------------------------------------------------
# Logging configuration
# -------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,  # change to DEBUG for more verbosity
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

load_dotenv()

async def scrape_news_linkedin(company_info):
    """
    Scrape LinkedIn posts for a company.

    Returns:
        str: Path to output CSV file on success
        None: On any failure (missing linkedin ID, browser error, etc.)
    """
    company_name = company_info.get('name', 'Unknown')
    linkedin_id = company_info.get('linkedin')

    # Check if we have a LinkedIn ID to scrape
    if not linkedin_id:
        logger.warning(f"No LinkedIn ID available for {company_name}, skipping LinkedIn scrape")
        return None

    company_url = f"https://www.linkedin.com/company/{linkedin_id}/posts/"
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    output_dir = os.path.join(project_root, "data", "output")
    output_file = os.path.join(output_dir, f"{company_name} Linkedin Posts.csv")
    scroll_loops = 5

    try:
        success = await run(company_url, scroll_loops, output_file)
        if success:
            return output_file
        else:
            logger.warning(f"LinkedIn scrape did not complete successfully for {company_name}")
            return None
    except Exception as e:
        logger.exception(f"LinkedIn scraper failed for {company_name}: {e}")
        return None

async def run(company_url, scroll_loops, output_file):
    """
    Run the LinkedIn scraper with Playwright.

    Returns:
        bool: True on success, False on failure
    """
    browser = None
    try:
        async with async_playwright() as p:
            # 1. Launch Browser (Note the 'await')
            browser = await p.chromium.launch(headless=False)

            # 2. Create Context
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            )

            # 3. Load Cookies
            script_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(script_dir)
            cookies_path = os.path.join(project_root, "cookies.json")

            try:
                with open(cookies_path, "r") as f:
                    cookies = json.load(f)
                    # Fix SameSite issues
                    for cookie in cookies:
                        if "sameSite" in cookie and cookie["sameSite"] not in ["Strict", "Lax", "None"]:
                            del cookie["sameSite"]

                    # add_cookies is async in the async API
                    await context.add_cookies(cookies)
                    logger.info("Cookies loaded successfully")
            except FileNotFoundError:
                logger.error(f"cookies.json not found at {cookies_path}")
                return False

            page = await context.new_page()

            # 4. Navigate
            logger.info(f"Navigating to {company_url}")
            try:
                await page.goto(company_url, timeout=60000)
                if "login" in page.url:
                    logger.warning("Hit 'Welcome Back' screen. Attempting to click account...")

                    # Option 1: Click specifically by your Account Name (Most Reliable for this screenshot)
                    # We use specific text visible in your image: Linkedin Username
                    account_button = page.locator(f"text={os.getenv("LINKEDIN_NAME")}")

                    if await account_button.count() > 0:
                        await account_button.click()
                        logger.info("Clicked account name")

                        # Wait for the feed to load so we know we are past the login screen
                        await page.wait_for_selector("div.feed-shared-update-v2", timeout=15000)
                    else:
                        # Option 2: Fallback to clicking the profile card container if text fails
                        # This targets the card structure itself
                        card = page.locator(".profile-card, .login__form_action_container").first
                        if await card.count() > 0:
                            await card.click()
            except Exception as e:
                logger.warning(f"Navigation warning: {e}")

            # 5. Scroll Loop (Async sleep)
            logger.info("Starting scroll loop...")
            for i in range(scroll_loops):
                await page.mouse.wheel(0, 1500)
                logger.debug(f"Scroll {i + 1}/{scroll_loops} completed")
                # Use asyncio.sleep, NOT time.sleep
                await asyncio.sleep(random.uniform(2, 4))

            # 6. Extract Data
            # wait for at least one post to ensure load
            try:
                await page.wait_for_selector("div.feed-shared-update-v2", timeout=10000)
            except asyncio.TimeoutError:
                logger.warning("Timeout waiting for posts selector")
            except Exception as e:
                logger.warning(f"Error waiting for posts: {e}")

            # locator.all() generates the list of elements
            posts = await page.locator("div.feed-shared-update-v2").all()
            logger.info(f"Found {len(posts)} posts. Parsing...")

            extracted_data = []

            # Loop through posts
            for idx, post in enumerate(posts):
                try:
                    # --- 1. HANDLE "SEE MORE" ---
                    see_more_button = post.locator("button.feed-shared-inline-show-more-text__see-more-less-toggle").first

                    if await see_more_button.count() > 0 and await see_more_button.is_visible():
                        try:
                            await see_more_button.click()

                            # CRITICAL FIX: Wait for the text to actually expand.
                            # We wait for the 'See more' button to become hidden or detached.
                            # This confirms the UI has updated.
                            try:
                                await see_more_button.wait_for(state="hidden", timeout=2000)
                            except asyncio.TimeoutError:
                                # If it times out, we just proceed. Sometimes it doesn't vanish but text expands.
                                pass

                        except Exception as e:
                            logger.debug(f"Could not click 'see more' for post {idx + 1}: {e}")

                    # --- 2. EXTRACT TEXT ---
                    text_locator = post.locator("span.break-words").first

                    if await text_locator.count() > 0:
                        # 'innerText' is usually safer than 'textContent' for visible text
                        text = await text_locator.inner_text()
                    else:
                        text = "No Text"

                    # --- 3. EXTRACT LIKES ---
                    likes_locator = post.locator(".social-details-social-counts__reactions-count").first
                    if await likes_locator.count() > 0:
                        likes = await likes_locator.inner_text()
                    else:
                        likes = "0"

                    # --- 4. EXTRACT DATE ---
                    date_locator = post.locator(".update-components-actor__sub-description").first
                    if await date_locator.count() == 0:
                        date_locator = post.locator("span[aria-hidden='true']").first

                    if await date_locator.count() > 0:
                        raw_date_text = await date_locator.inner_text()
                        date = raw_date_text.split("•")[0].strip()
                    else:
                        date = "Unknown"

                    # Remove the slicing [:200] so you see the FULL text in your CSV
                    extracted_data.append([date, likes, text.replace('\n', ' ')])
                    logger.debug(f"Successfully parsed post {idx + 1}: date={date}, likes={likes}")

                except Exception as e:
                    logger.warning(f"Error parsing post {idx + 1}, skipping: {e}")
                    continue

            # 7. Save to CSV
            with open(output_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["Date", "Likes", "Content"])
                writer.writerows(extracted_data)

            logger.info(f"Successfully saved {len(extracted_data)} posts to {output_file}")
            await browser.close()
            logger.info("Browser closed. Scraping complete.")
            return True

    except Exception as e:
        logger.exception(f"LinkedIn scraper error: {e}")
        if browser:
            try:
                await browser.close()
            except Exception:
                pass
        return False

if __name__ == "__main__":
    company_info = {
        'hq_location': '201 Kent St, Level 14, Sydney, NSW, 2000, AU', 
        'linkedin': 'grc-solutions-pty-ltd', 
        'industry': 'E-learning and online education', 
        'website': 'grc-solutions.com', 
        'name': 'GRC Solutions', 
        'city': 'Sydney'
        }
    # Standard boilerplate to run async functions
    asyncio.run(scrape_news_linkedin(company_info))