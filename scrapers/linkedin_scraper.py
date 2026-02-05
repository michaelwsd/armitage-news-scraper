import os
import asyncio
import random
import csv
import logging
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

# -------------------------------------------------------------------
# Logging configuration
# -------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

# Pool of recent, realistic user agents to rotate through
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:133.0) Gecko/20100101 Firefox/133.0",
]

VIEWPORTS = [
    {"width": 1280, "height": 720},
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
]

LOCALES = ["en-US", "en-GB", "en-AU"]

TIMEZONES = [
    "America/New_York",
    "America/Chicago",
    "America/Los_Angeles",
    "Europe/London",
    "Australia/Sydney",
    "Australia/Melbourne",
]


# -------------------------------------------------------------------
# Human-like behavior helpers
# -------------------------------------------------------------------

def _bezier_points(start, end, steps):
    """Generate points along a quadratic bezier curve between start and end."""
    cx = (start[0] + end[0]) / 2 + random.uniform(-120, 120)
    cy = (start[1] + end[1]) / 2 + random.uniform(-80, 80)

    points = []
    for i in range(steps + 1):
        t = i / steps
        inv = 1 - t
        x = inv * inv * start[0] + 2 * inv * t * cx + t * t * end[0]
        y = inv * inv * start[1] + 2 * inv * t * cy + t * t * end[1]
        points.append((int(x), int(y)))
    return points


async def human_move_mouse(page, target_x, target_y):
    """Move the mouse to (target_x, target_y) along a curved path."""
    current = await page.evaluate("() => ({x: window._mouseX || 640, y: window._mouseY || 360})")
    start = (current["x"], current["y"])
    end = (target_x, target_y)

    steps = random.randint(18, 35)
    points = _bezier_points(start, end, steps)

    for px, py in points:
        await page.mouse.move(px, py)
        await asyncio.sleep(random.uniform(0.004, 0.018))

    await page.evaluate(f"() => {{ window._mouseX = {target_x}; window._mouseY = {target_y}; }}")


async def human_scroll(page, distance, direction="down"):
    """Scroll by `distance` pixels using many small increments with varying speed."""
    remaining = abs(distance)
    sign = 1 if direction == "down" else -1

    while remaining > 0:
        chunk = min(random.randint(30, 120), remaining)
        await page.mouse.wheel(0, sign * chunk)
        remaining -= chunk
        if random.random() < 0.1:
            await asyncio.sleep(random.uniform(0.15, 0.4))
        else:
            await asyncio.sleep(random.uniform(0.02, 0.07))


async def human_click_element(page, locator):
    """Move the mouse to an element's bounding box with a curve, then click."""
    bbox = await locator.bounding_box()
    if not bbox:
        await locator.click()
        return

    target_x = bbox["x"] + bbox["width"] * random.uniform(0.25, 0.75)
    target_y = bbox["y"] + bbox["height"] * random.uniform(0.3, 0.7)

    await human_move_mouse(page, target_x, target_y)
    await asyncio.sleep(random.uniform(0.05, 0.2))
    await page.mouse.click(target_x, target_y)


async def human_type(page, locator, text):
    """Type text character by character with human-like delays."""
    await human_click_element(page, locator)
    await asyncio.sleep(random.uniform(0.2, 0.5))

    for char in text:
        await page.keyboard.type(char, delay=random.uniform(40, 180))
        if random.random() < 0.08:
            await asyncio.sleep(random.uniform(0.2, 0.6))


async def dismiss_signin_modal(page):
    """Detect and dismiss LinkedIn's 'Sign in' overlay modal if present."""
    try:
        close_selectors = [
            "button[aria-label='Dismiss']",
            "button.modal__dismiss",
            "button.contextual-sign-in-modal__modal-dismiss",
            "button.contextual-sign-in-modal__modal-dismiss-btn",
            "icon.contextual-sign-in-modal__modal-dismiss-icon",
        ]
        for sel in close_selectors:
            btn = page.locator(sel).first
            if await btn.count() > 0 and await btn.is_visible():
                logger.info(f"Sign-in modal detected, dismissing via {sel}...")
                await human_click_element(page, btn)
                await asyncio.sleep(random.uniform(0.5, 1.0))
                return True

        modal = page.locator("div.modal, div[role='dialog']").first
        if await modal.count() > 0 and await modal.is_visible():
            x_btn = modal.locator("button:has(svg), button:has(li-icon)").first
            if await x_btn.count() > 0 and await x_btn.is_visible():
                logger.info("Sign-in modal detected, dismissing via X button...")
                await human_click_element(page, x_btn)
                await asyncio.sleep(random.uniform(0.5, 1.0))
                return True
    except Exception as e:
        logger.debug(f"Error dismissing sign-in modal: {e}")

    return False


async def idle_behavior(page):
    """Simulate idle human behavior — small random mouse drift or brief pause."""
    action = random.random()
    vw = 1280
    vh = 720

    if action < 0.4:
        await human_move_mouse(page, random.randint(200, vw - 200), random.randint(150, vh - 150))
    elif action < 0.7:
        await asyncio.sleep(random.uniform(1.0, 3.0))
    else:
        nudge = random.randint(80, 200)
        await human_scroll(page, nudge, direction="up")
        await asyncio.sleep(random.uniform(0.5, 1.5))
        await human_scroll(page, nudge, direction="down")


# -------------------------------------------------------------------
# Main scraper
# -------------------------------------------------------------------

async def scrape_news_linkedin(company_info):
    """
    Scrape LinkedIn posts for a company via its public page (no login required).
    Navigates via DuckDuckGo search to appear as organic traffic.

    Returns:
        str: Path to output CSV file on success
        None: On any failure (missing linkedin ID, browser error, etc.)
    """
    company_name = company_info.get('name', 'Unknown')
    company_city = company_info.get('city', 'Unknown')
    linkedin_id = company_info.get('linkedin')

    if not linkedin_id:
        logger.warning(f"No LinkedIn ID available for {company_name}, skipping LinkedIn scrape")
        return None

    search_query = f"{company_name} {company_city} Linkedin"
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    output_dir = os.path.join(project_root, "data", "output")
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"{company_name} Linkedin Posts.csv")
    scroll_loops = random.randint(4, 7)

    try:
        success = await run(search_query, linkedin_id, scroll_loops, output_file)
        if success:
            return output_file
        else:
            logger.warning(f"LinkedIn scrape did not complete successfully for {company_name}")
            return None
    except Exception as e:
        logger.exception(f"LinkedIn scraper failed for {company_name}: {e}")
        return None


async def run(search_query, linkedin_id, scroll_loops, output_file):
    """
    Run the LinkedIn public page scraper with Playwright.
    Each invocation uses a fresh incognito context with a randomised fingerprint.

    Returns:
        bool: True on success, False on failure
    """
    browser = None
    context = None
    try:
        async with Stealth().use_async(async_playwright()) as p:
            # Build a unique fingerprint for this run
            ua = random.choice(USER_AGENTS)
            viewport = random.choice(VIEWPORTS)
            locale = random.choice(LOCALES)
            timezone = random.choice(TIMEZONES)
            logger.info(
                f"Identity: UA={ua[:40]}... "
                f"viewport={viewport['width']}x{viewport['height']} "
                f"locale={locale} tz={timezone}"
            )

            browser = await p.chromium.launch(
                headless=False,
                args=["--headless=new"]
                )
            context = await browser.new_context(
                user_agent=ua,
                viewport=viewport,
                locale=locale,
                timezone_id=timezone,
                color_scheme=random.choice(["light", "dark"]),
            )

            page = await context.new_page()

            # --- Step 1: Go to DuckDuckGo ---
            initial_delay = random.uniform(1, 3)
            logger.debug(f"Waiting {initial_delay:.1f}s before navigating...")
            await asyncio.sleep(initial_delay)

            logger.info("Navigating to DuckDuckGo...")
            try:
                await page.goto("https://duckduckgo.com", timeout=60000)
            except Exception as e:
                logger.warning(f"DuckDuckGo navigation warning: {e}")

            await asyncio.sleep(random.uniform(1.0, 2.5))

            # --- Step 2: Type search query ---
            search_box = page.locator("input[name='q']").first
            logger.info(f"Typing search query: {search_query}")
            await human_type(page, search_box, search_query)
            await asyncio.sleep(random.uniform(0.5, 1.2))

            await page.keyboard.press("Enter")
            logger.info("Submitted DuckDuckGo search, waiting for results...")

            try:
                await page.wait_for_load_state("domcontentloaded", timeout=15000)
            except Exception:
                pass
            await asyncio.sleep(random.uniform(1.5, 3.0))

            # --- Step 3: Find and click the LinkedIn company result ---
            # Prefer a link that matches the exact linkedin slug
            linkedin_link = page.locator(f"a[href*='linkedin.com/company/{linkedin_id}']").first
            if await linkedin_link.count() == 0:
                # Fallback: any linkedin company link
                linkedin_link = page.locator("a[href*='linkedin.com/company/']").first

            if await linkedin_link.count() == 0:
                logger.error(f"Could not find a LinkedIn company link in DuckDuckGo results for '{search_query}'")
                await context.close()
                await browser.close()
                return False

            href = await linkedin_link.get_attribute("href")
            logger.info(f"Found LinkedIn result: {href}. Clicking...")
            await asyncio.sleep(random.uniform(1.0, 3.0))
            await human_click_element(page, linkedin_link)

            try:
                await page.wait_for_load_state("domcontentloaded", timeout=30000)
            except Exception:
                pass
            await asyncio.sleep(random.uniform(1.5, 3.0))

            # --- Step 4: Dismiss sign-in modal if present ---
            await dismiss_signin_modal(page)

            current_url = page.url
            logger.info(f"Landed on: {current_url}")

            # Handle LinkedIn auth/captcha walls
            if "login" in page.url or "authwall" in page.url or "signup" in page.url:
                logger.error("LinkedIn auth wall hit — page requires login (expected for guest)")
                await context.close()
                await browser.close()
                return False

            if "checkpoint/challenge" in page.url or "security-verification" in page.url:
                logger.error("LinkedIn CAPTCHA/security check detected")
                await context.close()
                await browser.close()
                return False

            # --- Step 5: Scroll down to the Updates section ---
            read_delay = random.uniform(2, 5)
            logger.debug(f"Reading page for {read_delay:.1f}s before scrolling...")
            await asyncio.sleep(read_delay)

            await human_move_mouse(page, random.randint(400, 900), random.randint(300, 500))

            logger.info(f"Starting scroll loop ({scroll_loops} scrolls)...")
            for i in range(scroll_loops):
                scroll_distance = random.randint(600, 1400)
                await human_scroll(page, scroll_distance, direction="down")
                logger.debug(f"Scroll {i + 1}/{scroll_loops} ({scroll_distance}px)")

                await asyncio.sleep(random.uniform(0.5, 2.5))

                # Dismiss sign-in modal if it reappears during scrolling
                await dismiss_signin_modal(page)

                if random.random() < 0.3:
                    await idle_behavior(page)

                if random.random() < 0.15:
                    back_up = random.randint(100, 350)
                    await human_scroll(page, back_up, direction="up")
                    await asyncio.sleep(random.uniform(1.0, 2.5))
                    await human_scroll(page, back_up, direction="down")

            # --- Step 6: Extract posts from the Updates section ---
            posts = await page.locator("article[data-id='main-feed-card']").all()
            logger.info(f"Found {len(posts)} posts. Parsing...")

            extracted_data = []

            for idx, post in enumerate(posts):
                try:
                    # Extract text
                    text = "No Text"
                    text_loc = post.locator("p[data-test-id='main-feed-activity-card__commentary']").first
                    if await text_loc.count() > 0:
                        text = await text_loc.inner_text()

                    # Extract date (e.g. "1d", "5w", "2mo")
                    date = "Unknown"
                    date_loc = post.locator("time").first
                    if await date_loc.count() > 0:
                        date = (await date_loc.inner_text()).strip()

                    # Extract reaction count
                    likes = "0"
                    likes_loc = post.locator("[data-test-id='social-actions__reaction-count']").first
                    if await likes_loc.count() > 0:
                        likes = (await likes_loc.inner_text()).strip()

                    extracted_data.append([date, likes, text.replace('\n', ' ').strip()])
                    logger.debug(f"Parsed post {idx + 1}: date={date}, likes={likes}")

                    await asyncio.sleep(random.uniform(0.3, 1.0))

                except Exception as e:
                    logger.warning(f"Error parsing post {idx + 1}, skipping: {e}")
                    continue

            # Save to CSV
            with open(output_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["Date", "Likes", "Content"])
                writer.writerows(extracted_data)

            logger.info(f"Successfully saved {len(extracted_data)} posts to {output_file}")
            await context.close()
            await browser.close()
            logger.info("Browser closed. Scraping complete.")
            return True

    except Exception as e:
        logger.exception(f"LinkedIn scraper error: {e}")
        for closeable in (context, browser):
            if closeable:
                try:
                    await closeable.close()
                except Exception:
                    pass
        return False


if __name__ == "__main__":
    company_info = {
        'hq_location': '11 Camford Street, '
        'Milton, QLD, 4064, AU', 
        'linkedin': 'axcelerate-student-training-rto-management-systems', 
        'industry': 'E-learning and online education', 
        'website': 'axcelerate.com.au', 
        'name': 'Axcelerate', 
        'city': 'Queensland'
        }
    
    asyncio.run(scrape_news_linkedin(company_info))
