# Armitage Outreach Automation

Automated growth intelligence pipeline for private equity analysts. Imports target companies from Salesforce, scrapes news and LinkedIn for growth signals, generates AI-powered analysis and outreach recommendations, then delivers results via email digests and Salesforce CRM sync.

## How It Works

The pipeline runs in six stages, orchestrated by `main.py`:

```
                         Salesforce CRM
                        ┌──────────────┐
                        │  Dashboards  │
                        │  "GOWT High" │
                        └──────┬───────┘
                               │
                    ┌──────────▼──────────┐
              1.    │  Import Companies   │
                    │  + Owner Mapping    │
                    │  + Contact Mapping  │
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
              2.    │  Enrich Companies   │
                    │  SerpAPI + Firmable │
                    └──────────┬──────────┘
                               │
    ┌──────────────────────────┼──────────────────────────┐
    │                          │                          │
┌───▼──────────────┐  ┌────────▼──────────┐  ┌────────────▼─────────┐
│  3a. Scrape News │  │ 3b. Scrape Co.    │  │ 3c. Scrape Contact   │
│  Perplexity AI   │  │ LinkedIn Posts    │  │ LinkedIn Posts       │
│                  │  │ API → Req → Pwrt  │  │ SerpAPI + BrightData │
└───┬──────────────┘  └────────┬──────────┘  └────────────┬─────────┘
    │                          │                          │
    └──────────────────────────┼──────────────────────────┘
                               │
                    ┌──────────▼──────────┐
              4.    │   AI Analysis       │
                    │   OpenAI GPT-4o     │
                    │   Growth signals    │
                    │   Contact activity  │
                    │   Reachout message  │
                    │   Action items      │
                    └──────────┬──────────┘
                               │
              ┌────────────────┼────────────────┐
              │                │                │
   ┌──────────▼────┐  ┌────────▼──────┐  ┌──────▼────┐
   │ 5a. Salesforce│  │ 5b. Email     │  │ 5c. Clean │
   │ Push to CRM   │  │ Owner Digests │  │ up files  │
   └───────────────┘  └───────────────┘  └───────────┘
```

### Stage 1 — Import Companies

Authenticates with Salesforce (OAuth2 client credentials), reads all dashboards, and extracts company names and locations from the target reports ("GOWT Ultra High's", "GOWT High's"). Queries opportunity owner emails and primary contact names via SOQL (`OpportunityContactRole` where `IsPrimary = true`).

Produces:
- `data/input/companies.csv` — target company list
- `data/input/owner_mapping.json` — maps owner emails to their companies
- `data/input/contact_mapping.json` — maps company names to primary contact names

### Stage 2 — Enrich Companies

For each company:
1. **SerpAPI** — Google search to find the company's website domain
2. **Firmable API** — enriches with HQ location, LinkedIn company ID, and industry

Output: `company_info` dict used by all subsequent scrapers.

### Stage 3a — Scrape News

Uses **Perplexity AI** (sonar-pro model) with web search to find recent news articles. Searches the company's own website plus Australian business media (AFR, SmartCompany, StartupDaily, etc.) over the last 30 days.

Output: `data/output/{Company}.json` with articles (headline, date, summary, growth type, source URL).

### Stage 3b — Scrape Company LinkedIn

Three-tier fallback for LinkedIn company posts:

| Tier | Method | Details |
|------|--------|---------|
| 1 | **BrightData API** | Triggers async scrape, polls until complete, downloads JSON snapshot. Primary method. |
| 2 | **HTTP Requests** | Direct HTTP with anti-bot headers, user-agent rotation, random delays. Extracts posts from page source. |
| 3 | **Playwright** | Headless browser with stealth plugin. Randomized fingerprints, bezier mouse movements, DuckDuckGo search to reach company page. |

Each tier is tried in order. Tiers 2 and 3 are opt-in via environment variables.

### Stage 3c — Scrape Contact LinkedIn Activity

Scrapes the primary contact person's individual LinkedIn posts (past 30 days):

1. **SerpAPI** — Google searches `"{contact name} {company} LinkedIn"` and picks the first `linkedin.com/in/` result
2. **BrightData** — triggers an async profile scrape using `discover_by=profile_url` (same dataset API as company scraping, different discovery mode)
3. **OpenAI GPT-4o-mini** — summarizes each post into a one-sentence summary with date and topic category

If no primary contact exists, no LinkedIn URL is found, or the person has no recent posts, the pipeline continues and pushes a "no recent activity" message to Salesforce.

### Stage 4 — AI Analysis

Sends all scraped data to **OpenAI GPT-4o-mini** for:
- **Growth signal detection** — filters posts/articles for 9 growth indicator types
- **LinkedIn reachout message** — personalized, conversational, under 80 words
- **Potential actions** — 4-6 relationship-building activities (coffee, golf, introductions, etc.)

Merges everything into the final company JSON file.

### Stage 5 — Delivery

- **Salesforce** — updates Opportunity records with `Growth_News__c` (news + company posts), `Growth_Actions__c` (actions + outreach message), and `P__c` (contact LinkedIn activity, formatted HTML)
- **Email** — sends per-owner HTML digests via Gmail SMTP (each analyst gets only their companies)
- **Cleanup** — deletes all intermediate files from `data/input/` and `data/output/`

## Growth Signals

The system identifies these growth indicators:

- Awards and recognition
- Business expansion
- New hires / team growth
- Partnerships and collaborations
- Patents and innovations
- Financial success / funding
- Product launches
- Market expansion
- Client acquisitions

## Project Structure

```
├── main.py                               # Pipeline entry point (--company, --no-email)
├── scraper.py                            # Per-company scrape orchestration
├── salesforce.py                         # Salesforce import + push
├── company/
│   ├── get_company_info.py               # Aggregates SerpAPI + Firmable data
│   ├── serp_company_url.py               # Google Search for company website
│   ├── serp_contact_url.py              # Google Search for contact LinkedIn URL
│   └── firmable_data.py                  # Firmable API enrichment
├── scrapers/
│   ├── perplexity_scraper.py             # News scraping (Perplexity AI)
│   ├── linkedin_scraper_api.py           # Company LinkedIn via BrightData API
│   ├── linkedin_contact_scraper.py      # Contact LinkedIn via BrightData API
│   ├── linkedin_scraper_requests.py      # LinkedIn via HTTP requests
│   └── linkedin_scraper_playwright.py    # LinkedIn via browser automation
├── utils/
│   ├── summarizer.py                     # OpenAI analysis, reachout, actions, contact summaries
│   └── email_client.py                   # HTML email formatting + SMTP
├── data/
│   ├── input/                            # companies.csv, owner_mapping.json, contact_mapping.json
│   └── output/                           # {Company}.json reports
├── .github/
│   └── workflows/
│       └── run-schedule.yml              # Monthly GitHub Actions schedule
└── tests/
    ├── test_owner_mapping.py             # Preview owner → company distribution
    └── test_contact_pipeline.py          # End-to-end contact pipeline test
```

## Setup

### Prerequisites

- Python 3.12+
- Playwright browsers (only if using the Playwright fallback)

### Installation

```bash
git clone <your-repo-url>
cd armitage-automation
pip install -r requirements.txt

# Only if using Playwright fallback
playwright install --with-deps chromium
```

### Configuration

```bash
cp .env.sample .env
```

Fill in the following environment variables:

**Core APIs (required):**

| Variable | Service | Purpose |
|----------|---------|---------|
| `OPENAI_API_KEY` | OpenAI | Post analysis and reachout generation (GPT-4o-mini) |
| `PERPLEXITY_API_KEY` | Perplexity AI | News scraping (sonar-pro model) |
| `FIRMABLE_API_KEY` | Firmable | Company enrichment (HQ, LinkedIn ID, industry) |
| `SERP_API_KEY` | SerpAPI | Google Search for company website URLs |
| `BRIGHTDATA_API_KEY` | BrightData | LinkedIn post scraping (primary method) |

**Salesforce:**

| Variable | Purpose |
|----------|---------|
| `SALESFORCE_DOMAIN` | Instance URL (e.g., `https://yourorg.my.salesforce.com`) |
| `SALESFORCE_USERNAME` | Login email |
| `SALESFORCE_PASSWORD` | Password |
| `SALESFORCE_SECURITY_TOKEN` | Security token |
| `CONSUMER_KEY` | Connected App consumer key |
| `CONSUMER_SECRET` | Connected App consumer secret |
| `ACCESS_TOKEN` | OAuth2 access token |

**Email:**

| Variable | Default | Purpose |
|----------|---------|---------|
| `SMTP_USER` | — | Gmail address |
| `SMTP_PASSWORD` | — | Gmail app password |
| `SENDER_EMAIL` | `SMTP_USER` | From address |
| `EMAIL_RECIPIENTS` | — | Fallback recipients (comma-separated) |

**Optional flags:**

| Variable | Default | Purpose |
|----------|---------|---------|
| `USE_REQUESTS_FALLBACK` | `true` | Enable HTTP-based LinkedIn scraper as Tier 2 |
| `USE_PLAYWRIGHT_FALLBACK` | `false` | Enable Playwright browser scraper as Tier 3 |

## Usage

### Full Pipeline

```bash
python main.py
```

Runs all stages: Salesforce import, scraping (news + company LinkedIn + contact LinkedIn), AI analysis, Salesforce push, email delivery, and cleanup.

### Single Company (Testing)

```bash
# Run the full pipeline for one company only
python main.py --company "OnQ Software"

# Single company, skip emails
python main.py --company "OnQ Software" --no-email
```

Imports from Salesforce, looks up the company in `companies.csv` (case-insensitive match), runs the full scrape/analysis/push pipeline for just that company. No inter-company delay.

### Contact Pipeline Test

```bash
# Run all 5 steps end-to-end
python tests/test_contact_pipeline.py

# Test a single step with mock data for upstream steps
python tests/test_contact_pipeline.py --step search
python tests/test_contact_pipeline.py --step scrape
python tests/test_contact_pipeline.py --step push

# Override sample companies
python tests/test_contact_pipeline.py --companies "OnQ Software" "Axcelerate"

# Keep intermediate files for inspection
python tests/test_contact_pipeline.py --no-cleanup
```

Tests the contact-specific pipeline: Salesforce contact lookup, SerpAPI LinkedIn search, BrightData profile scrape, OpenAI summarization, and Salesforce push to `P__c`.

### Individual Components

```bash
# Scrape all companies from CSV
python scraper.py

# Import companies from Salesforce only
python salesforce.py

# Send digest email from existing output data
python utils/email_client.py recipient@example.com
```

### GitHub Actions (Recommended)

The pipeline is configured to run automatically via GitHub Actions:

- **Schedule:** 25th of each month at 00:00 UTC
- **Manual:** trigger via the Actions tab ("Run workflow")

**Setup:**

1. Push the repo to GitHub
2. Go to Settings > Secrets and variables > Actions
3. Add all `.env` variables as repository secrets
4. The workflow runs on `ubuntu-latest` with Python 3.12

Output JSON files are uploaded as artifacts with 30-day retention.

## Output Format

Each company produces a JSON report in `data/output/`:

```json
{
  "company": "OnQ Software",
  "articles": [
    {
      "headline": "OnQ wins excellence award",
      "date": "09/09/2025",
      "summary": "OnQ Software recognised for innovation...",
      "growth_type": "awards",
      "source_url": "https://..."
    }
  ],
  "posts": [
    {
      "summary": "Announced launch of AI assistant Sophia...",
      "growth_type": "product_launch",
      "date": "17/01/2026 - 3w"
    }
  ],
  "message": "Hi [Name], I noticed OnQ's recent launch of Sophia...",
  "potential_actions": [
    "Invite founders for a round of golf to discuss growth plans",
    "Arrange a coffee meeting to explore partnership opportunities"
  ],
  "linkedin_url": "https://www.linkedin.com/company/onqsoftware/posts/",
  "contact_name": "Nick Gannoulis",
  "contact_posts": [
    {
      "summary": "Shared insights on laboratory management trends for 2026",
      "date": "03/02/2026 - 1w",
      "topic": "industry insight"
    }
  ]
}
```

When no primary contact or no recent posts: `"contact_name": null, "contact_posts": []`.

## External Services

| Service | Model / API | Role |
|---------|-------------|------|
| [Salesforce](https://salesforce.com) | REST API v62.0 | Company import and results push |
| [SerpAPI](https://serpapi.com) | Google Search | Find company website domains |
| [Firmable](https://firmable.com) | Company API | Enrich with LinkedIn ID, industry, HQ |
| [Perplexity AI](https://perplexity.ai) | sonar-pro | News article discovery |
| [BrightData](https://brightdata.com) | Dataset API | LinkedIn post scraping |
| [OpenAI](https://openai.com) | gpt-4o-mini | Growth analysis, reachout, actions |
| Gmail | SMTP (587/TLS) | Email delivery |

## Error Handling

The pipeline is designed for graceful degradation:

| Failure | Impact | Recovery |
|---------|--------|----------|
| Perplexity API down | No news articles | Continues with LinkedIn data |
| All LinkedIn scrapers fail | No posts | Generates actions from news only |
| Contact not in Salesforce | No contact mapping | Pushes "no primary contact" to `P__c` |
| Contact LinkedIn URL not found | No contact posts | Pushes "no recent activity" to `P__c` |
| Contact scrape returns no posts | No contact summaries | Pushes "no recent activity" to `P__c` |
| SerpAPI returns nothing | Company skipped | Moves to next company |
| Firmable API fails | Reduced enrichment | Uses defaults, continues |
| Salesforce auth fails | No CRM sync | Reports still emailed |
| SMTP fails | Email not sent | Logged, pipeline completes |

Between companies, the pipeline waits 5 minutes to respect API rate limits. Individual company failures do not stop the pipeline. The contact pipeline is fully wrapped in error handling — any failure at any step logs a warning and continues.

## License

Proprietary — Armitage Associates
