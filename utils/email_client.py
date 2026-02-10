import os
import json
import logging
import smtplib
from dotenv import load_dotenv
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

load_dotenv()

class EmailClient:
    def __init__(
        self,
        smtp_host: Optional[str] = None,
        smtp_port: Optional[int] = None,
        smtp_user: Optional[str] = None,
        smtp_password: Optional[str] = None,
        sender_email: Optional[str] = None,
    ):
        self.smtp_host = smtp_host or os.getenv("SMTP_HOST", "smtp.gmail.com")
        self.smtp_port = smtp_port or int(os.getenv("SMTP_PORT", "587"))
        self.smtp_user = smtp_user or os.getenv("SMTP_USER")
        self.smtp_password = smtp_password or os.getenv("SMTP_PASSWORD")
        self.sender_email = sender_email or os.getenv("SENDER_EMAIL", self.smtp_user)

        if not self.smtp_user or not self.smtp_password:
            raise ValueError(
                "SMTP credentials not configured. Set SMTP_USER and SMTP_PASSWORD environment variables."
            )

    def _create_html_email(self, company_data: dict) -> str:
        """Create HTML email content from company data."""
        company_name = company_data.get("company", "Unknown Company")
        articles = company_data.get("articles", [])
        posts = company_data.get("posts", [])

        html = f"""
                <!DOCTYPE html>
                <html>
                <head>
                    <style>
                        body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; max-width: 800px; margin: 0 auto; }}
                        .header {{ background: #2c3e50; color: white; padding: 20px; text-align: center; }}
                        .section {{ margin: 20px 0; padding: 15px; background: #f9f9f9; border-radius: 8px; }}
                        .section-title {{ color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; }}
                        .article, .post {{ background: white; padding: 15px; margin: 10px 0; border-left: 4px solid #3498db; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
                        .headline {{ font-weight: bold; color: #2c3e50; margin-bottom: 8px; }}
                        .meta {{ font-size: 0.85em; color: #666; margin-bottom: 8px; }}
                        .growth-tag {{ display: inline-block; background: #27ae60; color: white; padding: 2px 8px; border-radius: 4px; font-size: 0.8em; }}
                        .summary {{ margin-top: 10px; }}
                        a {{ color: #3498db; }}
                        .footer {{ text-align: center; padding: 20px; color: #666; font-size: 0.85em; }}
                    </style>
                </head>
                <body>
                    <div class="header">
                        <h1>{company_name}</h1>
                        <p>Growth Intelligence Report</p>
                    </div>
                """

        html += """
                        <div class="section">
                            <h2 class="section-title">News & Articles</h2>
                    """
        if articles:
            for article in articles:
                headline = article.get("headline", "No headline")
                date = article.get("date", "Unknown date")
                summary = article.get("summary", "")
                growth_type = article.get("growth_type", "")
                source_url = article.get("source_url", "")

                html += f"""
                                <div class="article">
                                    <div class="headline">{headline}</div>
                                    <div class="meta">
                                        <span>{date}</span>
                                        {f'<span class="growth-tag">{growth_type}</span>' if growth_type else ''}
                                    </div>
                                    <div class="summary">{summary}</div>
                                    {f'<div class="meta"><a href="{source_url}">Source</a></div>' if source_url else ''}
                                </div>
                        """
        else:
            html += '            <p style="color: #999; font-style: italic;">No news activity found.</p>\n'
        html += "    </div>\n"

        linkedin_url = company_data.get("linkedin_url")
        linkedin_title = f'<a href="{linkedin_url}" style="color: #2c3e50; text-decoration: none;">LinkedIn Posts</a>' if linkedin_url else "LinkedIn Posts"
        html += f"""
                        <div class="section">
                            <h2 class="section-title">{linkedin_title}</h2>
                    """
        if posts:
            for post in posts:
                summary = post.get("summary", "")
                date = post.get("date", "Unknown date")
                growth_type = post.get("growth_type", "")

                html += f"""
                                <div class="post">
                                    <div class="meta">
                                        <span>{date}</span>
                                        {f'<span class="growth-tag">{growth_type}</span>' if growth_type else ''}
                                    </div>
                                    <div class="summary">{summary}</div>
                                </div>
                        """
        else:
            html += '            <p style="color: #999; font-style: italic;">No LinkedIn activity found.</p>\n'
        html += "    </div>\n"

        potential_actions = company_data.get("potential_actions", [])
        html += """
                        <div class="section">
                            <h2 class="section-title">Potential Actions for Analysts</h2>
                    """
        if potential_actions:
            html += '            <ul style="background: white; padding: 15px 15px 15px 35px; margin: 0; border-left: 4px solid #9b59b6; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">\n'
            for action in potential_actions:
                html += f'            <li style="margin: 8px 0; color: #333;">{action}</li>\n'
            html += '            </ul>\n'
        else:
            html += '            <p style="color: #999; font-style: italic;">No actions available.</p>\n'
        html += "        </div>\n"

        message = company_data.get("message", "")
        html += """
                        <div class="section">
                            <h2 class="section-title">Suggested LinkedIn Reachout</h2>
                    """
        if message:
            html += f"""
                            <div style="background: white; padding: 15px; border-left: 4px solid #27ae60; box-shadow: 0 1px 3px rgba(0,0,0,0.1); white-space: pre-line;">
                                {message}
                            </div>
                    """
        else:
            html += '            <p style="color: #999; font-style: italic;">No outreach message available.</p>\n'
        html += "        </div>\n"

        html += """
                    <div class="footer">
                        <p>Generated by Armitage Automation</p>
                    </div>
                </body>
                </html>
                """
        return html

    def send_email(
        self,
        recipients: list[str],
        subject: str,
        html_content: str,
        plain_content: Optional[str] = None,
    ) -> bool:
        """Send an email to one or more recipients."""
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = self.sender_email
        msg["To"] = ", ".join(recipients)

        if plain_content:
            msg.attach(MIMEText(plain_content, "plain"))
        msg.attach(MIMEText(html_content, "html"))

        try:
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.sendmail(self.sender_email, recipients, msg.as_string())
            logger.info(f"Email sent to {recipients}")
            return True
        except smtplib.SMTPAuthenticationError as e:
            logger.error(f"SMTP authentication failed: {e}")
            return False
        except smtplib.SMTPException as e:
            logger.error(f"SMTP error: {e}")
            return False
        except Exception as e:
            logger.error(f"Failed to send email: {e}")
            return False

    def send_company_report(self, company_data: dict, recipients: list[str]) -> bool:
        """Send a company report email."""
        company_name = company_data.get("company", "Unknown Company")
        subject = f"Growth Report: {company_name}"
        html_content = self._create_html_email(company_data)
        return self.send_email(recipients, subject, html_content)


def load_json_files(output_dir: str = "data/output") -> list[dict]:
    """Load all JSON files from the output directory."""
    script_dir = Path(__file__).parent.parent
    output_path = script_dir / output_dir

    if not output_path.exists():
        logger.warning(f"Output directory not found: {output_path}")
        return []

    json_files = list(output_path.glob("*.json"))
    logger.info(f"Found {len(json_files)} JSON files in {output_path}")

    data = []
    for json_file in json_files:
        # Skip LinkedIn Posts files (they are intermediate files, not company reports)
        if "Linkedin Posts" in json_file.name:
            logger.debug(f"Skipping intermediate file: {json_file.name}")
            continue
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                company_data = json.load(f)
                # Validate it's a company report dict, not a posts list
                if not isinstance(company_data, dict) or "company" not in company_data:
                    logger.warning(f"Skipping {json_file.name}: not a valid company report")
                    continue
                data.append(company_data)
                logger.debug(f"Loaded {json_file.name}")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse {json_file.name}: {e}")
        except Exception as e:
            logger.error(f"Error reading {json_file.name}: {e}")

    return data


def send_all_reports(recipients: list[str], output_dir: str = "data/output") -> dict:
    """
    Load all JSON files and send individual report emails for each company.

    Returns:
        dict with 'sent' and 'failed' counts
    """
    client = EmailClient()
    companies = load_json_files(output_dir)

    results = {"sent": 0, "failed": 0, "companies": []}

    for company_data in companies:
        company_name = company_data.get("company", "Unknown")
        success = client.send_company_report(company_data, recipients)
        if success:
            results["sent"] += 1
            results["companies"].append({"company": company_name, "status": "sent"})
        else:
            results["failed"] += 1
            results["companies"].append({"company": company_name, "status": "failed"})

    logger.info(f"Email summary: {results['sent']} sent, {results['failed']} failed")
    return results


def _build_digest_html(companies: list[dict]) -> str:
    """Build the HTML content for a digest email containing the given companies."""
    html = """
            <!DOCTYPE html>
            <html>
            <head>
                <style>
                    body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; max-width: 900px; margin: 0 auto; }
                    .header { background: #2c3e50; color: white; padding: 20px; text-align: center; }
                    .company-section { margin: 30px 0; padding: 20px; background: #f9f9f9; border-radius: 8px; }
                    .company-name { color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; }
                    .subsection { margin: 15px 0; }
                    .subsection-title { color: #34495e; font-size: 1.1em; }
                    .item { background: white; padding: 12px; margin: 8px 0; border-left: 3px solid #3498db; }
                    .headline { font-weight: bold; color: #2c3e50; }
                    .meta { font-size: 0.85em; color: #666; }
                    .growth-tag { display: inline-block; background: #27ae60; color: white; padding: 2px 6px; border-radius: 4px; font-size: 0.75em; }
                    a { color: #3498db; }
                    .footer { text-align: center; padding: 20px; color: #666; font-size: 0.85em; border-top: 1px solid #ddd; margin-top: 30px; }
                    .toc { background: #ecf0f1; padding: 15px; border-radius: 8px; margin: 20px 0; color: #2c3e50; }
                    .toc strong { color: #2c3e50; }
                    .toc a { text-decoration: none; display: block; padding: 5px 0; color: #3498db; }
                </style>
            </head>
            <body>
                <div class="header">
                    <h1>Growth Intelligence Digest</h1>
                    <p>""" + f"{len(companies)} Companies" + """</p>
                </div>

                <div class="toc">
                    <strong>Companies:</strong>
            """

    for i, company_data in enumerate(companies):
        company_name = company_data.get("company", f"Company {i+1}")
        html += f'        <a href="#company-{i}">{company_name}</a>\n'

    html += "    </div>\n"

    for i, company_data in enumerate(companies):
        company_name = company_data.get("company", f"Company {i+1}")
        articles = company_data.get("articles", [])
        posts = company_data.get("posts", [])

        html += f"""
                    <div class="company-section" id="company-{i}">
                        <h2 class="company-name">{company_name}</h2>
                """
        html += """
                            <div class="subsection">
                                <h3 class="subsection-title">News & Articles</h3>
                    """
        if articles:
            for article in articles[:5]:
                headline = article.get("headline", "No headline")
                date = article.get("date", "")
                summary = article.get("summary", "")
                growth_type = article.get("growth_type", "")
                source_url = article.get("source_url", "")

                html += f"""
                                    <div class="item">
                                        <div class="headline">{headline}</div>
                                        <div class="meta">{date}</div>
                                        {f'<div style="margin: 4px 0;"><span class="growth-tag">{growth_type}</span></div>' if growth_type else ''}
                                        {f'<p style="margin: 8px 0; color: #555;">{summary}</p>' if summary else ''}
                                        {f'<a href="{source_url}">Read more</a>' if source_url else ''}
                                    </div>
                        """
        else:
            html += '                <p style="color: #999; font-style: italic;">No news activity found.</p>\n'
        html += "        </div>\n"

        linkedin_url = company_data.get("linkedin_url")
        linkedin_title = f'<a href="{linkedin_url}" style="color: #34495e; text-decoration: none;">LinkedIn Activity</a>' if linkedin_url else "LinkedIn Activity"
        html += f"""
                            <div class="subsection">
                                <h3 class="subsection-title">{linkedin_title}</h3>
                    """
        if posts:
            for post in posts[:3]:
                summary = post.get("summary", "")
                date = post.get("date", "")
                growth_type = post.get("growth_type", "")

                html += f"""
                                    <div class="item">
                                        <div class="meta">{date}</div>
                                        {f'<div style="margin: 4px 0;"><span class="growth-tag">{growth_type}</span></div>' if growth_type else ''}
                                        {f'<p style="margin: 8px 0; color: #555;">{summary}</p>' if summary else ''}
                                    </div>
                        """
        else:
            html += '                <p style="color: #999; font-style: italic;">No LinkedIn activity found.</p>\n'
        html += "        </div>\n"

        potential_actions = company_data.get("potential_actions", [])
        html += """
                        <div class="subsection">
                            <h3 class="subsection-title">Potential Actions for Analysts</h3>
                    """
        if potential_actions:
            html += '                            <div class="item" style="border-left-color: #9b59b6;">\n'
            html += '                                <ul style="margin: 0; padding-left: 20px;">\n'
            for action in potential_actions:
                html += f'                <li style="margin: 6px 0; color: #555;">{action}</li>\n'
            html += '                                </ul>\n'
            html += '                            </div>\n'
        else:
            html += '                <p style="color: #999; font-style: italic;">No actions available.</p>\n'
        html += "        </div>\n"

        message = company_data.get("message", "")
        html += """
                        <div class="subsection">
                            <h3 class="subsection-title">Suggested LinkedIn Reachout</h3>
                    """
        if message:
            html += f"""
                            <div class="item" style="border-left-color: #27ae60; white-space: pre-line;">
                                {message}
                            </div>
                    """
        else:
            html += '                <p style="color: #999; font-style: italic;">No outreach message available.</p>\n'
        html += "        </div>\n"

        html += "    </div>\n"

    html += """
                <div class="footer">
                    <p>Generated by Armitage Automation</p>
                </div>
            </body>
            </html>
            """
    return html


def send_digest_report(recipients: list[str], output_dir: str = "data/output") -> bool:
    """
    Load all JSON files and send a single digest email with all companies.
    """
    client = EmailClient()
    companies = load_json_files(output_dir)

    if not companies:
        logger.warning("No company data to send")
        return False

    html = _build_digest_html(companies)
    subject = f"Growth Intelligence Digest - {len(companies)} Companies"
    return client.send_email(recipients, subject, html)


def load_owner_mapping(input_dir: str = "data/input") -> dict | None:
    """Load the owner_email -> company_names mapping from JSON."""
    script_dir = Path(__file__).parent.parent
    mapping_path = script_dir / input_dir / "owner_mapping.json"

    if not mapping_path.exists():
        logger.warning(f"Owner mapping not found at {mapping_path}")
        return None

    try:
        with open(mapping_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load owner mapping: {e}")
        return None


def send_owner_digests(
    fallback_recipients: list[str] = None,
    output_dir: str = "data/output",
    input_dir: str = "data/input",
) -> dict:
    """
    Send per-owner digest emails. Each owner receives a digest containing
    only their companies' data.

    Companies without a mapped owner are included in a fallback digest
    sent to fallback_recipients (if provided).
    """
    mapping = load_owner_mapping(input_dir)

    if mapping is None:
        logger.warning("No owner mapping found. Falling back to standard digest.")
        if fallback_recipients:
            success = send_digest_report(fallback_recipients, output_dir)
            return {"owners_sent": 0, "owners_failed": 0, "fallback_sent": success}
        return {"owners_sent": 0, "owners_failed": 0, "fallback_sent": False}

    all_companies = load_json_files(output_dir)
    company_lookup = {cd.get("company"): cd for cd in all_companies}

    results = {"owners_sent": 0, "owners_failed": 0, "fallback_sent": False}
    client = EmailClient()

    owner_to_companies = mapping.get("owner_to_companies", {})
    unmapped_names = mapping.get("unmapped_companies", [])

    for owner_email, company_names in owner_to_companies.items():
        owner_companies = [company_lookup[name] for name in company_names if name in company_lookup]

        if not owner_companies:
            logger.warning(f"No scraped data for {owner_email}'s companies: {company_names}")
            continue

        html = _build_digest_html(owner_companies)
        subject = f"Growth Intelligence Digest - {len(owner_companies)} Companies"

        success = client.send_email([owner_email], subject, html)
        if success:
            results["owners_sent"] += 1
            logger.info(f"Sent digest to {owner_email}: {[c.get('company') for c in owner_companies]}")
        else:
            results["owners_failed"] += 1

    if unmapped_names and fallback_recipients:
        unmapped_data = [company_lookup[name] for name in unmapped_names if name in company_lookup]
        if unmapped_data:
            html = _build_digest_html(unmapped_data)
            subject = f"Growth Intelligence Digest (Unassigned) - {len(unmapped_data)} Companies"
            results["fallback_sent"] = client.send_email(fallback_recipients, subject, html)
    elif unmapped_names:
        logger.warning(f"Unmapped companies with no fallback recipients: {unmapped_names}")

    logger.info(
        f"Owner digests: {results['owners_sent']} sent, "
        f"{results['owners_failed']} failed, "
        f"fallback={'sent' if results['fallback_sent'] else 'not sent'}"
    )
    return results


def send_alert_email(recipients: list[str], subject: str, message: str) -> bool:
    """
    Send a simple alert/notification email.

    Args:
        recipients: List of email addresses to notify.
        subject: Email subject line.
        message: Plain text message body.

    Returns:
        True on success, False on failure.
    """
    client = EmailClient()

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; }}
            .header {{ background: #c0392b; color: white; padding: 20px; text-align: center; }}
            .body {{ padding: 20px; background: #f9f9f9; border-radius: 0 0 8px 8px; }}
            .footer {{ text-align: center; padding: 15px; color: #666; font-size: 0.85em; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h2>{subject}</h2>
        </div>
        <div class="body">
            <p>{message}</p>
        </div>
        <div class="footer">
            <p>Armitage Automation Alert</p>
        </div>
    </body>
    </html>
    """

    return client.send_email(recipients, subject, html, plain_content=message)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python email_client.py <recipient_email> [additional_emails...]")
        print("\nEnvironment variables required:")
        print("  SMTP_USER     - SMTP username (email address)")
        print("  SMTP_PASSWORD - SMTP password (app password for Gmail)")
        print("\nOptional environment variables:")
        print("  SMTP_HOST     - SMTP server (default: smtp.gmail.com)")
        print("  SMTP_PORT     - SMTP port (default: 587)")
        print("  SENDER_EMAIL  - Sender email (default: SMTP_USER)")
        sys.exit(1)

    recipients = sys.argv[1:]
    print(f"Sending digest report to: {recipients}")

    success = send_digest_report(recipients)
    if success:
        print("Digest email sent successfully!")
    else:
        print("Failed to send digest email")
        sys.exit(1)
