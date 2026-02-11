import csv
import json
import logging
import os
import urllib.parse
from datetime import datetime
import requests
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv()

API_VERSION = "v62.0"
TARGET_REPORTS = ["GOWT Ultra High's", "GOWT High's"]

domain = os.getenv("SALESFORCE_DOMAIN")


def get_access_token():
    payload = {
        'grant_type': 'client_credentials',
        'client_id': os.getenv('CONSUMER_KEY'),
        'client_secret': os.getenv('CONSUMER_SECRET')
    }
    return requests.post(f"{domain}/services/oauth2/token", data=payload).json()['access_token']


def sf_get(endpoint, token):
    headers = {"Authorization": f"Bearer {token}"}
    return requests.get(f"{domain}/services/data/{API_VERSION}/{endpoint}", headers=headers).json()


def get_dashboard_ids(token):
    response = sf_get("analytics/dashboards", token)
    dashboards = response.get("dashboards", response) if isinstance(response, dict) else response
    return [db.get("id") or db.get("Id") for db in dashboards]


def extract_companies(token, dashboard_id):
    detail = sf_get(f"analytics/dashboards/{dashboard_id}", token)
    components = detail.get("componentData", detail.get("components", []))
    companies = []

    for comp in components:
        if "reportResult" not in comp:
            continue
        report = comp["reportResult"]
        metadata = report.get("reportMetadata", {})
        if metadata.get("name", "") not in TARGET_REPORTS:
            continue

        columns = metadata.get("detailColumns", [])
        name_idx = next((i for i, c in enumerate(columns) if c == "OPPORTUNITY_NAME"), None)
        addr_idx = next((i for i, c in enumerate(columns) if c == "Opportunity.fid5__c"), None)

        for fact in report.get("factMap", {}).values():
            for row in fact.get("rows", []):
                cells = row.get("dataCells", [])
                company = cells[name_idx].get("label", "") if name_idx is not None else ""
                location = cells[addr_idx].get("label", "") if addr_idx is not None else ""
                companies.append((company, location))

    return companies


def get_owner_emails(token, company_names):
    """Batch SOQL query to get the opportunity owner email for each company."""
    company_to_owner = {name: None for name in company_names}

    escaped_names = [name.replace("'", "\\'") for name in company_names]
    names_clause = ",".join(f"'{n}'" for n in escaped_names)
    soql = f"SELECT Name, Owner.Email FROM Opportunity WHERE Name IN ({names_clause})"
    endpoint = f"query/?q={urllib.parse.quote(soql)}"

    try:
        result = sf_get(endpoint, token)
        for record in result.get("records", []):
            name = record.get("Name")
            owner = record.get("Owner", {})
            email = owner.get("Email") if isinstance(owner, dict) else None
            if name in company_to_owner and company_to_owner[name] is None:
                company_to_owner[name] = email
    except Exception as e:
        logger.error(f"Batch owner query failed: {e}")

    unmapped = [n for n, e in company_to_owner.items() if e is None]
    if unmapped:
        logger.warning(f"Could not resolve owner for: {unmapped}")

    return company_to_owner


def write_owner_mapping(company_to_owner):
    """Write owner_email -> [company_names] mapping to JSON."""
    owner_to_companies = {}
    unmapped = []

    for company, email in company_to_owner.items():
        if email:
            owner_to_companies.setdefault(email, []).append(company)
        else:
            unmapped.append(company)

    mapping = {
        "owner_to_companies": owner_to_companies,
        "unmapped_companies": unmapped,
    }

    mapping_path = os.path.join(os.path.dirname(__file__), "data", "input", "owner_mapping.json")
    os.makedirs(os.path.dirname(mapping_path), exist_ok=True)
    with open(mapping_path, "w") as f:
        json.dump(mapping, f, indent=2)

    logger.info(f"Wrote owner mapping: {len(owner_to_companies)} owners, {len(unmapped)} unmapped")


def get_primary_contacts(token, company_names):
    """Batch SOQL query to get the primary contact name for each company."""
    company_to_contact = {name: None for name in company_names}

    escaped_names = [name.replace("'", "\\'") for name in company_names]
    names_clause = ",".join(f"'{n}'" for n in escaped_names)
    soql = (
        "SELECT Opportunity.Name, Contact.Name "
        "FROM OpportunityContactRole "
        f"WHERE Opportunity.Name IN ({names_clause}) "
        "AND IsPrimary = true"
    )
    endpoint = f"query/?q={urllib.parse.quote(soql)}"

    try:
        result = sf_get(endpoint, token)
        for record in result.get("records", []):
            opp_name = record.get("Opportunity", {}).get("Name")
            contact_name = record.get("Contact", {}).get("Name")
            if opp_name in company_to_contact and company_to_contact[opp_name] is None:
                company_to_contact[opp_name] = contact_name
    except Exception as e:
        logger.error(f"Batch contact query failed: {e}")

    unmapped = [n for n, c in company_to_contact.items() if c is None]
    if unmapped:
        logger.warning(f"Could not resolve primary contact for: {unmapped}")

    return company_to_contact


def write_contact_mapping(company_to_contact):
    """Write company_name -> contact_name mapping to JSON."""
    mapping_path = os.path.join(os.path.dirname(__file__), "data", "input", "contact_mapping.json")
    os.makedirs(os.path.dirname(mapping_path), exist_ok=True)
    with open(mapping_path, "w") as f:
        json.dump(company_to_contact, f, indent=2)

    mapped = sum(1 for v in company_to_contact.values() if v is not None)
    logger.info(f"Wrote contact mapping: {mapped} mapped, {len(company_to_contact) - mapped} unmapped")


def write_companies_csv(companies):
    csv_path = os.path.join(os.path.dirname(__file__), "data", "input", "companies.csv")
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["company", "location"])
        writer.writerows(companies)
    logger.info(f"Wrote {len(companies)} companies to {csv_path}")


def sf_patch(endpoint, token, payload):
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    return requests.patch(f"{domain}/services/data/{API_VERSION}/{endpoint}", headers=headers, json=payload)


def _get_opportunity_ids(token, company_names):
    """Batch SOQL query to get Opportunity IDs by name."""
    escaped = [name.replace("'", "\\'") for name in company_names]
    names_clause = ",".join(f"'{n}'" for n in escaped)
    soql = f"SELECT Id, Name FROM Opportunity WHERE Name IN ({names_clause})"
    endpoint = f"query/?q={urllib.parse.quote(soql)}"

    name_to_id = {}
    try:
        result = sf_get(endpoint, token)
        for record in result.get("records", []):
            name_to_id[record["Name"]] = record["Id"]
    except Exception as e:
        logger.error(f"Failed to query Opportunity IDs: {e}")

    return name_to_id


def _section_header(title):
    return (
        f'<div style="margin-top:16px; margin-bottom:8px;">'
        f'<b style="font-size:15px; color:#333;">{title}</b>'
        f'</div><hr style="border:none; border-top:1px solid #ccc; margin:0 0 10px 0;"/>'
    )


def _last_updated_banner():
    now = datetime.now().strftime("%d %b %Y, %I:%M %p")
    return (
        f'<div style="text-align:right; color:#999; font-size:11px; margin-bottom:8px;">'
        f'Last updated: {now}'
        f'</div>'
    )


def _format_news_html(data):
    """Format articles and LinkedIn posts as HTML."""
    html = _last_updated_banner()
    html += _section_header("Articles")
    if data.get("articles"):
        for i, article in enumerate(data["articles"]):
            html += (
                f'<div style="margin-bottom:12px; padding:8px; background:#f9f9f9; border-left:3px solid #4a90d9;">'
                f'<b>{article["headline"]}</b><br/>'
                f'<span style="color:#666; font-size:12px;">{article.get("date", "")} &bull; {article.get("growth_type", "")}</span><br/>'
                f'<span>{article["summary"]}</span><br/>'
            )
            if article.get("source_url"):
                html += f'<a href="{article["source_url"]}" style="color:#4a90d9;">View source</a>'
            html += '</div>'
    else:
        html += '<div style="padding:8px; color:#888;"><i>No articles found for this period.</i></div>'

    html += _section_header("LinkedIn Posts")
    if data.get("posts"):
        for post in data["posts"]:
            html += (
                f'<div style="margin-bottom:12px; padding:8px; background:#f9f9f9; border-left:3px solid #0a66c2;">'
                f'<b>{post.get("growth_type", "")}</b>'
                f'<span style="color:#666; font-size:12px;"> &bull; {post.get("date", "")}</span><br/>'
                f'<span>{post["summary"]}</span>'
                f'</div>'
            )
    else:
        html += '<div style="padding:8px; color:#888;"><i>No LinkedIn posts found for this period.</i></div>'

    return html


def _format_contact_activity_html(data):
    """Format contact LinkedIn activity as HTML for a separate Salesforce field."""
    try:
        contact_name = data.get("contact_name")
        contact_posts = data.get("contact_posts") or []

        html = _last_updated_banner()
        contact_title = f"Contact Activity: {contact_name}" if contact_name else "Contact LinkedIn Activity"
        html += _section_header(contact_title)

        if contact_posts:
            for post in contact_posts:
                html += (
                    f'<div style="margin-bottom:12px; padding:8px; background:#f9f9f9; border-left:3px solid #e67e22;">'
                    f'<span style="color:#666; font-size:12px;">{post.get("date", "")}</span>'
                    f'<span style="color:#666; font-size:12px;"> &bull; {post.get("topic", "")}</span><br/>'
                    f'<span>{post.get("summary", "")}</span>'
                    f'</div>'
                )
        else:
            if contact_name:
                html += f'<div style="padding:8px; color:#888;"><i>No recent LinkedIn activity found for {contact_name}.</i></div>'
            else:
                html += '<div style="padding:8px; color:#888;"><i>No primary contact identified for this opportunity.</i></div>'

        return html
    except Exception as e:
        logger.error(f"Error formatting contact activity HTML: {e}")
        return '<div style="padding:8px; color:#888;"><i>Contact activity unavailable.</i></div>'


def _format_actions_html(data):
    """Format potential actions and outreach message as HTML."""
    html = _section_header("Potential Actions")
    if data.get("potential_actions"):
        html += '<ol style="padding-left:20px; margin:8px 0;">'
        for action in data["potential_actions"]:
            html += f'<li style="margin-bottom:8px;">{action}</li>'
        html += '</ol>'
    else:
        html += '<div style="padding:8px; color:#888;"><i>No actions generated for this period.</i></div>'

    html += _section_header("Outreach Message")
    if data.get("message"):
        html += (
            f'<div style="padding:10px; background:#f9f9f9; border-left:3px solid #5cb85c; white-space:pre-wrap;">'
            f'{data["message"]}'
            f'</div>'
        )
    else:
        html += '<div style="padding:8px; color:#888;"><i>No outreach message generated for this period.</i></div>'

    return html


def push_to_salesforce(output_dir=None):
    """Push all scraped company data to Salesforce Opportunity fields."""
    if output_dir is None:
        output_dir = os.path.join(os.path.dirname(__file__), "data", "output")

    logger.info("Starting Salesforce push")
    token = get_access_token()

    # Load all output JSON files
    json_files = [f for f in os.listdir(output_dir) if f.endswith(".json")]
    if not json_files:
        logger.warning("No JSON files found in output directory")
        return

    company_data = {}
    for filename in json_files:
        try:
            with open(os.path.join(output_dir, filename)) as f:
                data = json.load(f)
            if isinstance(data, dict) and "company" in data:
                company_data[data["company"]] = data
        except Exception as e:
            logger.error(f"Failed to load {filename}, skipping: {e}")

    logger.info(f"Loaded {len(company_data)} company reports")

    # Get Opportunity IDs for all companies
    name_to_id = _get_opportunity_ids(token, list(company_data.keys()))
    logger.info(f"Matched {len(name_to_id)} companies to Opportunities")

    updated = 0
    failed = 0
    for company_name, data in company_data.items():
        try:
            opp_id = name_to_id.get(company_name)
            if not opp_id:
                logger.warning(f"No Opportunity found for: {company_name}")
                failed += 1
                continue

            payload = {
                "Growth_News__c": _format_news_html(data),
                "Growth_Actions__c": _format_actions_html(data),
                "P__c": _format_contact_activity_html(data),
            }

            resp = sf_patch(f"sobjects/Opportunity/{opp_id}", token, payload)
            if resp.status_code == 204:
                logger.info(f"Updated: {company_name}")
                updated += 1
            else:
                logger.error(f"Failed to update {company_name}: {resp.status_code} {resp.text}")
                failed += 1
        except Exception as e:
            logger.error(f"Error processing {company_name}, skipping: {e}")
            failed += 1

    logger.info(f"Push complete: {updated} updated, {failed} failed")


def import_companies_from_salesforce():
    logger.info("Starting Salesforce company import")
    token = get_access_token()
    logger.info("Authenticated successfully")

    dashboard_ids = get_dashboard_ids(token)
    logger.info(f"Found {len(dashboard_ids)} dashboard(s)")

    companies = []
    for dashboard_id in dashboard_ids:
        extracted = extract_companies(token, dashboard_id)
        logger.info(f"Dashboard {dashboard_id}: extracted {len(extracted)} companies")
        companies.extend(extracted)

    logger.info(f"Total companies extracted: {len(companies)}")
    write_companies_csv(companies)

    company_names = list(set(c[0] for c in companies))
    company_to_owner = get_owner_emails(token, company_names)
    write_owner_mapping(company_to_owner)

    try:
        company_to_contact = get_primary_contacts(token, company_names)
        write_contact_mapping(company_to_contact)
    except Exception as e:
        logger.error(f"Contact mapping failed (non-fatal, continuing): {e}")

    logger.info("Import complete")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    import_companies_from_salesforce()
