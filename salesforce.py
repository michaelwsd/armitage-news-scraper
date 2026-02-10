import csv
import json
import logging
import os
import urllib.parse
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


def write_companies_csv(companies):
    csv_path = os.path.join(os.path.dirname(__file__), "data", "input", "companies.csv")
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["company", "location"])
        writer.writerows(companies)
    logger.info(f"Wrote {len(companies)} companies to {csv_path}")


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

    logger.info("Import complete")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    import_companies_from_salesforce()
