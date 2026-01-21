import os 
import requests
from dotenv import load_dotenv

load_dotenv()

FIRMABLE_API_KEY = os.getenv("FIRMABLE_API_KEY")
BASE_URL = "https://api.firmable.com/company"

def get_company_info(url, linkedin=False):
    headers = {
        "Authorization": f"Bearer {FIRMABLE_API_KEY}",
        "Accept": "application/json"
    }

    if linkedin: params = {"ln_url": url}
    else: params = {"website": url}

    headers = {
        "Authorization": f"Bearer {FIRMABLE_API_KEY}"
    }

    response = requests.get(BASE_URL, headers=headers, params=params)
    response.raise_for_status()

    data = response.json()
    extracted = {
        "hq_location": data.get("hq_location"),
        "linkedin": data.get("linkedin"),
        "industry": data.get("industries")[0]
    }

    return extracted

if __name__ == "__main__":
    print(get_company_info("https://www.labgroup.com.au/"))