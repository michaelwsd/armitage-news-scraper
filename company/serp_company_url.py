import os 
import serpapi
from dotenv import load_dotenv
from urllib.parse import urlparse

load_dotenv()
API_KEY = os.getenv("SERP_API_KEY")

def clean_domain(url):
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        return urlparse(url).netloc.replace('www.', '').lower()

def get_company_url(name, location):
  params = {
    "engine": "google",
    "location": "Australia",
    "google_domain": "google.com.au",
    "hl": "en",
    "gl": "au",
    "q": f"{name} {location}",
    "api_key": API_KEY
  }

  client = serpapi.Client(api_key=params["api_key"])
  results = client.search(params)
  return clean_domain(results["organic_results"][0]["link"])

if __name__ == "__main__":
  print(get_company_url("LAB Group", "Melbourne"))