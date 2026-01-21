from .serp_company_url import get_company_url
from .firmable_data import get_company_info

def get_info(company_name, company_location):
    company_url = get_company_url(company_name, company_location)
    company_info = get_company_info(company_url)
    
    company_info['website'] = company_url 
    company_info['name'] = company_name
    company_info['city'] = company_location

    return company_info

if __name__ == "__main__":
    print(get_info("GRC Solutions", "Sydney"))