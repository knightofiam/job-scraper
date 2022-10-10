import concurrent.futures
import re
import requests
import time
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from functools import partial
from itertools import chain
from timeit import default_timer as timer

# ============================================================================== #

# CUSTOMIZABLE PREFERENCES:

# Search for any type of job, any number of words, separated by spaces
JOB_SEARCH_KEYWORDS = "java"  # <= CHANGE THIS

# Refine the search by a specific skill keyword, separated by commas, spaces optional
SKILLS = "enterprisesecurity"  # "git, mongodb, devops"  # <= CHANGE THIS

# Specify how old the listings are allowed to be, within range [0, 330]
MAX_DAYS_OLD = 30  # <= CHANGE THIS

# How many results per page, within range [1, 200]
RESULTS_PER_PAGE = 200  # <= CHANGE THIS

# Adjust for performance, within range [1, 100]
MAX_THREADS = 50  # <= CHANGE THIS

# Where to store the job results.
# Creates new file each time in same directory script is run from.
RESULTS_FILENAME = "jobs.txt"  # <= CHANGE THIS


# ============================================================================== #


def scrape_page(page_number, _industry_id, _industry_name):
    url = f"https://www.timesjobs.com/candidate/job-search.html?searchType=personalizedSearch&from=submit&luceneResultSize={RESULTS_PER_PAGE}&sequence={page_number}&txtKeywords={JOB_SEARCH_KEYWORDS.replace(' ', '+')}&clusterName=CLUSTER_IND&undokey=cboIndustry&cboIndustry={_industry_id}&gadLink={(_industry_name.replace(' ', '+'))}"
    html_text = requests.get(url).text
    soup = BeautifulSoup(html_text, "lxml")
    job_results = soup.find_all('li', class_="clearfix job-bx wht-shd-bx")
    jobs_in_page = []
    for job_result in job_results:
        company_name = job_result.find('h3', class_="joblist-comp-name")

        # Remove unwanted nested tags from company name, e.g., "(More Jobs)" link
        for more_jobs in company_name.find_all('span', class_="comp-more"): more_jobs.decompose()

        skills = job_result.find('span', class_="srp-skills").text.replace(' ', '').replace(chr(34), '').strip().lower()

        # Skip jobs that don't have our specific skills.
        if not set(SKILLS.replace(' ', '').split(',')).issubset(skills.split(',')): continue

        published_date = job_result.find('span', class_="sim-posted")

        # Remove unwanted nested tags from published date, e.g., "Work from Home"
        for job_status in published_date.find_all('span', class_=re.compile("jobs-status")): job_status.decompose()

        today = datetime.today()
        published_date_text = published_date.text.strip()
        days_old = -1

        # Convert the published date description to an exact number of days.
        if "today" in published_date_text:
            days_old = 0
        elif "few days" in published_date_text:
            days_old = 4  # Some jobs are listed as '3 days ago', use 4 for 'few days ago' to sort them below those.
        elif "a month" in published_date_text:
            days_old = 30  # Some jobs are listed as 'a month', instead of '1 month', so handle it separately.
        elif len(matches := re.findall(r'(\b\d+\b)\Wday', published_date_text)) > 0:
            days_old = int(matches[0])  # Find jobs listed as 'x days ago', where x is any number of days.
        elif len(matches := re.findall(r'(\b\d+\b)\Wmonth', published_date_text)) > 0:
            days_old = int(matches[0]) * 30  # Find jobs listed as 'x months ago', where x is any number of months.

        # Skip jobs that have not been posted within our specified time frame.
        if days_old < 0 or days_old > MAX_DAYS_OLD: continue

        date = today - timedelta(days=days_old)
        text = f"Company: {company_name.text.title().strip()}\n" \
               f"Industry: {_industry_name}\n" \
               f"Skills: {skills}\n" \
               f"Posted: {date.date()} ({published_date_text.replace('Posted ', '')})\n"
        jobs_in_page.append((days_old, text))

    return jobs_in_page


def get_industry(soup):
    industries = set()
    for i in soup.find_all('input', id=re.compile("ind_"), attrs={'type': 'radio', 'name': 'industryMap'}):
        url_params = {p[0]: p[1] for p in [v.split("=") for v in i.get("onclick").split("&")] if len(p) == 2}
        industries.add((url_params["gadLink"][:(url_params["gadLink"].find("'"))], url_params["cboIndustry"]))
    print("Choose an industry number to narrow down your search, or leave blank for none:\n")
    [print(f'{i: >2}' + ' ' + line) for i, line in enumerate(sorted(x for x, y in industries), start=1)]
    print()
    industry_choice = int(input("Industry number (blank for none): ") or "0")
    _industry_name = sorted(industries)[industry_choice - 1][0] if industry_choice else ""
    _industry_id = dict(industries)[_industry_name] if _industry_name else ""
    return _industry_id, _industry_name


def get_total_results(industry_id="", industry_name=""):
    total_results_url = f"https://www.timesjobs.com/candidate/job-search.html?searchType=personalizedSearch&from=submit&luceneResultSize=1&sequence=1&txtKeywords={JOB_SEARCH_KEYWORDS.replace(' ', '+')}&clusterName=CLUSTER_IND&undokey=cboIndustry&cboIndustry={industry_id}&gadLink={(industry_name.replace(' ', '+'))}"
    html_text = requests.get(total_results_url).text
    soup = BeautifulSoup(html_text, "lxml")
    # Get the total results as fast as possible, to figure out how many pages to scrape.
    # Misspelling of 'totol' is intentional here. Let's hope they never fix it...
    return soup, int(soup.find('span', id="totolResultCountsId").text)


def main(_industry_id="", _industry_name=""):
    # Time the entire scraping & parsing process, to provide feedback to the user.
    start_time = timer()

    # Get the total results as fast as possible, to figure out how many pages to scrape.
    soup, total_results = get_total_results()

    # Get the industry name & id for the URL to narrow down the search.

    if not (_industry_id and _industry_name):
        _industry_id, _industry_name = get_industry(soup)

    # Get the total results again as fast as possible, this time around we have the industry info to narrow things down.
    soup, total_results = get_total_results(_industry_id, _industry_name)

    # Results are split into pages, limited by RESULTS_PER_PAGE.
    total_pages = (total_results // RESULTS_PER_PAGE) + (1 if total_results % RESULTS_PER_PAGE > 0 else 0)

    # Limit to MAX_THREADS, since the total pages can easily get out of control.
    # One thread per page is only practical up to a certain point.
    threads = min(total_pages, MAX_THREADS)

    print(
        f"\nSearching {total_results:,} {JOB_SEARCH_KEYWORDS} jobs across {total_pages:,} pages @ {RESULTS_PER_PAGE:,} results per page, using {threads} threads...")

    # Run 1 worker thread per page - up to MAX_THREADS - for an exponential speedup.
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        # Collect results from all threads into one list.
        worker = partial(scrape_page, _industry_id=_industry_id, _industry_name=_industry_name)
        jobs = list(chain.from_iterable(executor.map(worker, range(1, total_pages + 1))))

    # Open output_file and write the results.
    with open(f'{RESULTS_FILENAME}', 'w') as file:
        file.write(f"{datetime.now()}\n\n")

        # Print scraping statistics summary.
        file.write(
            f"Searched {total_results:,} {JOB_SEARCH_KEYWORDS} jobs across {total_pages:,} pages @ {RESULTS_PER_PAGE:,} results per page, using {threads} threads.\n")

        # Sort by days old, with most recent first.
        jobs.sort(key=lambda x: x[0])

        # Print the total number of jobs found for our search keyword(s).
        file.write(f"\nFound {len(jobs):,} {JOB_SEARCH_KEYWORDS} jobs,\n")

        # Print the specified timeframe used to narrow the search.
        file.write(
            f"Posted within the last {'' if MAX_DAYS_OLD == 1 else str(MAX_DAYS_OLD) + ' '}day{'s' if MAX_DAYS_OLD != 1 else ''},\n")

        # Print the specific skills used to further narrow the search.
        file.write(f"Within your industry: {_industry_name}\n")

        # Print the specific skills used to further narrow the search.
        file.write(f"Matching your specific skills: {SKILLS}\n")

        # Print the number of seconds the search took.
        file.write(f"(Search took {timer() - start_time:.0f} seconds.)\n\n")

        # Print the jobs, numbered, newest-to-oldest.
        file.write('\n'.join([f"{i:,}" + '.\n' + str(job[1]) for i, job in enumerate(jobs, start=1)]).strip())

    print(f"Done. Wrote results to {RESULTS_FILENAME}.")

    return _industry_id, _industry_name


if __name__ == "__main__":
    industry_id, industry_name = main()
    while True:
        wait_time_mins = .1
        print(f"\nWaiting for {wait_time_mins} minutes...")
        print(f"Next update at {datetime.now() + timedelta(minutes=10)}")
        print("CTRL + C to quit.")
        time.sleep(wait_time_mins * 60)
        industry_id, industry_name = main(industry_id, industry_name)
