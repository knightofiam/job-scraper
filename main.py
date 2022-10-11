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
JOB_SEARCH_KEYWORDS = "java"

# Refine the search by a specific skill keyword, separated by commas, spaces optional.
# Must exactly match a skill name from the website.
# Leave blank to match any skills.
SKILLS = "git, mongodb"

# Refine the search by a specific industry, spaces & forward slashes allowed.
# Must exactly match an industry name from the website.
# Leave blank to choose a scraped industry name from a list.
# Fill it out here to avoid being prompted to (optionally) choose an industry from a list.
INDUSTRY_NAME = ""

# Specify how old the listings are allowed to be, within range [0, 330]
MAX_DAYS_OLD = 30

# How frequently to re-scrape the jobs.
# The re-scrape will overwrite the old results in RESULTS_FILENAME.
UPDATE_INTERVAL_MINS = 10

# Where to store the job results.
# Creates new file each time in same directory script is run from.
RESULTS_FILENAME = "jobs.txt"

# How many results per page, within range [1, 200]
RESULTS_PER_PAGE = 200

# Adjust for performance, within range [1, 100]
MAX_THREADS = 50

# The first part of the URL that is static, from which additional dynamic parameters will be appended as need.
BASE_URL = "https://www.timesjobs.com/candidate/job-search.html?searchType=personalizedSearch&from=submit"


# ============================================================================== #

# Scrape all jobs from all pages using multiple threads since GIL gets released for I/O.
def scrape_jobs(total_pages, total_threads, user_prefs):
    # Run 1 worker thread per page - up to MAX_THREADS - for an exponential speedup.
    with concurrent.futures.ThreadPoolExecutor(max_workers=total_threads) as executor:
        # Collect results from all threads into one list.
        worker = partial(scrape_jobs_from_page, user_prefs=user_prefs)
        return list(chain.from_iterable(executor.map(worker, range(1, total_pages + 1))))


# Scrape all jobs from a single page.
def scrape_jobs_from_page(page_number, user_prefs):
    max_days_old = user_prefs["max_days_old"]
    skills = user_prefs["skills"]
    soup = scrape_html(get_url(user_prefs, page_number))
    job_results = soup.find_all("li", class_="clearfix job-bx wht-shd-bx")
    jobs_in_page = []

    for job_result in job_results:

        company_name = job_result.find("h3", class_="joblist-comp-name")

        # Remove unwanted nested tags from company name, e.g., "(More Jobs)" link
        for more_jobs in company_name.find_all("span", class_="comp-more"): more_jobs.decompose()

        all_skills = job_result.find("span",
                                     class_="srp-skills").text.replace(" ", "").replace(chr(34), "").strip().lower()

        # Skip jobs that don't have our specific skills.
        if skills and not set(skills.replace(" ", "").split(",")).issubset(all_skills.split(",")): continue

        published_date = job_result.find("span", class_="sim-posted")

        # Remove unwanted nested tags from published date, e.g., "Work from Home"
        for job_status in published_date.find_all("span", class_=re.compile("jobs-status")): job_status.decompose()

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
        elif len(matches := re.findall(r"(\b\d+\b)\Wday", published_date_text)) > 0:
            days_old = int(matches[0])  # Find jobs listed as 'x days ago', where x is any number of days.
        elif len(matches := re.findall(r"(\b\d+\b)\Wmonth", published_date_text)) > 0:
            days_old = int(matches[0]) * 30  # Find jobs listed as 'x months ago', where x is any number of months.

        # Skip jobs that have not been posted within our specified time frame.
        if days_old < 0 or days_old > max_days_old: continue

        date = today - timedelta(days=days_old)

        # Get the URL of the job details page.
        job_link = job_result.header.h2.a["href"]

        # Scrape the industry name from the job details page.
        industry_name = re.sub(" +", " ",
                               scrape_html(job_link).find("label", text="Industry:").next_sibling.next_sibling.text)

        text = f"Company: {company_name.text.title().strip()}\n" \
               f"Industry: {industry_name}\n" \
               f"Skills: {all_skills}\n" \
               f"Posted: {date.date()} ({published_date_text.replace('Posted ', '')})\n" \
               f"Link: {job_link}\n"

        jobs_in_page.append((days_old, text))

    return jobs_in_page


# Download the raw html into Beautiful Soup for parsing with lxml library.
def scrape_html(url):
    return BeautifulSoup(requests.get(url).text, "lxml")


# Get the full url with modified content/query parameters, ready for scraping.
def get_url(user_prefs, page_number=1, results_per_page_override=None):
    # results_per_page usually comes from user_prefs, but can be overriden, e.g., if only 1 result is needed.
    results_per_page = user_prefs["results_per_page"] if not results_per_page_override else results_per_page_override
    search_keywords = user_prefs["search_keywords"]
    industry = user_prefs["industry"]
    base_url = user_prefs["base_url"]

    p1 = get_url_results_per_page_param(results_per_page)
    p2 = get_url_page_number_param(page_number)
    p3 = get_url_search_keywords_param(search_keywords)
    p4 = get_url_industry_param(industry)

    return f"{base_url}{p1}{p2}{p3}{p4}"


# Prompt the user to significantly narrow down the search by choosing an industry from a scraped list.
# When left blank, the job search will include all industries.
def get_industry_from_user(industries, user_prefs):
    # Check if industry has already been chosen (i.e., the scraper has run more than once).
    if industry := user_prefs.get("industry"): return industry

    if not (industry_name := user_prefs.get("industry_name")):
        # Ask the user to choose an industry from the scraped list.
        print("Choose an industry number to narrow down your search, or leave blank for all:\n")
        [print(f'{i: >2}' + ' ' + line) for i, line in enumerate(sorted(k for k, v in industries.items()), start=1)]
        print()
        raw_industry_choice = input("Industry number (leave blank for all industries): ") or "0"
        industry_choice = int(raw_industry_choice) if raw_industry_choice.isdigit() else raw_industry_choice
        is_valid_choice = str(industry_choice).isdigit() and industry_choice <= len(industries)
        if not is_valid_choice: print(f"Warning: Ignoring invalid industry number: {industry_choice}.")
        industry_name = sorted(industries.keys())[industry_choice - 1] if industry_choice and is_valid_choice else ""

    industry_id = industries.get(industry_name, "") if industry_name else ""
    if industry_name and not industry_id: print(f"Warning: Ignoring invalid industry: {industry_name}.")

    return {"id": industry_id, "name": industry_name if industry_id else ""}


def get_industries(soup):
    industries = {}
    for i in soup.find_all('input', id=re.compile("ind_"), attrs={'type': 'radio', 'name': 'industryMap'}):
        url_params = {p[0]: p[1] for p in [v.split("=") for v in i.get("onclick").split("&")] if len(p) == 2}
        industry_name = url_params["gadLink"][:(url_params["gadLink"].find("'"))]
        industry_id = url_params["cboIndustry"]
        industries[industry_name] = industry_id
    return industries


def get_url_results_per_page_param(results_per_page):
    return f"&luceneResultSize={results_per_page}"


def get_url_page_number_param(page_number):
    return f"&sequence={page_number}"


def get_url_search_keywords_param(search_keywords):
    return f"&txtKeywords={search_keywords.replace(' ', '+')}"


def get_url_industry_param(industry):
    if not industry: return ""
    _id = industry['id']
    name = industry['name'].replace(' ', '+')
    return f"&clusterName=CLUSTER_IND&undokey=cboIndustry&cboIndustry={_id}&gadLink={name}"


def get_total_results(user_prefs):
    total_results_url = get_url(user_prefs, page_number=1, results_per_page_override=1)
    soup = scrape_html(total_results_url)
    # Misspelling of 'totol' is intentional here. Let's hope they never fix it...
    return soup, int(soup.find('span', id="totolResultCountsId").text)


def print_jobs(results, user_prefs):
    skills = user_prefs["skills"]
    max_days_old = user_prefs["max_days_old"]
    industry_name = user_prefs["industry"]["name"]
    search_keywords = user_prefs["search_keywords"]
    results_filename = user_prefs["results_filename"]
    results_per_page = user_prefs["results_per_page"]

    jobs = results["jobs"]
    total_pages = results["total_pages"]
    total_threads = results["total_threads"]
    total_time_secs = results["total_time_secs"]
    total_search_results = results["total_search_results"]

    # Open output_file & write the results.
    with open(f"{results_filename}", "w") as file:
        # Print the date & time first at the top of the file.
        file.write(f"{datetime.now()}\n\n")

        # Print scraping statistics summary.
        file.write(
            f"Searched {total_search_results:,} {search_keywords} jobs across {total_pages:,} pages @ {results_per_page:,} results per page, using {total_threads} threads.\n")

        # Print the total_jobs number of jobs found for our search keyword(s).
        file.write(f"\nFound {len(jobs):,} {search_keywords} jobs,\n")

        # Print the specified timeframe used to narrow the search.
        file.write(
            f"Posted within the last {'' if max_days_old == 1 else str(max_days_old) + ' '}day{'s' if max_days_old != 1 else ''},\n")

        # Print the specific skills used to further narrow the search.
        file.write(f"Within your industry: {industry_name or 'Any'},\n")

        # Print the specific skills used to further narrow the search.
        file.write(f"Matching your specific skills: {skills or 'Any'}.\n")

        # Print the number of seconds the search took.
        file.write(f"(Search took {total_time_secs:.0f} seconds.)\n\n")

        # Print the jobs, numbered, newest-to-oldest.
        file.write('\n'.join([f"{i:,}" + '.\n' + str(job[1]) for i, job in enumerate(jobs, start=1)]).strip())

    print(f"Done. Wrote results to {results_filename}.")


def main(user_prefs):
    results_per_page = user_prefs["results_per_page"]
    max_threads = user_prefs["max_threads"]
    search_keywords = user_prefs["search_keywords"]

    # Time the entire scraping & parsing process, to provide feedback to the user.
    start_time_secs = timer()

    # Get the total results as fast as possible, to figure out how many pages to scrape.
    soup, total_search_results = get_total_results(user_prefs)

    # Get the optional industry choice from the user to narrow down the search.
    industry = get_industry_from_user(get_industries(soup), user_prefs)
    industry_name = industry["name"]
    user_prefs["industry"] = industry
    user_prefs["industry_name"] = industry_name

    # Get the total results again as fast as possible, this time around we have the industry info to narrow things down.
    soup, total_search_results = get_total_results(user_prefs)

    # Results are split into pages, limited by RESULTS_PER_PAGE.
    total_pages = (total_search_results // results_per_page) + (1 if total_search_results % results_per_page > 0 else 0)

    # Limit to MAX_THREADS, since the total pages can easily get out of control.
    # One thread per page is only practical up to a certain point.
    total_threads = min(total_pages, max_threads)

    print(
        f"\nSearching {total_search_results:,} {search_keywords} jobs in {industry_name or 'all industries'} across {total_pages:,} pages @ {results_per_page:,} results per page, using {total_threads} threads...")

    # Perform the actual multithreaded scraping.
    jobs = scrape_jobs(total_pages, total_threads, user_prefs)

    # Sort by days old, with most recent first.
    jobs.sort(key=lambda x: x[0])

    # Get the total time in seconds that it took to scrape & process the results.
    total_time_secs = timer() - start_time_secs

    # Collect the results into a list for easier parameter passing
    results = {"jobs": jobs,
               "total_pages": total_pages,
               "total_threads": total_threads,
               "total_time_secs": total_time_secs,
               "total_search_results": total_search_results}

    # Print the results to file.
    print_jobs(results, user_prefs)

    return _user_prefs


if __name__ == "__main__":

    _user_prefs = {
        "skills": SKILLS,
        "industry": None,
        "base_url": BASE_URL,
        "max_threads": MAX_THREADS,
        "max_days_old": MAX_DAYS_OLD,
        "industry_name": INDUSTRY_NAME,
        "results_filename": RESULTS_FILENAME,
        "results_per_page": RESULTS_PER_PAGE,
        "search_keywords": JOB_SEARCH_KEYWORDS,
        "update_interval_mins": UPDATE_INTERVAL_MINS}

    _user_prefs = main(_user_prefs)

    while True:
        update_interval_mins = _user_prefs["update_interval_mins"]
        print(f"\nWaiting for {update_interval_mins} minutes...")
        print(f"Next update at {datetime.now() + timedelta(minutes=update_interval_mins)}")
        print("CTRL + C to quit.")
        time.sleep(update_interval_mins * 60)
        _user_prefs = main(_user_prefs)
