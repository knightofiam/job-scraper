import itertools
import concurrent.futures
import requests
import re
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from timeit import default_timer as timer

# ============================================================================== #

# CUSTOMIZABLE PREFERENCES:

# Search for any type of job, any number of words, separated by spaces
JOB_SEARCH_KEYWORDS = "java"  # <= CHANGE THIS

# Refine the search by a specific skill keyword, separated by commas, spaces optional
SKILLS = "git, mongodb, devops"  # <= CHANGE THIS

# Specify how old the listings are allowed to be, within range [0, 330]
MAX_DAYS_OLD = 5  # <= CHANGE THIS

# How many results per page, within range [1, 200]
RESULTS_PER_PAGE = 200  # <= CHANGE THIS

# Adjust for performance
MAX_THREADS = 50


# ============================================================================== #


def scrape_page(page_number):
    url = f"https://www.timesjobs.com/candidate/job-search.html?searchType=personalizedSearch&from=submit&luceneResultSize={RESULTS_PER_PAGE}&sequence={page_number}&txtKeywords={JOB_SEARCH_KEYWORDS.replace(' ', '%20')}"
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
               f"Skills: {skills}\n" \
               f"Posted: {date.date()} ({published_date_text.replace('Posted ', '')})\n"
        jobs_in_page.append((days_old, text))

    return jobs_in_page


def main():
    # Time the entire scraping & parsing process, to provide feedback to the user.
    start_time = timer()

    # URL for getting just the total number of results as quickly as possible (i.e., 1 page, 1 result).
    total_results_url = f"https://www.timesjobs.com/candidate/job-search.html?searchType=personalizedSearch&from=submit&luceneResultSize=1&sequence=1&txtKeywords={JOB_SEARCH_KEYWORDS.replace(' ', '%20')}"

    # Get the total results as fast as possible, to figure out how many pages to scrape.
    # Misspelling of 'totol' is intentional here. Let's hope they never fix it...
    total_results = int(
        BeautifulSoup(requests.get(total_results_url).text, "lxml").find('span', id="totolResultCountsId").text)

    # Results are split into pages, limited by RESULTS_PER_PAGE.
    total_pages = (total_results // RESULTS_PER_PAGE) + (1 if total_results % RESULTS_PER_PAGE > 0 else 0)

    # Limit to MAX_THREADS, since the total pages can easily get out of control.
    # One thread per page is only practical up to a certain point.
    threads = min(total_pages, MAX_THREADS)

    # Print scraping statistics summary.
    print(
        f"Scraping {total_results:,} jobs across {total_pages:,} pages @ {RESULTS_PER_PAGE:,} results per page, using {threads} threads...")

    # Run 1 worker thread per page - up to MAX_THREADS - for an exponential speedup.
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        # Collect results from all threads into one list.
        jobs = list(itertools.chain.from_iterable(executor.map(scrape_page, range(1, total_pages + 1))))

    # Sort by days old, with most recent first.
    jobs.sort(key=lambda x: x[0])

    # Print the total number of jobs found for our search keyword(s).
    print(f"\nFound {len(jobs):,} {JOB_SEARCH_KEYWORDS} jobs,")

    # Print the specified timeframe used to narrow the search.
    print(
        f"Posted within the last {'' if MAX_DAYS_OLD == 1 else str(MAX_DAYS_OLD) + ' '}day{'s' if MAX_DAYS_OLD != 1 else ''},")

    # Print the specific skills used to further narrow the search.
    print(f"Matching your specific skills: {SKILLS}")

    # Print the number of seconds the search took.
    print(f"(Search took {timer() - start_time:.0f} seconds.)\n")

    # Print the jobs, numbered, newest-to-oldest.
    print('\n'.join([f"{i:,}" + '.\n' + job[1] for i, job in enumerate(jobs, start=1)]).strip())


if __name__ == "__main__":
    main()
