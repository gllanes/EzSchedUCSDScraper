import pathlib
import concurrent.futures
import pickle

from scraper_schedule_of_classes.spiders.subject_courses_spider \
    import SubjectCoursesSpider
from scrapy.http import Response

import urllib3

# Number of concurrent connections to keep.
CONCURRENT_N = 10
http = urllib3.PoolManager(CONCURRENT_N)


SCHEDULE_OF_CLASSES_URL = "https://act.ucsd.edu/scheduleOfClasses/scheduleOfClassesFacultyResult.htm"
SUBJECT_QUERY_STR = "selectedSubjects"
TERM_QUERY_STR = "selectedTerm"
SCHED_OPT_1_STR = "schedOption1"
SCHED_OPT_2_STR = "schedOption2"
PAGE_QUERY_STR = "page"


_TESTING_DIR = pathlib.Path(__file__).parent.absolute()
TEST_FILES_DIR = _TESTING_DIR / "test_data_files"

def fn_spider_parser_items(quarter_code, subject_code, page_num):
    fn = f"{quarter_code}_{subject_code}_page_{page_num}_spider_items"
    return TEST_FILES_DIR / fn


def save_spider_parser_items(quarter_code, subject_code, page_num):
    """
    Pickle a list of the items returned by the spider parser for one
    page.
    """
    # First, get the binary html data for this page.
    payload = {
        SUBJECT_QUERY_STR: subject_code,
        TERM_QUERY_STR: quarter_code,
        SCHED_OPT_1_STR: "true",
        SCHED_OPT_2_STR: "true", 
        PAGE_QUERY_STR: page_num
    }

    response = http.request(
        "GET",
        SCHEDULE_OF_CLASSES_URL,
        fields=payload
    )

    spider = SubjectCoursesSpider(quarter_code, subject_code)
    scrapy_response = Response(SCHEDULE_OF_CLASSES_URL, 200, body=response.data)
    items = list(spider.parse_extra_page(scrapy_response))

    fn = fn_spider_parser_items(quarter_code, subject_code, page_num)
    with open(fn, "wb") as f:
        pickle.dump(items, f)


def get_spider_parser_items(quarter_code, subject_code, page_num):
    fn = fn_spider_parser_items(quarter_code, subject_code, page_num)
    with open(fn, "rb") as f:
        items = pickle.load(f)
    return items


def fn_html(quarter_code, subject_code, page_num):
    """
    File name for html data given the quarter, subject, and page number.
    """
    fn = f"{quarter_code}_{subject_code}_page_{page_num}.html"
    return TEST_FILES_DIR / fn


def save_html_for_page(quarter_code, subject_code, page_num):
    print(f"getting html for {quarter_code} {subject_code} page {page_num}")
    payload = {
        SUBJECT_QUERY_STR: subject_code,
        TERM_QUERY_STR: quarter_code,
        SCHED_OPT_1_STR: "true",
        SCHED_OPT_2_STR: "true", 
        PAGE_QUERY_STR: page_num
    }

    response = http.request(
        "GET",
        SCHEDULE_OF_CLASSES_URL,
        fields=payload
    )
    
    fn = fn_html(quarter_code, subject_code, page_num)
    with open(fn, "wb") as f:
        f.write(response.data)


def get_html_binary(quarter_code, subject_code, page_num):

    fn = fn_html(quarter_code, subject_code, page_num)

    with open (fn, "rb") as f:
        data = f.read()
    return data


if __name__ == "__main__":
    # Get testing data here.
    quarters_subjects_pages = [
        ("WI21", "CSE", 1),     # Mostly normal - one lecture, couple sections, exam
        ("WI21", "PHYS", 8),    # Additional lecture, invalid length main meeting
        ("WI21", "ECE", 1)
    ]

    for q, s, p in quarters_subjects_pages:
        save_html_for_page(q, s, p)
        save_spider_parser_items(q, s, p)