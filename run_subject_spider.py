import sys

from scrapy.crawler import CrawlerProcess

from scraper_schedule_of_classes.db.db import DataAccess
from scraper_schedule_of_classes.spiders.subjects_spider import SubjectsSpider
from scraper_schedule_of_classes.spiders.subject_courses_spider import SubjectCoursesSpider

process = CrawlerProcess()


if __name__ == "__main__":
    
    quarter_code = "SP21"
    quarter_name = "Spring 2021"

    argv = sys.argv
    if len(argv) < 3:
        print(f"Either quarter code or name not specified. Getting subjects for "
            f"{quarter_code}-{quarter_name}")
    else:
        quarter_code = argv[1]
        quarter_name = argv[2]

    # Insert this quarter into the database first.
    conn = DataAccess.get_conn()
    with conn:
        DataAccess.insert_quarter(conn, quarter_code, quarter_name)

    # Need to get all the subjects first.
    process.crawl(SubjectsSpider, quarter_code = quarter_code)
    process.start()