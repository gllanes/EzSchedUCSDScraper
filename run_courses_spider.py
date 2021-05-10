import sys

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from scraper_schedule_of_classes.db.db import DataAccess
from scraper_schedule_of_classes.spiders.subjects_spider import SubjectsSpider
from scraper_schedule_of_classes.spiders.subject_courses_spider import SubjectCoursesSpider

settings = get_project_settings().copy()
settings.set("LOG_FILE", "courses_spider_out")

process = CrawlerProcess(settings)


if __name__ == "__main__":
    
    quarter_code = "SP21"

    argv = sys.argv
    if len(argv) < 2:
        print("No quarter specified. Getting course info for "
            f"{quarter_code}")
    else: 
        quarter_code = argv[1]

    process.crawl(SubjectCoursesSpider, quarter_code = quarter_code)
    process.start()