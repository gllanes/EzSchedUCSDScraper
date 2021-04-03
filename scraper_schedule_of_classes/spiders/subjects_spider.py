import json
import re
import sys

import scrapy
import urllib


SUBJECTS_BASE_URL = "https://act.ucsd.edu/scheduleOfClasses/subject-list.json"


class ScraperError(Exception):
    pass

class SubjectsSpider(scrapy.Spider):
    name = "subjects"
    custom_settings = {
        "ITEM_PIPELINES": {
            "scraper_schedule_of_classes.pipelines.SubjectCleanerPipeline": 100
        }
    }

    def __init__(self, quarter_code = None, *args, **kwargs):
        super(SubjectsSpider, self).__init__(*args, **kwargs)
        if not quarter_code:
            raise ScraperError(f"spider {self.name} needs a quarter.")
        self.quarter_code = quarter_code

    def start_requests(self):
        query = {
            "selectedTerm": self.quarter_code
        }
        url = f"{SUBJECTS_BASE_URL}?{urllib.parse.urlencode(query)}"
        yield scrapy.Request(url, callback=self.parse)


    def parse(self, response):
        if response.status != 200:
            raise ScraperError(f"Unable to get subjects: site responded with status code {response.status}")
        
        subjects_list = json.loads(response.text)

        yield {
            "subjects_codes_values": subjects_list
        }