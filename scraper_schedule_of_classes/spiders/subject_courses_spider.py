import re
import datetime

import scrapy
import urllib
from scrapy.loader import ItemLoader
from more_itertools import split_before
from bs4 import BeautifulSoup

import scraper_schedule_of_classes.errors as errors
from scraper_schedule_of_classes.items \
    import CourseMeetingsUncategorized, CourseMeetingsUncategorizedLoader, Meeting, MeetingLoader
import scraper_schedule_of_classes.utils as utils
from scraper_schedule_of_classes.db.db import DataAccess


SCHEDULE_OF_CLASSES_URL = "https://act.ucsd.edu/scheduleOfClasses/scheduleOfClassesFacultyResult.htm"
SUBJECT_QUERY_STR = "selectedSubjects"
TERM_QUERY_STR = "selectedTerm"
SCHED_OPT_1_STR = "schedOption1"
SCHED_OPT_2_STR = "schedOption2"
PAGE_QUERY_STR = "page"

PAGE_NUM_REGEX = re.compile(r"Page\s+\([0-9]+\s+of\s+([0-9+])\)")


XPATH_CRSHEADER = "//tr[td/@class='crsheader']"
XPATH_MEETING = "//tr[(@class='sectxt' or @class='nonenrtxt') and count(td)>=4]"
XPATH_ALL = " | ".join((XPATH_CRSHEADER, XPATH_MEETING))


SECTXT_VALID_TDS_LEN = 12
NONENRTXT_VALID_TDS_LEN = 10
IND_SEC_ID = 1
IND_MEETING_TYPE = 2
IND_SEC_NUM_OR_DATE = 3
IND_DAYS = 4
IND_TIME = 5
IND_BLDG = 6
IND_ROOM = 7
IND_INSTRUCTOR = 8
IND_SEATS_AVAIL = 9


class SubjectCoursesSpider(scrapy.Spider):
    """
    A spider that scrapes all all of the courses/meetings
    for a subject in a given term.
    """
    name = "subject_courses"

    def __init__(self, quarter_code=None, *args, **kwargs):
        super(SubjectCoursesSpider, self).__init__(*args, **kwargs)
        if not quarter_code:
            raise errors.MissingQuarterError(f"The {self.name} spider needs a quarter.")
        self.quarter_code = quarter_code

        # Query database for all subjects. FIXME: get all subjects instead.
        self.conn = DataAccess.get_conn()
        with self.conn:
            self.subject_codes = DataAccess.get_all_subjects(self.conn)

    
    def closed(self, reason):
        print('subject courses spider closing.')
        DataAccess.put_conn(self.conn)
        

    def start_requests(self):

        for subject_code in self.subject_codes:

            payload = {
                SUBJECT_QUERY_STR: subject_code,
                TERM_QUERY_STR: self.quarter_code, 
                SCHED_OPT_1_STR: "true",
                SCHED_OPT_2_STR: "true"
            }

            query = urllib.parse.urlencode(payload)
            url = f"{SCHEDULE_OF_CLASSES_URL}?{query}"

            # Request for the first page of each subject.
            yield scrapy.Request(url, self.parse, cb_kwargs=dict(subject_code=subject_code))


    def parse(self, response, subject_code):
        """
        Parse html for the number of pages, then create more requests
        for additional pages if needed.
        """

        soup = BeautifulSoup(response.body, "lxml")
        num_pages = self.get_num_pages(soup)
        if num_pages == 0:
            return
        
        # Dispatch the rest of the requests (pages 2 to num_pages)
        # Don't need to request the first page again.
        for i in range (2, num_pages + 1):
            payload = {
                SUBJECT_QUERY_STR: subject_code,
                TERM_QUERY_STR: self.quarter_code, 
                SCHED_OPT_1_STR: "true",
                SCHED_OPT_2_STR: "true",
                PAGE_QUERY_STR: i
            }

            query = urllib.parse.urlencode(payload)
            url = f"{SCHEDULE_OF_CLASSES_URL}?{query}"

            yield scrapy.Request(url, self.parse_extra_page, cb_kwargs=dict(subject_code=subject_code))

        # Get all the tags with course information
        tags = soup.find_all(utils.tag_matches_any)

        # Group by course header.
        for item in self.course_meeting_items(tags, subject_code):
            yield item
        

    def get_num_pages(self, soup):
        """
        Get the number of pages given the soup for the response.
        """
        tds = soup.select("td[align='right']")
        num_pages = 0
        for td in tds:
            match = PAGE_NUM_REGEX.search(td.text)
            if match:
                num_pages = int(match.group(1))
        return num_pages

        
    def parse_extra_page(self, response, subject_code):
        """
        Parser for pages beyond the first.
        """

        soup = BeautifulSoup(response.body, "lxml")    
        # Get all the tags with course information
        tags = soup.find_all(utils.tag_matches_any)

        # Group by course header.
        for item in self.course_meeting_items(tags, subject_code):
            yield item

    
    def course_meeting_items(self, tags, subject_code):
        """
        Given some all of the matching tags for one page, 
        return rough items representing all the meetings
        for a particular course/section group.
        """

        # Crsheader doesn't have class. Group tags by crsheader.
        tags_grouped = self.group_tags(tags)

        for group in tags_grouped:
            course_item = self.build_item_from_group(group, subject_code)
            if course_item:
                yield course_item


    def group_tags(self, tags):
        return split_before(tags, lambda tag: not tag.has_attr("class"))


    def build_item_from_group(self, tag_group, subject_code):
        """
        Build a course item from the given group of selectors.
        The first row will always be a crsheader.
        The rest of the rows will be meetings.
        """

        # If there is only one row, it is just a crsheader
        # without any meetings. Not useful.
        if len(tag_group) == 1:
            return None

        crsheader_tag = tag_group[0]
        loader = CourseMeetingsUncategorizedLoader(item = CourseMeetingsUncategorized())
        loader.add_value("quarter_code", self.quarter_code)
        loader.add_value("subj_code", subject_code)

        crsheader_tag_tds = crsheader_tag.find_all("td")
        # course number comes from index 2 td
        loader.add_value("number", crsheader_tag_tds[1].text)
        # course name comes from index 3 td. 
        # Check first if there is a link - if so, the course name is in there.
        a = crsheader_tag_tds[2].a
        if a:
            loader.add_value("title", a.text)
        else:
            loader.add_value("title", crsheader_tag_tds[2].text)


        first_meeting_tag = tag_group[1]
        try:
            first_meeting_item = self.build_item_from_meeting(first_meeting_tag)
            loader.add_value("first_meeting", first_meeting_item)
        # First main meeting invalid - usually because cancelled.
        except errors.ScraperError:
            return None


        # # Building an item for each subsequent tag.
        for tag in tag_group[2:]:
            try:
                meeting_item = self.build_item_from_meeting(tag)
            except errors.ScraperError:
                # Just continue with the next meeting if this row is unparseable.
                continue

            tr_class = (tag["class"])[0]
            if tr_class == "sectxt":
                loader.add_value("sectxt_meetings", meeting_item)
            else:
                loader.add_value("nonenrtxt_meetings", meeting_item)

        return loader.load_item()


    def build_item_from_meeting(self, tag):
        """
        Build an uncategorized meeting item from each selector.
        They will have the correct types.
        """

        # Check if the meeting was cancelled.
        if "Cancelled" in tag.text:
            raise errors.ScraperError("Can't build a cancelled meeting item.")

        tr_classes = tag["class"]
        tr_class = tr_classes[0]
        loader = MeetingLoader(item=Meeting())

        tds = tag.find_all("td")
        
        # Values that we can always try to extract.
        type_val = utils.parse_meeting_type(tds[IND_MEETING_TYPE].text)
        loader.add_value("type_", type_val)
        try:
            number_val = utils.parse_sec_num(tds[IND_SEC_NUM_OR_DATE].text)
            loader.add_value("number", number_val)
        except errors.ScraperError:
            date_val = utils.parse_date(tds[IND_SEC_NUM_OR_DATE].text)
            loader.add_value("date", date_val)

        if tr_class == "sectxt":
            loader.add_value("sec_id", tds[IND_SEC_ID].text)

        is_valid_sectxt = tr_class == "sectxt" and len(tds) == SECTXT_VALID_TDS_LEN
        is_valid_nonenrtxt = tr_class == "nonenrtxt" and len(tds) == NONENRTXT_VALID_TDS_LEN

        if is_valid_sectxt or is_valid_nonenrtxt:
            days_val = utils.parse_days(tds[IND_DAYS].text)
            loader.add_value("days", days_val)
            loader.add_value("bldg", tds[IND_BLDG].text)
            loader.add_value("room", tds[IND_ROOM].text)
            (start_time_val, end_time_val) = utils.parse_time_range(tds[IND_TIME].text)
            loader.add_value("start_time", start_time_val)
            loader.add_value("end_time", end_time_val)

        if is_valid_sectxt:
            loader.add_value("instructor", tds[IND_INSTRUCTOR].text)
            # Section id number is present.
            if tds[IND_SEC_ID].text.strip():
                seats_avail_val = utils.parse_seats_avail(tds[IND_SEATS_AVAIL].text)
                loader.add_value("seats_avail", seats_avail_val)

        return loader.load_item()