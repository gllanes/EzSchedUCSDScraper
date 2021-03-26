import re
import datetime

import scrapy
import urllib
from scrapy.loader import ItemLoader
from more_itertools import split_before
from bs4 import BeautifulSoup

import scraper_schedule_of_classes.errors as errors

from scraper_schedule_of_classes.items \
    import CourseMeetingsUncategorized, CourseMeetingsUncategorizedLoader, MeetingUncategorized, MeetingUncategorizedLoader



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


DAYS_REGEX = re.compile(r"(M|Tu|W|Th|F|S)+")
def parse_days(txt):
    txt = txt.strip()
    match = DAYS_REGEX.search(txt)
    if not match:
        raise errors.ScraperError(f"Could not parse days from {txt}")
    return match.group(0)


MEETING_TYPE_REGEX = re.compile(r"(AC|CL|CO|DI|FI|FM|FW|IN|IT|LA|LE|MI|MU|OT|PB|PR|RE|SE|ST|TU)")
def parse_meeting_type(txt):
    txt = txt.strip()
    match = MEETING_TYPE_REGEX.search(txt)
    if not match:
        raise errors.ScraperError(f"Could not parse meeting type from {txt}")
    return match.group(0)


SEC_NUM_REGEX = re.compile(r"^([A-Z][0-9]{2}|[0-9]{3})$")
def parse_sec_num(txt):
    txt = txt.strip()
    match = SEC_NUM_REGEX.search(txt)
    if not match:
        raise errors.ScraperError(f"Could not parse section number from {txt}")
    return match.group(0)


TIME_RANGE_REGEX = re.compile(r"([0-9]+):([0-9]{2})(a|p)-([0-9]+):([0-9]{2})(a|p)")
def parse_time_range(txt):
    txt = txt.strip()
    match = TIME_RANGE_REGEX.search(txt)
    if not match:
        raise errors.ScraperError(f"Could not parse time range from {txt}")
    
    start_time_hour = int(match.group(1))
    start_time_min = int(match.group(2))
    start_time_per = match.group(3)
    if start_time_per == "p" and start_time_hour != 12:
        start_time_hour += 12
    elif start_time_per == "a" and start_time_hour == 12:
        start_time_hour = 0

    end_time_hour = int(match.group(4))
    end_time_min = int(match.group(5))
    end_time_per = match.group(6)
    if end_time_per == "p" and end_time_hour != 12:
        end_time_hour += 12
    elif end_time_per == "a" and end_time_hour == 12:
        end_time_hour = 0

    start_time = datetime.time(start_time_hour, start_time_min)
    end_time = datetime.time(end_time_hour, end_time_min)

    return (start_time, end_time)


SEATS_AVAIL_REGEX = re.compile(r"^[0-9]+$")
def parse_seats_avail(txt):
    txt = txt.strip()
    # This will usually happen if the meeting is full. We don't want to 
    # include meetings that have no seats.
    match = SEATS_AVAIL_REGEX.search(txt)
    if match:
        seats_avail = int(match.group(0))
    elif "FULL" in txt:
        seats_avail = 0
    else:
        raise errors.ScraperError(f"Could not parse seats avail from {txt}")
    return seats_avail


DATE_REGEX = re.compile(r"^[0-9]+/[0-9]+/[0-9]+$")
def parse_date(txt):
    txt = txt.strip()
    match = DATE_REGEX.search(txt)
    if match:
        date = match.group(0)
    else:
        raise errors.ScraperError(f"Could not parse date from {txt}")
    return date


class SubjectCoursesSpider(scrapy.Spider):
    """
    A spider that scrapes all all of the courses/meetings
    for a subject in a given term.
    """
    name = "subject_courses"
    custom_settings = {
        "ITEM_PIPELINES": {
            "scraper_schedule_of_classes.pipelines.CourseCleanerPipeline": 100
        }
    }

    def __init__(self, quarter_code = None, subject_code = None, *args, **kwargs):
        super(SubjectCoursesSpider, self).__init__(*args, **kwargs)
        if not quarter_code:
            raise errors.MissingQuarterError(f"The {self.name} spider needs a quarter.")
        if not subject_code:
            raise errors.MissingSubjectError(f"The {self.name} spider needs a subject.")
        self.quarter_code = quarter_code
        self.subject_code = subject_code


    def start_requests(self):
        
        payload = {
            SUBJECT_QUERY_STR: self.subject_code,
            TERM_QUERY_STR: self.quarter_code, 
            SCHED_OPT_1_STR: "true",
            SCHED_OPT_2_STR: "true"
        }

        query = urllib.parse.urlencode(payload)
        url = f"{SCHEDULE_OF_CLASSES_URL}?{query}"

        yield scrapy.Request(url, self.parse)


    def parse(self, response):
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
                SUBJECT_QUERY_STR: self.subject_code,
                TERM_QUERY_STR: self.quarter_code, 
                SCHED_OPT_1_STR: "true",
                SCHED_OPT_2_STR: "true",
                PAGE_QUERY_STR: i
            }

            query = urllib.parse.urlencode(payload)
            url = f"{SCHEDULE_OF_CLASSES_URL}?{query}"

            yield scrapy.Request(url, self.parse_extra_page)

        # Get all the tags with course information
        tags = soup.find_all(self.tag_matches_any)

        # Group by course header.
        yield from self.course_meeting_items(tags)
        

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


    def tag_matches_any(self, tag):
        """
        Either crsheader or sectxt/nonenrtxt
        """
        return (
            self.tag_matches_crsheader(tag) or 
            self.tag_matches_meeting(tag)
        )


    def tag_matches_crsheader(self, tag):
        """
        Returns true if the bs4 element is a crsheader row.
        """
        return (
            tag.name == "tr" and
            tag.td and
            tag.td.has_attr("class") and
            "crsheader" in tag.td["class"]
        )


    def tag_matches_meeting(self, tag):
        return (
            tag.name == "tr" and
            tag.has_attr("class") and
            ("sectxt" in tag["class"] or "nonenrtxt" in tag["class"]) and
            len(tag.find_all("td")) >= 4
        )

        
    def parse_extra_page(self, response):
        # Parser for pages beyond the first.

        soup = BeautifulSoup(response.body, "lxml")    
        # Get all the tags with course information
        tags = soup.find_all(self.tag_matches_any)

        # Group by course header.
        yield from self.course_meeting_items(tags)
    
    def course_meeting_items(self, tags):
        """
        Given some all of the matching selectors for one page, 
        return rough items representing all the meetings
        for a particular course/section group.
        """

        tags_grouped = split_before(tags, 
            lambda tag: not tag.has_attr("class"))

        for group in tags_grouped:
            course_item = self.build_item_from_group(group)
            if course_item:
                yield course_item


    def build_item_from_group(self, tag_group):
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
        loader.add_value("subj_code", self.subject_code)

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
        They will have the correct types, however.
        """

        # Check if the meeting was cancelled.
        if "Cancelled" in tag.text:
            raise errors.ScraperError("Can't build a cancelled meeting item.")

        tr_classes = tag["class"]
        tr_class = tr_classes[0]
        loader = MeetingUncategorizedLoader(item=MeetingUncategorized())

        tds = tag.find_all("td")
        
        # Values that we can always try to extract.
        type_val = parse_meeting_type(tds[IND_MEETING_TYPE].text)
        loader.add_value("type_", type_val)
        try:
            number_val = parse_sec_num(tds[IND_SEC_NUM_OR_DATE].text)
            loader.add_value("number", number_val)
        except errors.ScraperError:
            date_val = parse_date(tds[IND_SEC_NUM_OR_DATE].text)
            loader.add_value("date", date_val)

        if tr_class == "sectxt":
            loader.add_value("sec_id", tds[IND_SEC_ID].text)

        is_valid_sectxt = tr_class == "sectxt" and len(tds) == SECTXT_VALID_TDS_LEN
        is_valid_nonenrtxt = tr_class == "nonenrtxt" and len(tds) == NONENRTXT_VALID_TDS_LEN

        if is_valid_sectxt or is_valid_nonenrtxt:
            days_val = parse_days(tds[IND_DAYS].text)
            loader.add_value("days", days_val)
            loader.add_value("bldg", tds[IND_BLDG].text)
            loader.add_value("room", tds[IND_ROOM].text)
            (start_time_val, end_time_val) = parse_time_range(tds[IND_TIME].text)
            loader.add_value("start_time", start_time_val)
            loader.add_value("end_time", end_time_val)

        if is_valid_sectxt:
            loader.add_value("instructor", tds[IND_INSTRUCTOR].text)
            if tds[IND_SEC_ID].text.strip():
                seats_avail_val = parse_seats_avail(tds[IND_SEATS_AVAIL].text)
                loader.add_value("seats_avail", seats_avail_val)

        return loader.load_item()