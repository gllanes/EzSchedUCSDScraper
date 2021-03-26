# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html
import re
import datetime

from scrapy import Item, Field
from scrapy.loader import ItemLoader
from itemloaders.processors \
    import TakeFirst, MapCompose, Identity, Join

import scraper_schedule_of_classes.errors as errors


class MeetingUncategorized(Item):
    """
    A scrapy item representing the information of an individual meeting.
    Uncategorized - each meeting is not yet assigned as:
        general, section, or dated.
    """
    sec_id = Field()
    type_ = Field()
    number = Field()
    date = Field()
    days = Field()
    start_time = Field()
    end_time = Field()
    bldg = Field()
    room = Field()
    instructor = Field()
    seats_avail = Field()
    essential = Field()


def filter_empty_string(s):
    if s == "":
        return None
    return s


class MeetingUncategorizedLoader(ItemLoader):
    """
    ItemLoader for the MeetingUncategorized item.
    """
    default_output_processor = TakeFirst()

    # These fields are just the string values extracted from their respective
    # html element(s).
    sec_id_in = bldg_in = room_in = instructor_in = MapCompose(str.strip, filter_empty_string)


class CourseMeetingsUncategorized(Item):
    """
    A scrapy item representing a course, section group, and all its meetings.
    Its meetings are not yet categorized as general, section, or dated.
    """
    quarter_code = Field()
    subj_code = Field()
    number = Field()
    title = Field()
    first_meeting = Field()
    sectxt_meetings = Field()
    nonenrtxt_meetings = Field()


class CourseMeetingsUncategorizedLoader(ItemLoader):
    """
    An ItemLoader for the CourseMeetingsUncategorized item.
    """
    default_output_processor = TakeFirst()

    quarter_code_in = subj_code_in = number_in = title_in = MapCompose(str.strip)

    first_meeting_out = TakeFirst()
    sectxt_meetings_out = nonenrtxt_meetings_out \
        = Identity()


class Meeting(Item):
    sec_id = Field()
    type_ = Field()
    number = Field()
    date = Field()
    days = Field()
    start_time = Field()
    end_time = Field()
    bldg = Field()
    room = Field()
    seats_avail = Field()


class MeetingLoader(ItemLoader):
    default_output_processor = TakeFirst()


class CourseMeetings(Item):
    quarter_code = Field()
    subj_code = Field()
    number = Field()
    title = Field()
    section_group_code = Field()
    instructor = Field()
    section_meetings = Field()
    general_meetings = Field()
    dated_meetings = Field()


class CourseMeetingsLoader(ItemLoader):
    default_output_processor = TakeFirst()

    section_meetings_out = general_meetings_out = dated_meetings_out = \
        Identity()