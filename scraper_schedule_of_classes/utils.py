import datetime
import json
import re

import scraper_schedule_of_classes.errors as errors


def tag_matches_any(tag):
    """
    Either crsheader or sectxt/nonenrtxt
    """
    return (
        tag_matches_crsheader(tag) or 
        tag_matches_meeting(tag)
    )


def tag_matches_crsheader(tag):
    """
    Returns true if the bs4 element is a crsheader row.
    """
    return (
        tag.name == "tr" and
        tag.td and
        tag.td.has_attr("class") and
        "crsheader" in tag.td["class"]
    )


def tag_matches_meeting(tag):
    return (
        tag.name == "tr" and
        tag.has_attr("class") and
        ("sectxt" in tag["class"] or "nonenrtxt" in tag["class"]) and
        len(tag.find_all("td")) >= 4
    )


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


class CourseItemEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.time):
            return obj.isoformat(timespec='seconds')
        else:
            return json.JSONEncoder.default(self, obj)