import unittest
import datetime

from scraper_schedule_of_classes.items import *
from scraper_schedule_of_classes.pipelines import CourseCleanerPipeline

from test.meeting_comparator import MeetingComparator
from test.data import get_spider_parser_items

# class CourseMeetings(Item):
#     quarter_code = Field()
#     subj_code = Field()
#     number = Field()
#     title = Field()
#     section_group_code = Field()
#     instructor = Field()
#     section_meetings = Field()
#     general_meetings = Field()
#     dated_meetings = Field()


class TestCourseCleanerPipeline(MeetingComparator):
    
    @classmethod
    def setUpClass(cls):
        cls.quarter_code = "WI21"
        cls.subject_code = "PHYS"
        page_num = 8

        cls.spider_parser_items = get_spider_parser_items(cls.quarter_code, cls.subject_code, page_num)
        cls.pipeline = CourseCleanerPipeline()


    def test_pipeline_additional_lecture(self):
        # PHYS 4D A00
        item = self.spider_parser_items[2]
        p_item = self.pipeline.process_item(item, None)

        instructor_exp = "Frano Pereira, Alex M"
        # Section group information is correct.
        self.assertEqual(p_item.get("quarter_code"), self.quarter_code)
        self.assertEqual(p_item.get("subj_code"), self.subject_code)
        self.assertEqual(p_item.get("number"), "4D")
        self.assertEqual(p_item.get("title"), "Phys Majrs-EM Wav,Spec Rel,Opt")
        self.assertEqual(p_item.get("section_group_code"), "A00")
        self.assertEqual(p_item.get("instructor"), instructor_exp)

        general_meetings_exp = [
            self.build_meeting_item(
                None, "LE", "A00", None, "MWF", datetime.time(13, 0),
                datetime.time(13, 50), "RCLAS", "R39", instructor_exp,
                None, True
            ),
            self.build_meeting_item(
                None, "LE", "A00", None, "Th", datetime.time(18, 0),
                datetime.time(18, 50), "RCLAS", "R106", essential=True
            ),
        ]

        self.compare_meeting_item_lists(general_meetings_exp, p_item.get("general_meetings"))

        section_meetings_exp = [
            self.build_meeting_item(
                "31483", "DI", "A01", None, "Tu", datetime.time(13, 0), 
                datetime.time(13, 50), "RCLAS", "R175", instructor_exp,
                3
            ),
            self.build_meeting_item(
                "36920", "DI", "A02", None, "Tu", datetime.time(17, 0), 
                datetime.time(17, 50), "RCLAS", "R115", instructor_exp,
                2
            ),
            self.build_meeting_item(
                "36921", "DI", "A03", None, "Tu", datetime.time(18, 0), 
                datetime.time(18, 50), "RCLAS", "R28", instructor_exp,
                10
            )
        ]

        self.compare_meeting_item_lists(section_meetings_exp, p_item.get("section_meetings"))

        dated_meetings_exp = [
            self.build_meeting_item(
                None, "FI", None, "03/19/2021", "F", datetime.time(11, 30),
                datetime.time(14, 29), "TBA", "TBA"
            ),
        ]

        self.compare_meeting_item_lists(dated_meetings_exp, p_item.get("dated_meetings"))