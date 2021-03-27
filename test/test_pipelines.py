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
        cls.pipeline = CourseCleanerPipeline()


    def test_all_nonenrtxt_meeting_types(self):
        # PHYS 4D A00
        quarter_code = "WI21"
        subject_code = "PHYS"
        page_num = 8
        spider_parser_items = get_spider_parser_items(quarter_code, subject_code, page_num)
        item = spider_parser_items[2]
        p_item = self.pipeline.process_item(item, None)

        instructor_exp = "Frano Pereira, Alex M"
        # Section group information is correct.
        self.assertEqual(p_item.get("quarter_code"), quarter_code)
        self.assertEqual(p_item.get("subj_code"), subject_code)
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


    def test_pipeline_all_sectxt_types(self):
        """
        Test items where there is supposed to be at least one sectxt
        of each type:
            - general, essential
            - general, nonessential
            - section meeting.
        Example: WI21, MATH page 1
        """
        quarter_code = "WI21"
        subject_code = "MATH"
        page_num = 1

        spider_parser_items = get_spider_parser_items(quarter_code, subject_code, page_num)
        # WI21 MATH 2
        item = spider_parser_items[0]
        p_item = self.pipeline.process_item(item, None)
        
        instructor_exp = "Quarfoot, David James"
        self.assertEqual(p_item.get("quarter_code"), quarter_code)
        self.assertEqual(p_item.get("subj_code"), subject_code)
        self.assertEqual(p_item.get("number"), "2")
        self.assertEqual(p_item.get("title"), "Intro to College Mathematics")
        self.assertEqual(p_item.get("section_group_code"), "A00")
        self.assertEqual(p_item.get("instructor"), instructor_exp)

        general_meetings_exp = [
            self.build_meeting_item(
                None, "LE", "A00", None, "MWF", datetime.time(12, 0),
                datetime.time(12, 50), "RCLAS", "R67", instructor_exp,
                None, True
            ), 
            self.build_meeting_item(
                None, "TU", "A50", None, "Th", datetime.time(14, 0),
                datetime.time(15, 20), "RCLAS", "R102", instructor_exp,
                None, False
            ), 
        ]

        self.compare_meeting_item_lists(general_meetings_exp, p_item.get("general_meetings"))

        section_meetings_exp = [
            self.build_meeting_item(
                "27080", "DI", "A01", None, "Tu", datetime.time(14, 0),
                datetime.time(14, 50), "RCLAS", "R193", instructor_exp,
                25
            ), 
        ]

        self.compare_meeting_item_lists(section_meetings_exp, p_item.get("section_meetings"))

        dated_meetings_exp = [
            self.build_meeting_item(
                None, "FI", None, "03/17/2021", "W", datetime.time(11, 30),
                datetime.time(14, 29), "TBA", "TBA",
            ), 
        ]

        self.compare_meeting_item_lists(dated_meetings_exp, p_item.get("dated_meetings"))

    
    def test_pipeline_first_meeting_is_section(self):
        """
        Testing the case where the first meeting is a section meeting.
        """
        quarter_code = "WI21"
        subject_code = "BENG"
        page_num = 2

        spider_parser_items = get_spider_parser_items(quarter_code, subject_code, page_num)
        # WI21 MATH 2
        item = spider_parser_items[9]
        p_item = self.pipeline.process_item(item, None)

        instructor_exp = "Huang, Xiaohua"
        self.assertEqual(p_item.get("quarter_code"), quarter_code)
        self.assertEqual(p_item.get("subj_code"), subject_code)
        self.assertEqual(p_item.get("number"), "161B")
        self.assertEqual(p_item.get("title"), "Biochemical Engineering")
        self.assertEqual(p_item.get("section_group_code"), "A00")
        self.assertEqual(p_item.get("instructor"), instructor_exp)

        general_meetings_exp = [
            self.build_meeting_item(
                None, "DI", "A01", None, "M", datetime.time(10, 0),
                datetime.time(10, 50), "RCLAS", "R139", instructor_exp, 
                essential = False
            ),
            self.build_meeting_item(
                None, "DI", "A02", None, "M", datetime.time(11, 0),
                datetime.time(11, 50), "RCLAS", "R180", instructor_exp, 
                essential = False
            )
        ]

        self.compare_meeting_item_lists(general_meetings_exp, p_item.get("general_meetings"))

        section_meetings_exp = [
            self.build_meeting_item(
                "34353", "LE", "A00", None, "TuTh", datetime.time(15, 30),
                datetime.time(16, 50), "RCLAS", "R87", instructor_exp, 
                1
            )
        ]

        self.compare_meeting_item_lists(section_meetings_exp, p_item.get("section_meetings"))

        dated_meetings_exp = [
            self.build_meeting_item(
                None, "FI", None, "03/16/2021", "Tu", datetime.time(15, 0),
                datetime.time(17, 59), "TBA", "TBA"
            )
        ]

        self.compare_meeting_item_lists(dated_meetings_exp, p_item.get("dated_meetings"))