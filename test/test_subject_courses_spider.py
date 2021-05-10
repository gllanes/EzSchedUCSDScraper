import datetime

from scraper_schedule_of_classes.spiders.subject_courses_spider \
    import SubjectCoursesSpider
from scraper_schedule_of_classes import utils
from bs4 import BeautifulSoup

from test.meeting_comparator import MeetingComparator
import test.data


class CoursesSpiderBuildItemTest(MeetingComparator):
    """
    SubjectCoursesSpider build_item_from_group
    should build a correct item representing a course and
    its uncategorized sectxt/nonrenrtxt meetings.
    """

    @classmethod
    def setUpClass(cls):
        """
        Get the grouped bs4 tags.
        """
        quarter_code = "WI21"
        subject_code = "ECE"
        page_num = 1

        cls.spider = SubjectCoursesSpider(quarter_code, subject_code)

        html = test.data.get_html_binary(quarter_code, subject_code, page_num)
        soup = BeautifulSoup(html, "lxml")
        tags = soup.find_all(utils.tag_matches_any)

        # Tag groups indexable.
        cls.tags_grouped = list(cls.spider.group_tags(tags))


    def test_build_item_from_single(self):
        """
        If the tag contains of only a crsheader, no course item should 
        be returned.
        """
        single_tag_group = self.tags_grouped[0]
        item = self.spider.build_item_from_group(single_tag_group, 'ECE')
        self.assertIsNone(item)


    def test_build_item_with_invalid_length_meeting(self):
        """
        Build an item where one of the tags has an invalid length, so the meeting
        has ambiguous information.
        """
        ambig_tag_group = self.tags_grouped[6]
        item = self.spider.build_item_from_group(ambig_tag_group, 'ECE')
        
        first_meeting_exp = self.build_meeting_item(
            None, "LE", "A00", None, "MW", datetime.time(17, 0),
            datetime.time(18, 20), "RCLAS", "R51", "Dey, Sujit",
            None, None
        )

        self.compare_meeting_items(first_meeting_exp, item.get("first_meeting"))

        sectxt_meetings_exp = [
            self.build_meeting_item(
                None, "DI", "A01", None, "M", datetime.time(16, 0),
                datetime.time(16, 50), "RCLAS", "R95", "Dey, Sujit",
                None, None
            ),
            self.build_meeting_item(
                None, "DI", "A02", None, "F", datetime.time(12, 0),
                datetime.time(12, 50), "RCLAS", "R83", "Dey, Sujit",
                None, None
            ),
            self.build_meeting_item(
                type_ = "LA", number="A50", sec_id="32250"
            ),
        ]

        nonenrtxt_meetings_exp = [
            self.build_meeting_item(
                None, "FI", None, "03/15/2021", "M", datetime.time(19, 0),
                datetime.time(21, 59), "TBA", "TBA", None, None, None
            )
        ]

        self.compare_meeting_item_lists(sectxt_meetings_exp, item.get("sectxt_meetings"))
        self.compare_meeting_item_lists(nonenrtxt_meetings_exp, item.get("nonenrtxt_meetings"))


    def test_build_item_with_cancelled_meetings(self):
        cancelled_tag_group = self.tags_grouped[7]
        item = self.spider.build_item_from_group(cancelled_tag_group, 'ECE')

        first_meeting_exp = self.build_meeting_item(
            None, "LE", "A00", None, "TuTh", datetime.time(15, 30),
            datetime.time(16, 50), "RCLAS", "R72", "Mir Arabbaygi, Siavash",
            None, None
        )

        self.compare_meeting_items(first_meeting_exp, item.get("first_meeting"))

        sectxt_meetings_exp = [
            self.build_meeting_item(
                None, "DI", "A03", None, "F", datetime.time(10, 0),
                datetime.time(10, 50), "RCLAS", "R118", "Mir Arabbaygi, Siavash",
                None, None
            ),
            self.build_meeting_item(
                "32259", "LA", "A50", None, "Tu", datetime.time(19, 0),
                datetime.time(22, 0), "RCLAS", "R111", "Mir Arabbaygi, Siavash",
                5
            ),
            self.build_meeting_item(
                "32260", "LA", "A51", None, "W", datetime.time(16, 0),
                datetime.time(19, 0), "RCLAS", "R165", "Mir Arabbaygi, Siavash",
                3
            ),
            self.build_meeting_item(
                "32261", "LA", "A52", None, "W", datetime.time(19, 0),
                datetime.time(22, 0), "RCLAS", "R03", "Mir Arabbaygi, Siavash",
                8
            ),
            self.build_meeting_item(
                "32262", "LA", "A53", None, "Th", datetime.time(19, 0),
                datetime.time(22, 0), "RCLAS", "R107", "Mir Arabbaygi, Siavash",
                4
            ),
            self.build_meeting_item(
                "32263", "LA", "A54", None, "F", datetime.time(19, 0),
                datetime.time(22, 0), "RCLAS", "R04", "Mir Arabbaygi, Siavash",
                3
            ),
        ]

        self.compare_meeting_item_lists(sectxt_meetings_exp, item.get("sectxt_meetings"))

        nonenrtxt_meetings_exp = [
            self.build_meeting_item(
                None, "FI", None, "03/16/2021", "Tu", datetime.time(15, 0),
                datetime.time(17, 59), "TBA", "TBA"
            )
        ]

        self.compare_meeting_item_lists(nonenrtxt_meetings_exp, item.get("nonenrtxt_meetings"))