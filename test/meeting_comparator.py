import unittest

from itertools import zip_longest

from scraper_schedule_of_classes.items import *


class MeetingComparator(unittest.TestCase):

    def build_meeting_item(
        self,
        sec_id = None,
        type_ = None,
        number = None,
        date = None,
        days = None,
        start_time = None,
        end_time = None,
        bldg = None,
        room = None,
        instructor = None,
        seats_avail = None,
        essential = None
    ):
        loader = MeetingLoader(Meeting())
        loader.add_value("sec_id", sec_id)
        loader.add_value("type_", type_)
        loader.add_value("number", number)
        loader.add_value("date", date)
        loader.add_value("days", days)
        loader.add_value("start_time", start_time)
        loader.add_value("end_time", end_time)
        loader.add_value("bldg", bldg)
        loader.add_value("room", room)
        loader.add_value("instructor", instructor)
        loader.add_value("seats_avail", seats_avail)
        loader.add_value("essential", essential)
        return loader.load_item()


    def compare_meeting_items(self, first, second):
        for key in first.keys():
            self.assertEqual(first.get(key), second.get(key))
        for key in second.keys():
            self.assertEqual(first.get(key), second.get(key))

    def compare_meeting_item_lists(self, first, second):
        for i, (first_meeting, second_meeting) in enumerate(zip_longest(first, second)):
            with self.subTest("MEETINGS UNEQUAL", meeting_idx = i):
                self.compare_meeting_items(first_meeting, second_meeting)