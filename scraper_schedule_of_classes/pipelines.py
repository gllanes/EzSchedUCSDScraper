# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import re
import pprint

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
from .db.db import DataAccess
from .items import *


pp = pprint.PrettyPrinter(indent = 2)


class SubjectCleanerPipeline:

    SUBJECT_NAME_REGEX = re.compile(r"[A-Z]+\s+-(.*)")

    def process_item(self, item, spider):
        """
        Returns a cleaned list of subject codes to subject names.
        Also persist subjects here.
        """
        adapter = ItemAdapter(item)
        # Get the list of all subjects
        subjects_codes_values = adapter.get("subjects_codes_values")

        # To store the cleaned values.
        subjects_codes_names = []
        
        # Each entry has the format:
        # {
        #     "code": (<subject_code>),
        #     "value": (<subject_code> - <subject_title>),
        # }
        for scv in subjects_codes_values:
            subj_code = scv["code"].strip()
            match = self.SUBJECT_NAME_REGEX.search(scv["value"])
            subj_name = match.group(1).strip()
            subjects_codes_names.append({
                "code": subj_code,
                "name": subj_name
            })

        # Store in database.
        try:
            self.DA.insert_subjects(subjects_codes_names)
            self.DA.commit()
        except Exception as e:
            self.DA.rollback()
            raise e

        return {
            "message": "Subjects successfully saved."
        }

    def open_spider(self, spider):
        self.DA = DataAccess()

    def close_spider(self, spider):
        self.DA.close()


class CourseCleanerPipeline:

    def process_item(self, item, spider):
        """
        Items will be items of type CourseMeetings.
        Decide which meetings will be section meetings, 
        general meetings, or dated meetings.
        Return a well-formatted ready for persistence.
        """

        course_loader = CourseMeetingsLoader(CourseMeetings())

        # quarter, subject, number, title are the same.
        course_loader.add_value("quarter_code", item.get("quarter_code"))
        course_loader.add_value("subj_code", item.get("subj_code"))
        course_loader.add_value("number", item.get("number"))
        course_loader.add_value("title", item.get("title"))

        # Set the section group and instructor from the first meeting.
        # Drop if the section group is not of the form LETTER00 or ###
        first_meeting_item = item.get("first_meeting")
        section_group = first_meeting_item.get("number")
        instructor = first_meeting_item.get("instructor")
        course_loader.add_value("section_group_code", section_group)
        course_loader.add_value("instructor", instructor)
    

        # First meeting is either essential main or section.
        if first_meeting_item.get("sec_id"):
            course_loader.add_value("section_meetings", first_meeting_item)
        else: 
            first_meeting_item["essential"] = True
            course_loader.add_value("general_meetings", first_meeting_item)

        # Sectxt meetings:
        # if secid value is not empty, section meeting.
        # otherwise, non essential main meeting.
        for sectxt_item in item.get("sectxt_meetings", []):
            if sectxt_item.get("sec_id"):
                course_loader.add_value("section_meetings", sectxt_item)
            else: 
                sectxt_item["essential"] = False
                course_loader.add_value("general_meetings", sectxt_item)

        # Nonenrtxt:
        # if date, dated
        # if same sec num as sec group, essential main
        # otherwise, non essential main
        for nonenrtxt_item in item.get("nonenrtxt_meetings", []):
            if nonenrtxt_item.get("date"):
                course_loader.add_value("dated_meetings", nonenrtxt_item)
            elif nonenrtxt_item.get("number") == section_group:
                nonenrtxt_item["essential"] = True
                course_loader.add_value("general_meetings", nonenrtxt_item)

            # I don't think this case is possible, but better to check anyway.
            else: 
                nonenrtxt_item["essential"] = False
                course_loader.add_value("general_meetings", nonenrtxt_item)


        # If there are no section meetings, after grouping, drop.
        # The scheduler can't schedule only non section meetings.
        course_item = course_loader.load_item()
        if not course_item.get("section_meetings"):
            err_msg = f"{item.get('quarter_code')} {item.get('subj_code')} "\
                f"{item.get('number')} {section_group}: no valid sectxt"
            raise DropItem(err_msg)

        return course_item


class CoursePersistencePipeline:
    """
    Given an item from the CourseCleanerPipeline, save all the information
    about this course/section group to the database.
    """

    def open_spider(self, spider):
        self.DA = DataAccess()

    def close_spider(self, spider):
        self.DA.close()

    def process_item(self, item, spider):
        
        try:
            self.DA.insert_section_group_all_info(item) 
            self.DA.commit()

        except Exception as e:
            self.DA.rollback()
            raise e

        # Insert course offering, section group, and meetings.
        # All of this should take place in one transaction, preferably.
        pass