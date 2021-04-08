import os
import pathlib

import psycopg2 as pg
import psycopg2.extras as pg_extras
import psycopg2.pool as pgpool
from dotenv import load_dotenv

load_dotenv()
DB_USER=os.getenv("DB_USER")
DB_PASSWORD=os.getenv("DB_PASSWORD")
DB_NAME=os.getenv("DB_NAME")
DB_HOST=os.getenv("DB_HOST")
DB_PORT=os.getenv("DB_PORT")

DB_DIR = pathlib.Path(__file__).parent.absolute()

class DataAccess:
    
    # There is going to be one thread to scrape each page.
    # Keep one connection and cursor per thread.
    
    def __init__(self):
        """
        Acquire database connection.
        """            
        self.conn = pg.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME
        )

        # various prepared statements for inserting.
        insert_course_prepare = open(DB_DIR / "insert_course_prepare.sql", "r").read()
        insert_course_offering_prepare = open(DB_DIR / "insert_course_offering_prepare.sql", "r").read()
        insert_section_group_prepare = open(DB_DIR / "insert_section_group_prepare.sql", "r").read()
        insert_section_meeting_prepare = open(DB_DIR / "insert_section_meeting_prepare.sql", "r").read()
        insert_general_meeting_prepare = open(DB_DIR / "insert_general_meeting_prepare.sql", "r").read()
        insert_dated_meeting_prepare = open(DB_DIR / "insert_dated_meeting_prepare.sql", "r").read()


        # prepare all statements for this session
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(insert_course_prepare)
                cur.execute(insert_course_offering_prepare)
                cur.execute(insert_section_group_prepare)
                cur.execute(insert_section_meeting_prepare)
                cur.execute(insert_general_meeting_prepare)
                cur.execute(insert_dated_meeting_prepare)


    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    
    def execute_str(self, query_str, values = None, do_return = False):

        returning = None
        
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(query_str, values)
                if do_return:
                    returning = cur.fetchall()

        return returning


    def execute_str_batch(self, query_str, values_list):

        with self.conn.cursor() as cur:
            pg_extras.execute_batch(cur, query_str, values_list, 
                page_size=len(values_list))


    def insert_quarter(self, code, name):

        query_str = """
            INSERT INTO quarter (code, name)
            VALUES (%s, %s)
            ON CONFLICT (code)
            DO NOTHING;
        """
        values = (code, name)
        
        self.execute_str(query_str, values)


    def insert_subjects(self, subject_codes_names):
        """
        subject_codes_names is an array of entries of the following
        format:
        {
            "code": code,
            "name": name
        }
        """

        query_str = """
            INSERT INTO subject (code, name)
            VALUES (%s, %s)
            ON CONFLICT (code)
            DO NOTHING;
        """

        # Build a tuple for each entry
        subject_codes_names_params = [(scn["code"], scn["name"]) for scn in subject_codes_names]
        self.execute_str_batch(query_str, subject_codes_names_params)
            

    def get_all_subjects(self):
        """
        retrieve all existing subjects in the database.

        returns an array of subjects.
        Each subject is of the form (subject_code, subject_name)
        """

        query_str = """
            SELECT code from subject;
        """

        result = self.execute_str(query_str)
        subject_codes = [code for (code, ) in result]
        return subject_codes


    def insert_section_group_all_info(self, item):
        """
        Given an item from the course persistence pipeline, insert
        all information from that item (course, offering, section, meetings.)
        """
        # Insert course.
        course_id = self.insert_course(item.get("subj_code"), 
            item.get("number"), item.get("title"))

        # Insert course offering.
        course_offering_id = self.insert_course_offering(course_id,
            item.get("quarter_code"))

        # Insert section group
        section_group_id = self.insert_section_group(course_offering_id,
            item.get("section_group_code"), item.get("instructor"))

        section_meetings = item.get("section_meetings")
        section_meeting_vals = [
            (section_group_id, m_item.get("type_"), m_item.get("days"),
            m_item.get("start_time"), m_item.get("end_time"), m_item.get("bldg"),
            m_item.get("room"), m_item.get("number"), m_item.get("seats_avail"))
            for m_item in section_meetings
        ]
        self.insert_section_meetings(section_meeting_vals)

        general_meetings = item.get("general_meetings")
        if general_meetings:
            general_meeting_vals = [
                (section_group_id, m_item.get("type_"), m_item.get("days"),
                m_item.get("start_time"), m_item.get("end_time"), m_item.get("bldg"),
                m_item.get("room"), m_item.get("number"), m_item.get("essential"))
                for m_item in general_meetings
            ]
            self.insert_general_meetings(general_meeting_vals)

        dated_meetings = item.get("dated_meetings")
        if dated_meetings:
            dated_meeting_vals = [
                (section_group_id, m_item.get("type_"), m_item.get("days"),
                m_item.get("start_time"), m_item.get("end_time"), m_item.get("bldg"),
                m_item.get("room"), m_item.get("date"))
                for m_item in dated_meetings
            ]
            self.insert_dated_meetings(dated_meeting_vals)

    
    def insert_course(self, subject_code, number, title):
        """
        Insert the course given by the combination
        (subject_code, number, title)
        into the database.
        """
        # Get subject code id
        # insert course, return id
        # prepared statement parameters: (subject_code, number, title)

        values = (subject_code, number, title)
        with self.conn.cursor() as cur:
            cur.execute("EXECUTE insert_course (%s, %s, %s)", values)
            course_id = cur.fetchone()[0]
        
        return course_id
        

    def insert_course_offering(self, course_id, quarter_code):

        values = (course_id, quarter_code)
        with self.conn.cursor() as cur:
            cur.execute("EXECUTE insert_course_offering (%s, %s)", values)
            course_offering_id = cur.fetchone()[0]

        return course_offering_id


    def insert_section_group(self, course_offering_id, code, instructor):
        
        values = (course_offering_id, code, instructor)
        with self.conn.cursor() as cur:
            cur.execute("EXECUTE insert_section_group (%s, %s, %s)", values)
            section_group_id = cur.fetchone()[0]

        return section_group_id


    def insert_section_meetings(self, section_meeting_values):
        # $1: section group id 
        # $2: meeting type
        # $3: days
        # $4: start time
        # $5: end time
        # $6: building
        # $7: room
        # $8: (meeting) number
        # $9: seats available
        self.execute_str_batch("EXECUTE insert_section_meetings "
            "(%s, %s, %s, %s, %s, %s, %s, %s, %s);", section_meeting_values)


    def insert_general_meetings(self, general_meeting_values):
        # $1: section group id 
        # $2: meeting type
        # $3: days
        # $4: start time
        # $5: end time
        # $6: building
        # $7: room
        # $8: (meeting) number
        # $9: essential
        self.execute_str_batch("EXECUTE insert_general_meetings "
            "(%s, %s, %s, %s, %s, %s, %s, %s, %s);", general_meeting_values)


    def insert_dated_meetings(self, dated_meeting_values):
        # $1: section group id 
        # $2: meeting type
        # $3: days
        # $4: start time
        # $5: end time
        # $6: building
        # $7: room
        # $8: date
        self.execute_str_batch("EXECUTE insert_dated_meetings "
            "(%s, %s, %s, %s, %s, %s, %s, %s);", dated_meeting_values)    


    def reset_for_scrape(self):
        """
        Reset database for a new scrape. 
        Delete all course offerings, section groups, and meetings.
        """
        query_str = """
            TRUNCATE section_meeting, general_meeting, dated_meeting, meeting, section_group, course_offering;
        """
        self.execute_str(query_str)

    def close(self):
        self.conn.close()