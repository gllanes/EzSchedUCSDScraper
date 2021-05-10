import os
import pathlib

import psycopg2 as pg
import psycopg2.extras as pg_extras
import psycopg2.pool as pgpool
import psycopg2.errors

from scraper_schedule_of_classes.db.config \
    import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME

DB_DIR = pathlib.Path(__file__).parent.absolute()

class DataAccess:

    conn_pool = pgpool.ThreadedConnectionPool(1, 20,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME)
    
    
    @classmethod
    def get_conn(cls):
        conn = cls.conn_pool.getconn()
        
        # various prepared statements for inserting.
        insert_course_prepare = open(DB_DIR / "insert_course_prepare.sql", "r").read()
        insert_course_offering_prepare = open(DB_DIR / "insert_course_offering_prepare.sql", "r").read()
        insert_section_group_prepare = open(DB_DIR / "insert_section_group_prepare.sql", "r").read()
        insert_section_meeting_prepare = open(DB_DIR / "insert_section_meeting_prepare.sql", "r").read()
        insert_general_meeting_prepare = open(DB_DIR / "insert_general_meeting_prepare.sql", "r").read()
        insert_dated_meeting_prepare = open(DB_DIR / "insert_dated_meeting_prepare.sql", "r").read()

        # prepare statements if they have not already been prepared for this connection.
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(insert_course_prepare)
                    cur.execute(insert_course_offering_prepare)
                    cur.execute(insert_section_group_prepare)
                    cur.execute(insert_section_meeting_prepare)
                    cur.execute(insert_general_meeting_prepare)
                    cur.execute(insert_dated_meeting_prepare)

        # duplicate prepared statement
        except psycopg2.errors.lookup('42P05'):
            pass

        return conn

    
    @classmethod
    def put_conn(cls, conn):
        cls.conn_pool.putconn(conn)

    
    @classmethod
    def close(cls):
        cls.conn_pool.closeall()

    
    @staticmethod
    def execute_str(conn, query_str, values = None, do_return = False):

        returning = None
        
        with conn.cursor() as cur:
            cur.execute(query_str, values)
            if do_return:
                returning = cur.fetchall()

        return returning


    @staticmethod
    def execute_str_batch(conn, query_str, values_list):

        with conn.cursor() as cur:
            pg_extras.execute_batch(cur, query_str, values_list, 
                page_size=len(values_list))


    @classmethod
    def insert_quarter(cls, conn, code, name):

        query_str = """
            INSERT INTO quarter (code, name)
            VALUES (%s, %s)
            ON CONFLICT (code)
            DO NOTHING;
        """
        values = (code, name)
        
        cls.execute_str(conn, query_str, values)


    @classmethod
    def insert_subjects(cls, conn, subject_codes_names):
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
        cls.execute_str_batch(conn, query_str, subject_codes_names_params)
            

    @classmethod
    def get_all_subjects(cls, conn):
        """
        retrieve all existing subjects in the database.

        returns an array of subjects.
        Each subject is of the form (subject_code, subject_name)
        """

        query_str = """
            SELECT code from subject;
        """

        result = cls.execute_str(conn, query_str, do_return=True)
        subject_codes = [code for (code, ) in result]
        return subject_codes


    @classmethod
    def insert_section_group_all_info(cls, conn, item):
        """
        Given an item from the course persistence pipeline, insert
        all information from that item (course, offering, section, meetings.)
        """
        # Insert course.
        course_id = cls.insert_course(conn, item.get("subj_code"), 
            item.get("number"), item.get("title"))

        # Insert course offering.
        course_offering_id = cls.insert_course_offering(conn, course_id,
            item.get("quarter_code"))

        # Insert section group
        section_group_id = cls.insert_section_group(conn, course_offering_id,
            item.get("section_group_code"), item.get("instructor"))

        section_meetings = item.get("section_meetings")
        section_meeting_vals = [
            (section_group_id, m_item.get("type_"), m_item.get("days"),
            m_item.get("start_time"), m_item.get("end_time"), m_item.get("bldg"),
            m_item.get("room"), m_item.get("number"), m_item.get("seats_avail"))
            for m_item in section_meetings
        ]
        cls.insert_section_meetings(conn, section_meeting_vals)

        general_meetings = item.get("general_meetings")
        if general_meetings:
            general_meeting_vals = [
                (section_group_id, m_item.get("type_"), m_item.get("days"),
                m_item.get("start_time"), m_item.get("end_time"), m_item.get("bldg"),
                m_item.get("room"), m_item.get("number"), m_item.get("essential"))
                for m_item in general_meetings
            ]
            cls.insert_general_meetings(conn, general_meeting_vals)

        dated_meetings = item.get("dated_meetings")
        if dated_meetings:
            dated_meeting_vals = [
                (section_group_id, m_item.get("type_"), m_item.get("days"),
                m_item.get("start_time"), m_item.get("end_time"), m_item.get("bldg"),
                m_item.get("room"), m_item.get("date"))
                for m_item in dated_meetings
            ]
            cls.insert_dated_meetings(conn, dated_meeting_vals)

    
    @classmethod
    def insert_course(cls, conn, subject_code, number, title):
        """
        Insert the course given by the combination
        (subject_code, number, title)
        into the database.
        """
        # Get subject code id
        # insert course, return id
        # prepared statement parameters: (subject_code, number, title)

        values = (subject_code, number, title)
        with conn.cursor() as cur:
            cur.execute("EXECUTE insert_course (%s, %s, %s)", values)
            course_id = cur.fetchone()[0]
        
        return course_id
        

    @staticmethod
    def insert_course_offering(conn, course_id, quarter_code):

        values = (course_id, quarter_code)
        with conn.cursor() as cur:
            cur.execute("EXECUTE insert_course_offering (%s, %s)", values)
            course_offering_id = cur.fetchone()[0]

        return course_offering_id


    @staticmethod
    def insert_section_group(conn, course_offering_id, code, instructor):
        
        values = (course_offering_id, code, instructor)
        with conn.cursor() as cur:
            cur.execute("EXECUTE insert_section_group (%s, %s, %s)", values)
            section_group_id = cur.fetchone()[0]

        return section_group_id


    @classmethod
    def insert_section_meetings(cls, conn, section_meeting_values):
        # $1: section group id 
        # $2: meeting type
        # $3: days
        # $4: start time
        # $5: end time
        # $6: building
        # $7: room
        # $8: (meeting) number
        # $9: seats available
        cls.execute_str_batch(conn, "EXECUTE insert_section_meetings "
            "(%s, %s, %s, %s, %s, %s, %s, %s, %s);", section_meeting_values)


    @classmethod
    def insert_general_meetings(cls, conn, general_meeting_values):
        # $1: section group id 
        # $2: meeting type
        # $3: days
        # $4: start time
        # $5: end time
        # $6: building
        # $7: room
        # $8: (meeting) number
        # $9: essential
        cls.execute_str_batch(conn, "EXECUTE insert_general_meetings "
            "(%s, %s, %s, %s, %s, %s, %s, %s, %s);", general_meeting_values)


    @classmethod
    def insert_dated_meetings(cls, conn, dated_meeting_values):
        # $1: section group id 
        # $2: meeting type
        # $3: days
        # $4: start time
        # $5: end time
        # $6: building
        # $7: room
        # $8: date
        cls.execute_str_batch(conn, "EXECUTE insert_dated_meetings "
            "(%s, %s, %s, %s, %s, %s, %s, %s);", dated_meeting_values)    


    @classmethod
    def reset_for_scrape(cls, conn):
        """
        Reset database for a new scrape. 
        Delete all course offerings, section groups, and meetings.
        """
        query_str = """
            TRUNCATE section_meeting, general_meeting, dated_meeting, meeting, section_group, course_offering;
        """
        cls.execute_str(conn, query_str)