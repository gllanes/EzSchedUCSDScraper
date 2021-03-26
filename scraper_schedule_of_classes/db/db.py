import os

import psycopg2 as pg
import psycopg2.extras as pg_extras
import psycopg2.pool as pgpool
from dotenv import load_dotenv

load_dotenv()
DB_CONNECTION_STR = "dbname={} user={}".format(
    os.getenv("DB_NAME_LOCAL"),
    os.getenv("DB_USER_LOCAL")
)

class DataAccess:
    
    # There is going to be one thread to scrape each page.
    # Keep one connection and cursor per thread.
    
    def __init__(self):
        """
        Acquire database connection.
        """            
        self.conn = pg.connect(DB_CONNECTION_STR)

    def insert_quarter(self, quarter, name):
        query_str = """
            INSERT INTO quarter (code, name)
            VALUES (%s, %s)
            ON CONFLICT (code)
            DO UPDATE SET code = EXCLUDED.code
            RETURNING id;
        """
        
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(query_str, (quarter, name))
                quarter_id = cur.fetchone()[0]
            
        return quarter_id


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
            VALUES %s
            ON CONFLICT (code)
            DO UPDATE SET code = EXCLUDED.code
            RETURNING id;
        """

        # Build a tuple for each entry
        subject_codes_names_params = [(scn["code"], scn["name"]) for scn in subject_codes_names]
        
        with self.conn:
            with self.conn.cursor() as cur:
                pg_extras.execute_values(cur, query_str, subject_codes_names_params, 
                    page_size=len(subject_codes_names))
                subj_ids = cur.fetchall()
            
        return subj_ids

    def get_all_subjects(self):
        """
        retrieve all existing subjects in the database.

        returns an array of subjects.
        Each subject is of the form (subject_code, subject_name)
        """

        query_str = """
            SELECT code, name from subject;
        """

        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(query_str)
                subjects = cur.fetchall()

        return subjects

    def close(self):
        self.conn.close()
    
    # @classmethod
    # def insert_course(cls, conn_cur, subj_id, number, title):
    #     (conn, cur) = conn_cur
    #     query_str = """
    #         INSERT INTO course (subject_id, _number, title)
    #         VALUES (%s, %s, %s)
    #         ON CONFLICT (subject_id, _number)
    #         DO UPDATE SET subject_id = EXCLUDED.subject_id
    #         RETURNING id;
    #     """
        
    #     with conn:
    #         cur.execute(query_str, (subj_id, number, title))
    #         course_id = cur.fetchone()[0]
            
    #     return course_id

    # @classmethod
    # def insert_courses(cls, conn_cur, courses):
    #     (conn, cur) = conn_cur
    #     query_str = """
    #         INSERT INTO course (subject_id, _number, title)
    #         VALUES %s
    #         ON CONFLICT (subject_id, _number)
    #         DO UPDATE SET subject_id = EXCLUDED.subject_id
    #         RETURNING id;
    #     """
        
    #     with conn:
    #         pg_extras.execute_values(cur, query_str, courses, page_size=len(courses))
    #         course_ids = cur.fetchall()
            
    #     return course_ids

    # @classmethod
    # def insert_course_offerings(cls, conn_cur, quarter_id, courses):
    #     # First, get the ids of all the courses inserted
    #     course_ids = cls.insert_courses(conn_cur, courses)
    #     quarter_to_course_id = [(quarter_id, c_id) for c_id in course_ids]

    #     (conn, cur) = conn_cur
    #     query_str = """
    #         INSERT INTO course_offering (quarter_id, course_id)
    #         VALUES %s
    #         ON CONFLICT (quarter_id, course_id)
    #         DO NOTHING;
    #     """

    #     with conn:
    #         pg_extras.execute_values(cur, query_str, quarter_to_course_id)
    
    # @classmethod
    # def insert_course_offering(cls, conn_cur, quarter_id, course_id):
    #     (conn, cur) = conn_cur
    #     query_str = """
    #         INSERT INTO course_offering (quarter_id, course_id)
    #         VALUES (%s, %s)
    #         ON CONFLICT (quarter_id, course_id)
    #         DO NOTHING;
    #     """
        
    #     with conn:
    #         cur.execute(query_str, (quarter_id, course_id))

    # @classmethod
    # def close(cls):
    #     cls.connection_pool.closeall()