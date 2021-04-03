/*
Parameters:
$1 - course id
$2 - quarter code
*/
PREPARE insert_course_offering (int, text) AS
    WITH quarter_id AS (
        SELECT id FROM quarter
        WHERE code = $2
    ), insert_course_offering_get_id AS (
        INSERT INTO course_offering (quarter_id, course_id)
        VALUES 
            ((SELECT * FROM quarter_id), $1)
        ON CONFLICT (quarter_id, course_id) DO NOTHING
        RETURNING id
    )
    SELECT * FROM insert_course_offering_get_id
    UNION 
        SELECT id FROM course_offering
        WHERE 
            quarter_id = (SELECT * FROM quarter_id) AND
            course_id = $1;