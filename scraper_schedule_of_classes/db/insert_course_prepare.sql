/*
Parameters: 
$1 - subject code
$2 - course number
$3 - course title
*/
PREPARE insert_course (text, text, text) AS 
    WITH subject_id AS (
        SELECT id FROM subject
        WHERE code = $1
    ), insert_course_get_id AS (
        INSERT INTO course (subject_id, number_, title)
        VALUES 
            ((SELECT * from subject_id), $2, $3)
        ON CONFLICT (subject_id, number_) DO NOTHING
        RETURNING id
    )
    SELECT * from insert_course_get_id
    UNION 
        SELECT id from course
        WHERE 
            subject_id = (SELECT * FROM subject_id) AND
            number_ = $2;