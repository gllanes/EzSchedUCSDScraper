/*
Parameters
$1 - course offering id
$2 - section group code
$3 - section group instructor
*/
PREPARE insert_section_group (int, text, text) AS
    WITH insert_section_group_get_id AS (
        INSERT INTO section_group (course_offering_id, code, instructor)
        VALUES 
            ($1, $2, $3)
        ON CONFLICT (course_offering_id, code) DO NOTHING
        RETURNING id
    )
    SELECT * FROM insert_section_group_get_id
    UNION
        SELECT id FROM section_group
        WHERE
            course_offering_id = $1 AND 
            code = $2