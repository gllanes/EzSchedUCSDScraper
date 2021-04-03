/*insert common meeting info first, then section meeting info*/
/*
Parameters:
    $1: section group id 
    $2: meeting type
    $3: days
    $4: start time
    $5: end time
    $6: building
    $7: room
    $8: (meeting) number
    $9: seats available
*/
PREPARE insert_section_meetings (integer, text, text, time, time, text, text, text, integer) AS
    WITH insert_meeting_get_id AS (
        INSERT INTO meeting (section_group_id, type_, days, start_time, end_time, building, room)
        VALUES
            ($1, $2, $3, $4, $5, $6, $7)
        RETURNING id
    )
    INSERT INTO section_meeting (meeting_id, number_, seats_available)
        VALUES ((SELECT * FROM insert_meeting_get_id), $8, $9)