-- liquibase formatted sql

-- changeset GerardLlanes:CreateSubjectTable
create table if not exists subject (
	id serial primary key,
	code varchar(4) not null unique,
	name varchar(100)
);
-- rollback drop table if exists subject;

-- changeset GerardLlanes:CreateQuarterTable
create table if not exists quarter (
	id serial primary key,
	code char(4) not null unique,
	name varchar(50)
);
-- rollback drop table if exists quarter;

-- changeset GerardLlanes:CreateCourseTable
create table if not exists course (
	id serial primary key,
	subject_id integer not null references subject (id),
	number_ varchar(8) not null,
	title varchar(100),
	unique (subject_id, number_)
);
-- rollback drop table if exists course;

-- changeset GerardLlanes:CreateCourseOfferingTable
create table if not exists course_offering (
	id serial primary key,
	quarter_id integer not null references quarter (id),
	course_id integer not null references course (id),
	unique (quarter_id, course_id)
);
-- rollback drop table if exists course_offering;

-- changeset GerardLlanes:CreateSectionGroupTable
create table if not exists section_group (
	id serial primary key,
	course_offering_id integer not null references course_offering (id),
	code char(3) not null,
	instructor varchar(100),
	unique (course_offering_id, code)
);
-- rollback drop table if exists section_group;

-- changeset GerardLlanes:CreateMeetingTable
create table if not exists meeting (
	id serial primary key,
	section_group_id integer not null references section_group (id),
	type_ char(2) not null,
	days varchar(8),
	start_time time(0),
	end_time time(0),
	building varchar(50),
	room varchar(50)
);
-- rollback drop table if exists meeting;

-- changeset GerardLlanes:CreateGeneralMeetingTable
create table if not exists general_meeting (
	id serial primary key,
	meeting_id integer not null references meeting (id),
	number_ char(3) not null
);
-- rollback drop table if exists general_meeting;

-- changeset GerardLlanes:AddColumnEssentialToGeneralMeeting
alter table general_meeting
add column essential boolean not null;
-- rollback alter table general_meeting drop column essential;

-- changeset GerardLlanes:CreateSectionMeetingTable
create table if not exists section_meeting (
	id serial primary key,
	meeting_id integer not null references meeting (id),
	number_ char(3) not null,
	seats_available integer
);
-- rollback drop table if exists section_meeting;

--changeset GerardLlanes:CreateDatedMeetingTable
create table if not exists dated_meeting (
	id serial primary key,
	meeting_id integer not null references meeting (id),
	date_ varchar(10)
);
-- rollback drop table if exists dated_meeting;
