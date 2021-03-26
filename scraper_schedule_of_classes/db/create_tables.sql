create table if not exists quarter (
	id serial primary key,
	code char(4) not null unique,
	name varchar(40) not null
);

create table if not exists subject (
	id serial primary key,
	code varchar(4) not null unique,
	name varchar(50) not null
);

create table if not exists course (
	id serial primary key,
	subject_id integer references subject (id),
	_number varchar(7) not null,
	title varchar(200),
	unique (subject_id, _number)
);

create table if not exists course_offering (
	id serial primary key,
	quarter_id integer references quarter (id),
	course_id integer references course (id),
	unique (quarter_id, course_id)
);