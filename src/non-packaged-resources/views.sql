DROP SCHEMA IF EXISTS amp CASCADE;
CREATE SCHEMA amp; 
 
DROP MATERIALIZED VIEW IF EXISTS amp.department CASCADE;
CREATE MATERIALIZED VIEW amp.department AS
 SELECT
    id AS department_id,
    name::text AS name,
    description::text AS description,
    version
FROM demo_survey.department;

comment on materialized view amp.department is 'An AMP "department"';
comment on column amp.department.department_id is 'The unique identifier for an AMP department';
comment on column amp.department.name is 'The name of an AMP department';
comment on column amp.department.description is 'The description of an AMP department';

DROP FUNCTION IF EXISTS refresh_department CASCADE;
CREATE FUNCTION refresh_department() RETURNS TRIGGER AS $body$
    BEGIN
        REFRESH MATERIALIZED VIEW amp.department;
        RETURN NEW;
    END;
$body$ LANGUAGE plpgsql ;

DROP TRIGGER IF EXISTS refresh_department ON demo_survey.department CASCADE;
CREATE TRIGGER refresh_department
AFTER
    INSERT
 OR UPDATE OF id,name,description,version
 OR DELETE
 OR TRUNCATE
ON demo_survey.department
EXECUTE PROCEDURE refresh_department();

DROP MATERIALIZED VIEW IF EXISTS amp.survey CASCADE;
CREATE MATERIALIZED VIEW amp.survey AS
SELECT
    id AS survey_id,
    name::text AS name,
    substring(description, '<p>(.*)</p>') AS description,
    is_public,
    status::text AS status,
    department_id
FROM demo_survey.survey_definition;

comment on materialized view amp.survey is 'An AMP survey';
comment on column amp.survey.survey_id is 'The unique identifier for an AMP survey';
comment on column amp.survey.name is 'The name of an AMP survey';
comment on column amp.survey.description is 'The description of an AMP survey';
comment on column amp.survey.is_public   is 'A flag indicating public access to the survey';
comment on column amp.survey.status is 'The status of an AMP survey';
comment on column amp.survey.department_id is 'The unique id of an AMP department to which this survey belongs';

DROP FUNCTION IF EXISTS refresh_survey CASCADE;
CREATE FUNCTION refresh_survey() RETURNS TRIGGER AS $body$
    BEGIN
	    REFRESH MATERIALIZED VIEW amp.survey;
        RETURN NEW;
    END;
$body$ LANGUAGE plpgsql ;

DROP TRIGGER IF EXISTS refresh_survey ON demo_survey.survey_definition CASCADE;
CREATE TRIGGER refresh_survey
AFTER
    INSERT
 OR UPDATE OF id,name,description,is_public,status,department_id
 OR DELETE
 OR TRUNCATE
ON demo_survey.survey_definition
EXECUTE PROCEDURE refresh_survey();

DROP MATERIALIZED VIEW IF EXISTS amp.survey_page CASCADE;
CREATE MATERIALIZED VIEW amp.survey_page AS
SELECT
    id AS page_id,
    survey_definition_id AS survey_id,
    page_order,
    title::text AS title,
    instructions::text AS instructions
FROM demo_survey.survey_definition_page;

comment on materialized view amp.survey_page is 'A survey page for an AMP survey';
comment on column amp.survey_page.page_id is 'The unique identifier for a page in an AMP survey';
comment on column amp.survey_page.survey_id is 'The unique identifier for an AMP survey';
comment on column amp.survey_page.page_order is 'The presentation order of a page in an AMP survey';
comment on column amp.survey_page.title is 'The label of a question an AMP survey';
comment on column amp.survey_page.instructions is 'The instructions for responding to a question in an AMP survey';

DROP FUNCTION IF EXISTS refresh_survey_page CASCADE;
CREATE FUNCTION refresh_survey_page() RETURNS TRIGGER AS $body$
    BEGIN
        REFRESH MATERIALIZED VIEW amp.survey_page;
        RETURN NEW;
    END;
$body$ LANGUAGE plpgsql ;

DROP TRIGGER IF EXISTS refresh_survey_page ON demo_survey.survey_definition_page CASCADE;
CREATE TRIGGER refresh_survey_page
AFTER
    INSERT
 OR UPDATE OF id,survey_definition_id,page_order,title,instructions
 OR DELETE
 OR TRUNCATE
ON demo_survey.survey_definition_page
EXECUTE PROCEDURE refresh_survey_page();

DROP MATERIALIZED VIEW IF EXISTS amp.question CASCADE;
CREATE MATERIALIZED VIEW amp.question AS
SELECT
    id AS question_id,
    survey_definition_page_id as page_id,
    question_order,
    substring(question_text, '<p>(.*)</p>') AS question_text,
    type::text as type
FROM demo_survey.question;

comment on materialized view amp.question is 'A question on an AMP survey';
comment on column amp.question.question_id is 'The unique identifier for a question in an AMP survey';
comment on column amp.question.page_id is 'The unique identifier for the page on which a question appeears in an AMP survey';
comment on column amp.question.question_order is 'The presentation order of a question on a page in an AMP survey';
comment on column amp.question.question_text is 'The presented description of a question an AMP survey';
comment on column amp.question.type is 'The type of a question in an AMP survey';

DROP FUNCTION IF EXISTS refresh_question CASCADE;
CREATE FUNCTION refresh_question() RETURNS TRIGGER AS $body$
    BEGIN
        REFRESH MATERIALIZED VIEW amp.question;
        RETURN NEW;
    END;
$body$ LANGUAGE plpgsql ;

DROP TRIGGER IF EXISTS refresh_question ON demo_survey.question CASCADE;
CREATE TRIGGER refresh_question
AFTER
    INSERT
 OR UPDATE OF id,survey_definition_page_id,question_order,question_text,type
 OR DELETE
 OR TRUNCATE
ON demo_survey.question
EXECUTE PROCEDURE refresh_question();

DROP MATERIALIZED VIEW IF EXISTS amp.question_option CASCADE;
CREATE MATERIALIZED VIEW amp.question_option AS
SELECT
    id AS question_option_id,
    question_id,
    option_order,
    option_text::text as option_text,
    option_value::text as option_value
FROM demo_survey.question_option;

DROP FUNCTION IF EXISTS refresh_question_option CASCADE;
CREATE FUNCTION refresh_question_option() RETURNS TRIGGER AS $body$
    BEGIN
        REFRESH MATERIALIZED VIEW amp.question_option;
        RETURN NEW;
    END;
$body$ LANGUAGE plpgsql ;

DROP TRIGGER IF EXISTS refresh_question_option ON demo_survey.question_option CASCADE;
CREATE TRIGGER refresh_question_option
AFTER
    INSERT
 OR UPDATE OF id,question_id,option_order,option_text,option_value
 OR DELETE
 OR TRUNCATE
ON demo_survey.question_option
EXECUTE PROCEDURE refresh_question_option();

DROP MATERIALIZED VIEW IF EXISTS amp.question_row_label CASCADE;
CREATE MATERIALIZED VIEW amp.question_row_label AS
SELECT
    id AS question_row_id,
    question_id,
    row_label_order,
    label::text as label
FROM demo_survey.question_row_label;

DROP FUNCTION IF EXISTS refresh_question_row_label CASCADE;
CREATE FUNCTION refresh_question_row_label() RETURNS TRIGGER AS $body$
    BEGIN
        REFRESH MATERIALIZED VIEW amp.question_row_label;
        RETURN NEW;
    END;
$body$ LANGUAGE plpgsql ;

DROP TRIGGER IF EXISTS refresh_question_row_label ON demo_survey.question_row_label CASCADE;
CREATE TRIGGER refresh_question_row_label
AFTER
    INSERT
 OR UPDATE OF id,question_id,row_label_order,label
 OR DELETE
 OR TRUNCATE
ON demo_survey.question_row_label
EXECUTE PROCEDURE refresh_question_row_label();

DROP MATERIALIZED VIEW IF EXISTS amp.question_column_label CASCADE;
CREATE MATERIALIZED VIEW amp.question_column_label AS
SELECT
    id AS question_column_id,
    question_id,
    column_label_order,
    label::text as label
FROM demo_survey.question_column_label;

DROP FUNCTION IF EXISTS refresh_question_column_label CASCADE;
CREATE FUNCTION refresh_question_column_label() RETURNS TRIGGER AS $body$
    BEGIN
        REFRESH MATERIALIZED VIEW amp.question_column_label;
        RETURN NEW;
    END;
$body$ LANGUAGE plpgsql ;

DROP TRIGGER IF EXISTS refresh_question_column_label ON demo_survey.question_column_label CASCADE;
CREATE TRIGGER refresh_question_column_label
AFTER
    INSERT
 OR UPDATE OF id,question_id,column_label_order,label
 OR DELETE
 OR TRUNCATE
ON demo_survey.question_column_label
EXECUTE PROCEDURE refresh_question_column_label();
