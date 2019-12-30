DROP SCHEMA IF EXISTS amp CASCADE;
CREATE SCHEMA amp; 
 
DROP MATERIALIZED VIEW IF EXISTS amp.department CASCADE;
CREATE MATERIALIZED VIEW amp.department AS
 SELECT
    id AS department_id,
    name::text AS name,
    description::text AS description,
    version
FROM amp_survey.department;

DROP FUNCTION IF EXISTS refresh_department CASCADE;
CREATE FUNCTION refresh_department() RETURNS TRIGGER AS $body$
    BEGIN
        REFRESH MATERIALIZED VIEW amp.department;
        RETURN NEW;
    END;
$body$ LANGUAGE plpgsql ;

DROP TRIGGER IF EXISTS refresh_department ON amp_survey.department CASCADE;
CREATE TRIGGER refresh_department
AFTER
    INSERT
 OR UPDATE OF id,name,description,version
 OR DELETE
 OR TRUNCATE
ON amp_survey.department
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
FROM amp_survey.survey_definition;

DROP FUNCTION IF EXISTS refresh_survey CASCADE;
CREATE FUNCTION refresh_survey() RETURNS TRIGGER AS $body$
    BEGIN
	    REFRESH MATERIALIZED VIEW amp.survey;
        RETURN NEW;
    END;
$body$ LANGUAGE plpgsql ;

DROP TRIGGER IF EXISTS refresh_survey ON amp_survey.survey_definition CASCADE;
CREATE TRIGGER refresh_survey
AFTER
    INSERT
 OR UPDATE OF id,name,description,is_public,status,department_id
 OR DELETE
 OR TRUNCATE
ON amp_survey.survey_definition
EXECUTE PROCEDURE refresh_survey();

DROP MATERIALIZED VIEW IF EXISTS amp.survey_page CASCADE;
CREATE MATERIALIZED VIEW amp.survey_page AS
SELECT
    id AS page_id,
    survey_definition_id AS survey_id,
    page_order,
    title::text AS title,
    instructions::text AS instructions
FROM amp_survey.survey_definition_page;

DROP FUNCTION IF EXISTS refresh_survey_page CASCADE;
CREATE FUNCTION refresh_survey_page() RETURNS TRIGGER AS $body$
    BEGIN
        REFRESH MATERIALIZED VIEW amp.survey_page;
        RETURN NEW;
    END;
$body$ LANGUAGE plpgsql ;

DROP TRIGGER IF EXISTS refresh_survey_page ON amp_survey.survey_definition_page CASCADE;
CREATE TRIGGER refresh_survey_page
AFTER
    INSERT
 OR UPDATE OF id,survey_definition_id,page_order,title,instructions
 OR DELETE
 OR TRUNCATE
ON amp_survey.survey_definition_page
EXECUTE PROCEDURE refresh_survey_page();

DROP MATERIALIZED VIEW IF EXISTS amp.question CASCADE;
CREATE MATERIALIZED VIEW amp.question AS
SELECT
    id AS question_id,
    survey_definition_page_id as page_id,
    question_order,
    substring(question_text, '<p>(.*)</p>') AS question_text,
    type::text as type
FROM amp_survey.question;

DROP FUNCTION IF EXISTS refresh_question CASCADE;
CREATE FUNCTION refresh_question() RETURNS TRIGGER AS $body$
    BEGIN
        REFRESH MATERIALIZED VIEW amp.question;
        RETURN NEW;
    END;
$body$ LANGUAGE plpgsql ;

DROP TRIGGER IF EXISTS refresh_question ON amp_survey.question CASCADE;
CREATE TRIGGER refresh_question
AFTER
    INSERT
 OR UPDATE OF id,survey_definition_page_id,question_order,question_text,type
 OR DELETE
 OR TRUNCATE
ON amp_survey.question
EXECUTE PROCEDURE refresh_question();

DROP MATERIALIZED VIEW IF EXISTS amp.question_option CASCADE;
CREATE MATERIALIZED VIEW amp.question_option AS
SELECT
    id AS question_option_id,
    question_id,
    option_order,
    option_text::text as option_text,
    option_value::text as option_value
FROM amp_survey.question_option;

DROP FUNCTION IF EXISTS refresh_question_option CASCADE;
CREATE FUNCTION refresh_question_option() RETURNS TRIGGER AS $body$
    BEGIN
        REFRESH MATERIALIZED VIEW amp.question_option;
        RETURN NEW;
    END;
$body$ LANGUAGE plpgsql ;

DROP TRIGGER IF EXISTS refresh_question_option ON amp_survey.question_option CASCADE;
CREATE TRIGGER refresh_question_option
AFTER
    INSERT
 OR UPDATE OF id,question_id,option_order,option_text,option_value
 OR DELETE
 OR TRUNCATE
ON amp_survey.question_option
EXECUTE PROCEDURE refresh_question_option();

DROP MATERIALIZED VIEW IF EXISTS amp.question_row_label CASCADE;
CREATE MATERIALIZED VIEW amp.question_row_label AS
SELECT
    id AS question_row_id,
    question_id,
    row_label_order,
    label::text as label
FROM amp_survey.question_row_label;

DROP FUNCTION IF EXISTS refresh_question_row_label CASCADE;
CREATE FUNCTION refresh_question_row_label() RETURNS TRIGGER AS $body$
    BEGIN
        REFRESH MATERIALIZED VIEW amp.question_row_label;
        RETURN NEW;
    END;
$body$ LANGUAGE plpgsql ;

DROP TRIGGER IF EXISTS refresh_question_row_label ON amp_survey.question_row_label CASCADE;
CREATE TRIGGER refresh_question_row_label
AFTER
    INSERT
 OR UPDATE OF id,question_id,row_label_order,label
 OR DELETE
 OR TRUNCATE
ON amp_survey.question_row_label
EXECUTE PROCEDURE refresh_question_row_label();

DROP MATERIALIZED VIEW IF EXISTS amp.question_column_label CASCADE;
CREATE MATERIALIZED VIEW amp.question_column_label AS
SELECT
    id AS question_column_id,
    question_id,
    column_label_order,
    label::text as label
FROM amp_survey.question_column_label;

DROP FUNCTION IF EXISTS refresh_question_column_label CASCADE;
CREATE FUNCTION refresh_question_column_label() RETURNS TRIGGER AS $body$
    BEGIN
        REFRESH MATERIALIZED VIEW amp.question_column_label;
        RETURN NEW;
    END;
$body$ LANGUAGE plpgsql ;

DROP TRIGGER IF EXISTS refresh_question_column_label ON amp_survey.question_column_label CASCADE;
CREATE TRIGGER refresh_question_column_label
AFTER
    INSERT
 OR UPDATE OF id,question_id,column_label_order,label
 OR DELETE
 OR TRUNCATE
ON amp_survey.question_column_label
EXECUTE PROCEDURE refresh_question_column_label();
