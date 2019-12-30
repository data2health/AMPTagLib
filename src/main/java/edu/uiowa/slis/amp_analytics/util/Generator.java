package edu.uiowa.slis.amp_analytics.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Hashtable;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.cd2h.JSONTagLib.util.LocalProperties;
import org.cd2h.JSONTagLib.util.PropertyLoader;

public class Generator {
    protected static Logger logger = Logger.getLogger(Generator.class);
    protected static Connection conn = null;
    protected static LocalProperties props = null;
    
    static Hashtable<String, Integer> idHash = new Hashtable<String, Integer>();

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
	PropertyConfigurator.configure("log4j.info");
	conn = getConnection();
	
	departments();
    }
    
    static void departments() throws SQLException {
	PreparedStatement comment = conn.prepareStatement("comment on materialized view amp.department is 'An AMP \"department\"'");
	comment.executeUpdate();
	comment.close();
	comment = conn.prepareStatement("comment on column amp.department.department_id is 'The unique identifier for an AMP department'");
	comment.executeUpdate();
	comment.close();
	comment = conn.prepareStatement("comment on column amp.department.name is 'The name of an AMP department'");
	comment.executeUpdate();
	comment.close();
	comment = conn.prepareStatement("comment on column amp.department.description is 'The description of an AMP department'");
	comment.executeUpdate();
	comment.close();
	
	PreparedStatement stmt = conn.prepareStatement("select department_id,name,description from amp.department");
	ResultSet rs = stmt.executeQuery();
	while (rs.next()) {
	    int departmentID = rs.getInt(1);
	    String departmentName = rs.getString(2);
	    String departmentDescription = rs.getString(3);
	    logger.info("department: " + departmentID + " - " + departmentName + " - " + departmentDescription);
	    surveys(departmentID);
	}
	stmt.close();
    }

    static void surveys(int departmentID) throws SQLException {
	PreparedStatement comment = conn.prepareStatement("comment on materialized view amp.survey is 'An AMP survey'");
	comment.executeUpdate();
	comment.close();
	comment = conn.prepareStatement("comment on column amp.survey.survey_id is 'The unique identifier for an AMP survey'");
	comment.executeUpdate();
	comment.close();
	comment = conn.prepareStatement("comment on column amp.survey.name is 'The name of an AMP survey'");
	comment.executeUpdate();
	comment.close();
	comment = conn.prepareStatement("comment on column amp.survey.description is 'The description of an AMP survey'");
	comment.executeUpdate();
	comment.close();
	comment = conn.prepareStatement("comment on column amp.survey.is_public	 is 'A flag indicating public access to the survey'");
	comment.executeUpdate();
	comment.close();
	comment = conn.prepareStatement("comment on column amp.survey.status is 'The status of an AMP survey'");
	comment.executeUpdate();
	comment.close();
	comment = conn.prepareStatement("comment on column amp.survey.department_id is 'The unique id of an AMP department to which this survey belongs'");
	comment.executeUpdate();
	comment.close();
	
	PreparedStatement stmt = conn.prepareStatement("select survey_id,name,description,is_public,status,department_id from amp.survey where department_id = ?");
	stmt.setInt(1, departmentID);
	ResultSet rs = stmt.executeQuery();
	while (rs.next()) {
	    int surveyID = rs.getInt(1);
	    String surveyName = rs.getString(2);
	    String surveyDescription = rs.getString(3);
	    boolean isPublic = rs.getBoolean(4);
	    String surveyStatus = rs.getString(5);
	    //int surveyDepartmentID = rs.getInt(6);
	    logger.info("\tsurvey: " + surveyID + " - " + surveyName + " - " + surveyDescription + " - " + isPublic + " - " + surveyStatus);
	    survey_pages(surveyID);
	}
	stmt.close();
    }

    static void survey_pages(int surveyID) throws SQLException {
	PreparedStatement comment = conn.prepareStatement("comment on materialized view amp.survey_page is 'A survey page for an AMP survey'");
	comment.executeUpdate();
	comment.close();
	comment = conn.prepareStatement("comment on column amp.survey_page.page_id is 'The unique identifier for a page in an AMP survey'");
	comment.executeUpdate();
	comment.close();
	comment = conn.prepareStatement("comment on column amp.survey_page.survey_id is 'The unique identifier for an AMP survey'");
	comment.executeUpdate();
	comment.close();
	comment = conn.prepareStatement("comment on column amp.survey_page.page_order is 'The presentation order of a page in an AMP survey'");
	comment.executeUpdate();
	comment.close();
	comment = conn.prepareStatement("comment on column amp.survey_page.title is 'The label of a question an AMP survey'");
	comment.executeUpdate();
	comment.close();
	comment = conn.prepareStatement("comment on column amp.survey_page.instructions is 'The instructions for responding to a question in an AMP survey'");
	comment.executeUpdate();
	comment.close();
	
	StringBuffer attributes = new StringBuffer("survey_id");
	StringBuffer triggerAttributes = new StringBuffer("survey_id");
	PreparedStatement stmt = conn.prepareStatement("select page_id,survey_id,page_order,title,instructions from amp.survey_page where survey_id = ? order by page_order");
	stmt.setInt(1, surveyID);
	ResultSet rs = stmt.executeQuery();
	while (rs.next()) {
	    int pageID = rs.getInt(1);
	    //int pageSurveyID = rs.getInt(2);
	    int pageOrder = rs.getInt(3);
	    String pageTitle = rs.getString(4);
	    String pageInstruction = rs.getString(5);
	    logger.info("\t\tpage: " + pageID + " - " + pageOrder + " - " + pageTitle + " - " + pageInstruction);
	    questions(surveyID, pageID, pageOrder, attributes, triggerAttributes);
	}
	logger.debug("\t\tattributes: " + attributes.toString());
	stmt.close();
	materializeView("survey_data_" + surveyID, "survey_data_" + surveyID, attributes.toString(), triggerAttributes.toString());
	idHash = new Hashtable<String, Integer>();
    }

    static void questions(int surveyID, int pageID, int pageOrder, StringBuffer attributes, StringBuffer triggerAttributes) throws SQLException {
	PreparedStatement comment = conn.prepareStatement("comment on materialized view amp.question is 'A question on an AMP survey'");
	comment.executeUpdate();
	comment.close();
	comment = conn.prepareStatement("comment on column amp.question.question_id is 'The unique identifier for a question in an AMP survey'");
	comment.executeUpdate();
	comment.close();
	comment = conn.prepareStatement("comment on column amp.question.page_id is 'The unique identifier for the page on which a question appeears in an AMP survey'");
	comment.executeUpdate();
	comment.close();
	comment = conn.prepareStatement("comment on column amp.question.question_order is 'The presentation order of a question on a page in an AMP survey'");
	comment.executeUpdate();
	comment.close();
	comment = conn.prepareStatement("comment on column amp.question.question_text is 'The presented description of a question an AMP survey'");
	comment.executeUpdate();
	comment.close();
	comment = conn.prepareStatement("comment on column amp.question.type is 'The type of a question in an AMP survey'");
	comment.executeUpdate();
	comment.close();
	
	PreparedStatement stmt = conn.prepareStatement("select question_id,page_id,question_order,question_text,type from amp.question where page_id = ? order by question_order");
	stmt.setInt(1, pageID);
	ResultSet rs = stmt.executeQuery();
	while (rs.next()) {
	    int questionID = rs.getInt(1);
	    //int questionPageID = rs.getInt(2);
	    int questionOrder = rs.getInt(3);
	    String questionText = rs.getString(4);
	    String type = rs.getString(5);
	    logger.info("\t\t\tquestion: " + questionID + " - " + questionOrder + " - " + questionText + " - " + type);
	    switch(type) {
	    case "YES_NO_DROPDOWN":
		attributes.append(", p" + pageOrder + "q" + questionOrder + " as " + identifier(questionText));
		triggerAttributes.append(", p" + pageOrder + "q" + questionOrder);
		break;
	    case "SHORT_TEXT_INPUT":
	    case "LONG_TEXT_INPUT":
	    case "HUGE_TEXT_INPUT":
		attributes.append(", p" + pageOrder + "q" + questionOrder + "::text as " + identifier(questionText));
		triggerAttributes.append(", p" + pageOrder + "q" + questionOrder);
		break;
	    case "INTEGER_INPUT":
		attributes.append(", p" + pageOrder + "q" + questionOrder + " as " + identifier(questionText));
		triggerAttributes.append(", p" + pageOrder + "q" + questionOrder);
		break;
	    case "CURRENCY_INPUT":
	    case "DECIMAL_INPUT":
		attributes.append(", p" + pageOrder + "q" + questionOrder + " as " + identifier(questionText));
		triggerAttributes.append(", p" + pageOrder + "q" + questionOrder);
		break;
	    case "DATE_INPUT":
		attributes.append(", p" + pageOrder + "q" + questionOrder + " as " + identifier(questionText));
		triggerAttributes.append(", p" + pageOrder + "q" + questionOrder);
		break;
	    case "SINGLE_CHOICE_DROP_DOWN":
		attributes.append(", p" + pageOrder + "q" + questionOrder + "::text as " + identifier(questionText));
		triggerAttributes.append(", p" + pageOrder + "q" + questionOrder);
		break;
	    case "MULTIPLE_CHOICE_CHECKBOXES":
		PreparedStatement subStmt = conn.prepareStatement("select option_order,option_text,option_value from amp.question_option where question_id = ? order by option_order");
		subStmt.setInt(1, questionID);
		ResultSet subrs = subStmt.executeQuery();
		while (subrs.next()) {
		    int optionOrder = subrs.getInt(1);
		    String optionText = subrs.getString(2);
		    String optionValue = subrs.getString(3);
		    logger.info("\t\t\t\tcheckbox option: " + optionOrder + " - " + optionText + " - " + optionValue);
		    attributes.append(", p" + pageOrder + "q" + questionOrder + "o" + optionOrder + " as " + identifier(questionText+" "+optionText));
		    triggerAttributes.append(", p" + pageOrder + "q" + questionOrder + "o" + optionOrder);
		}
		subStmt.close();
		break;
	    //case "SINGLE_CHOICE_DROP_DOWN": // DataSet Drop Down appears like this in the database
	    case "SINGLE_CHOICE_RADIO_BUTTONS":
		attributes.append(", p" + pageOrder + "q" + questionOrder + "::text as " + identifier(questionText));
		attributes.append(", p" + pageOrder + "q" + questionOrder + "text::text as " + identifier(questionText+" text"));
		triggerAttributes.append(", p" + pageOrder + "q" + questionOrder);
		triggerAttributes.append(", p" + pageOrder + "q" + questionOrder + "text");
		break;
	    case "STAR_RATING":
	    case "SMILEY_FACES_RATING":
		attributes.append(", p" + pageOrder + "q" + questionOrder + " as " + identifier(questionText));
		triggerAttributes.append(", p" + pageOrder + "q" + questionOrder);
		break;
	    case "YES_NO_DROPDOWN_MATRIX":
	    case "INTEGER_INPUT_MATRIX":
	    case "CURRENCY_INPUT_MATRIX":
	    case "DECIMAL_INPUT_MATRIX":
	    case "DATE_INPUT_MATRIX":
		subStmt = conn.prepareStatement("select row_label_order,label from amp.question_row_label where question_id = ? order by row_label_order");
		subStmt.setInt(1, questionID);
		subrs = subStmt.executeQuery();
		while (subrs.next()) {
		    int rowOrder = subrs.getInt(1);
		    String rowLabel = subrs.getString(2);
		    logger.info("\t\t\t\tmatrix row label: " + rowOrder + " - " + rowLabel);
			PreparedStatement subSubStmt = conn.prepareStatement("select column_label_order,label from amp.question_column_label where question_id = ? order by column_label_order");
			subSubStmt.setInt(1, questionID);
			ResultSet subsubrs = subSubStmt.executeQuery();
			while (subsubrs.next()) {
			    int columnOrder = subsubrs.getInt(1);
			    String columnLabel = subsubrs.getString(2);
			    logger.info("\t\t\t\t\tmatrix column label: " + columnOrder + " - " + columnLabel);
			    attributes.append(", p" + pageOrder + "q" + questionOrder + "r" + rowOrder + "c" + columnOrder + " as " + identifier(questionText+" "+rowLabel+" "+columnLabel));
			    triggerAttributes.append(", p" + pageOrder + "q" + questionOrder + "r" + rowOrder + "c" + columnOrder);
			}
			subSubStmt.close();
		}
		subStmt.close();
		break;
	    case "SHORT_TEXT_INPUT_MATRIX":
		subStmt = conn.prepareStatement("select row_label_order,label from amp.question_row_label where question_id = ? order by row_label_order");
		subStmt.setInt(1, questionID);
		subrs = subStmt.executeQuery();
		while (subrs.next()) {
		    int rowOrder = subrs.getInt(1);
		    String rowLabel = subrs.getString(2);
		    logger.info("\t\t\t\tmatrix row label: " + rowOrder + " - " + rowLabel);
			PreparedStatement subSubStmt = conn.prepareStatement("select column_label_order,label from amp.question_column_label where question_id = ? order by column_label_order");
			subSubStmt.setInt(1, questionID);
			ResultSet subsubrs = subSubStmt.executeQuery();
			while (subsubrs.next()) {
			    int columnOrder = subsubrs.getInt(1);
			    String columnLabel = subsubrs.getString(2);
			    logger.info("\t\t\t\t\tmatrix column label: " + columnOrder + " - " + columnLabel);
			    attributes.append(", p" + pageOrder + "q" + questionOrder + "r" + rowOrder + "c" + columnOrder + "::text as " + identifier(questionText+" "+rowLabel+" "+columnLabel));
			    triggerAttributes.append(", p" + pageOrder + "q" + questionOrder + "r" + rowOrder + "c" + columnOrder);
			}
			subSubStmt.close();
		}
		subStmt.close();
		break;
	    case "IMAGE_DISPLAY":
	    case "VIDEO_DISPLAY":
		// nothing to do for these two
		break;
	    case "FILE_UPLOAD":
		//TODO we'll need to suss out what to do with this at some point
		break;
	    default:	
		break;
	    }
	}
	stmt.close();
    }
    
    static void materializeView(String viewName, String sourceTable, String attributes, String triggerAttributes) {
	simpleStmt("drop materialized view if exists amp." + viewName + " cascade");
	simpleStmt("create materialized view amp." + viewName + " as select " + attributes + " from amp_survey." + sourceTable);
	
	simpleStmt("DROP FUNCTION IF EXISTS refresh_" + sourceTable + " CASCADE;");
	simpleStmt("CREATE FUNCTION refresh_" + sourceTable + "() RETURNS TRIGGER AS $body$"
		   +" BEGIN"
		   +"    REFRESH MATERIALIZED VIEW amp." + viewName + ";"
		   +"    RETURN NEW;"
		   +" END;"
		   +" $body$ LANGUAGE plpgsql ;");

	simpleStmt("DROP TRIGGER IF EXISTS refresh_" + sourceTable + " ON amp_survey." + sourceTable + " CASCADE;");
	simpleStmt("CREATE TRIGGER refresh_" + sourceTable
		   +" AFTER"
		   +"    INSERT"
		   +" OR UPDATE OF " + triggerAttributes
		   +" OR DELETE"
		   +" OR TRUNCATE"
		   +" ON amp_survey." + sourceTable
		   +" EXECUTE PROCEDURE refresh_" + sourceTable + "();");
    }
    
    static String identifier(String source) {
	String candidate = source.replaceAll("[^a-zA-Z0-9]", "_").toLowerCase();
	if (!Character.isAlphabetic(candidate.charAt(0)))
	    candidate = "x" + candidate;
	if (idHash.containsKey(candidate)) {
	    int count = idHash.get(candidate) + 1;
	    idHash.put(candidate, count);
	    candidate += "_"+count;
	} else {
	    idHash.put(candidate, 1);
	}
	return candidate;
    }

    public static Connection getConnection() throws SQLException, ClassNotFoundException {
	props = PropertyLoader.loadProperties("cd2h");
	Class.forName("org.postgresql.Driver");
    	Properties pprops = new Properties();
    	pprops.setProperty("user", props.getProperty("jdbc.user"));
    	pprops.setProperty("password", props.getProperty("jdbc.password"));
    	Connection conn = DriverManager.getConnection(props.getProperty("jdbc.url"), pprops);
    	return conn;
        }

    public static void simpleStmt(String queryString) {
	try {
	    logger.debug("executing " + queryString + "...");
	    PreparedStatement beginStmt = conn.prepareStatement(queryString);
	    beginStmt.executeUpdate();
	    beginStmt.close();
	} catch (Exception e) {
	    logger.error("Error in database initialization: " + e);
	    e.printStackTrace();
	}
    }

}
