package edu.uiowa.slis.amp_analytics.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.cd2h.JSONTagLib.util.LocalProperties;
import org.cd2h.JSONTagLib.util.PropertyLoader;

public class Generator {
    protected static Logger logger = Logger.getLogger(Generator.class);
    protected static Connection conn = null;
    protected static LocalProperties props = null;
    
    static String sourceSchema = null;
    static Hashtable<String, Integer> idHash = new Hashtable<String, Integer>();
    static Vector<String> questionIdentifiers = new Vector<String>();
    static Hashtable<String, String> questionLabelHash = new Hashtable<String, String>();
    
    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
	PropertyConfigurator.configure("log4j.info");
	props = PropertyLoader.loadProperties("amp");
	conn = getConnection();

	departments();
    }
    
    static void departments() throws SQLException, IOException {
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

    static void surveys(int departmentID) throws SQLException, IOException {
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

    static void survey_pages(int surveyID) throws SQLException, IOException {
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
	createDashboardPages("survey_data_" + surveyID);
	questionIdentifiers = new Vector<String>();
	questionLabelHash = new Hashtable<String, String>();
    }

    static void questions(int surveyID, int pageID, int pageOrder, StringBuffer attributes, StringBuffer triggerAttributes) throws SQLException {
	PreparedStatement stmt = conn.prepareStatement("select question_id,page_id,question_order,question_text,type from amp.question where page_id = ? order by question_order");
	stmt.setInt(1, pageID);
	ResultSet rs = stmt.executeQuery();
	while (rs.next()) {
	    int questionID = rs.getInt(1);
	    //int questionPageID = rs.getInt(2);
	    int questionOrder = rs.getInt(3);
	    String questionText = rs.getString(4).replace("\n ", "");
	    String type = rs.getString(5);
	    logger.info("\t\t\tquestion: " + questionID + " - " + questionOrder + " - " + questionText + " - " + type);
	    String identifier = null;
	    
	    switch(type) {
	    case "YES_NO_DROPDOWN":
		identifier = identifier(questionText, "", "");
		attributes.append(", p" + pageOrder + "q" + questionOrder + " as " + identifier);
		triggerAttributes.append(", p" + pageOrder + "q" + questionOrder);
		questionIdentifiers.add(identifier);
		questionLabelHash.put(identifier, questionText);
		break;
	    case "SHORT_TEXT_INPUT":
	    case "LONG_TEXT_INPUT":
	    case "HUGE_TEXT_INPUT":
		identifier = identifier(questionText, "", "");
		attributes.append(", p" + pageOrder + "q" + questionOrder + "::text as " + identifier);
		triggerAttributes.append(", p" + pageOrder + "q" + questionOrder);
		questionIdentifiers.add(identifier);
		questionLabelHash.put(identifier, questionText);
		break;
	    case "INTEGER_INPUT":
		identifier = identifier(questionText, "", "");
		attributes.append(", p" + pageOrder + "q" + questionOrder + " as " + identifier);
		triggerAttributes.append(", p" + pageOrder + "q" + questionOrder);
		questionIdentifiers.add(identifier);
		questionLabelHash.put(identifier, questionText);
		break;
	    case "CURRENCY_INPUT":
	    case "DECIMAL_INPUT":
		identifier = identifier(questionText, "", "");
		attributes.append(", p" + pageOrder + "q" + questionOrder + " as " + identifier);
		triggerAttributes.append(", p" + pageOrder + "q" + questionOrder);
		questionIdentifiers.add(identifier);
		questionLabelHash.put(identifier, questionText);
		break;
	    case "DATE_INPUT":
		identifier = identifier(questionText, "", "");
		attributes.append(", p" + pageOrder + "q" + questionOrder + " as " + identifier);
		triggerAttributes.append(", p" + pageOrder + "q" + questionOrder);
		questionIdentifiers.add(identifier);
		questionLabelHash.put(identifier, questionText);
		break;
	    case "SINGLE_CHOICE_DROP_DOWN":
		identifier = identifier(questionText, "", "");
		attributes.append(", p" + pageOrder + "q" + questionOrder + "::text as " + identifier);
		triggerAttributes.append(", p" + pageOrder + "q" + questionOrder);
		questionIdentifiers.add(identifier);
		questionLabelHash.put(identifier, questionText);
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
		    identifier = identifier(questionText, ""+optionText, "");
		    attributes.append(", p" + pageOrder + "q" + questionOrder + "o" + optionOrder + " as " + identifier);
		    triggerAttributes.append(", p" + pageOrder + "q" + questionOrder + "o" + optionOrder);
		    questionIdentifiers.add(identifier);
		    questionLabelHash.put(identifier, questionText + " : " + optionText);
		}
		subStmt.close();
		break;
	    //case "SINGLE_CHOICE_DROP_DOWN": // DataSet Drop Down appears like this in the database
	    case "SINGLE_CHOICE_RADIO_BUTTONS":
		identifier = identifier(questionText, "", "");
		attributes.append(", p" + pageOrder + "q" + questionOrder + "::text as " + identifier);
		triggerAttributes.append(", p" + pageOrder + "q" + questionOrder);
		questionIdentifiers.add(identifier);
		questionLabelHash.put(identifier, questionText);

		identifier = identifier(questionText, " text", "");
		attributes.append(", p" + pageOrder + "q" + questionOrder + "text::text as " + identifier);
		triggerAttributes.append(", p" + pageOrder + "q" + questionOrder + "text");
		questionIdentifiers.add(identifier);
		questionLabelHash.put(identifier, questionText + " text");
		break;
	    case "STAR_RATING":
	    case "SMILEY_FACES_RATING":
		identifier = identifier(questionText, "", "");
		attributes.append(", p" + pageOrder + "q" + questionOrder + " as " + identifier);
		triggerAttributes.append(", p" + pageOrder + "q" + questionOrder);
		questionIdentifiers.add(identifier);
		questionLabelHash.put(identifier, questionText);
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
			    identifier = identifier(questionText, rowLabel, columnLabel);
			    attributes.append(", p" + pageOrder + "q" + questionOrder + "r" + rowOrder + "c" + columnOrder + " as " + identifier);
			    triggerAttributes.append(", p" + pageOrder + "q" + questionOrder + "r" + rowOrder + "c" + columnOrder);
			    questionIdentifiers.add(identifier);
			    questionLabelHash.put(identifier, questionText + " : " + rowLabel + " : " + columnLabel);
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
			    identifier = identifier(questionText, rowLabel, columnLabel);
			    attributes.append(", p" + pageOrder + "q" + questionOrder + "r" + rowOrder + "c" + columnOrder + "::text as " + identifier);
			    triggerAttributes.append(", p" + pageOrder + "q" + questionOrder + "r" + rowOrder + "c" + columnOrder);
			    questionIdentifiers.add(identifier);
			    questionLabelHash.put(identifier, questionText + " : " + rowLabel + " : " + columnLabel);
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
	logger.info("attributes: " + attributes);
	simpleStmt("drop materialized view if exists amp." + viewName + " cascade");
	simpleStmt("create materialized view amp." + viewName + " as select " + attributes + " from " + sourceSchema + "." + sourceTable);
	
	simpleStmt("DROP FUNCTION IF EXISTS refresh_" + sourceTable + " CASCADE;");
	simpleStmt("CREATE FUNCTION refresh_" + sourceTable + "() RETURNS TRIGGER AS $body$"
		   +" BEGIN"
		   +"    REFRESH MATERIALIZED VIEW amp." + viewName + ";"
		   +"    RETURN NEW;"
		   +" END;"
		   +" $body$ LANGUAGE plpgsql ;");

	simpleStmt("DROP TRIGGER IF EXISTS refresh_" + sourceTable + " ON " + sourceSchema + "." + sourceTable + " CASCADE;");
	simpleStmt("CREATE TRIGGER refresh_" + sourceTable
		   +" AFTER"
		   +"    INSERT"
		   +" OR UPDATE OF " + triggerAttributes
		   +" OR DELETE"
		   +" OR TRUNCATE"
		   +" ON " + sourceSchema + "." + sourceTable
		   +" EXECUTE PROCEDURE refresh_" + sourceTable + "();");
    }
    
    static void createDashboardPages(String viewName) throws IOException {
	File feedFile = new File("/Users/eichmann/Documents/Components/workspace/AMP-dashboard/WebContent/dashboard/" + viewName + "_feed.jsp");
	FileWriter fileWriter = new FileWriter(feedFile);
	BufferedWriter writer = new BufferedWriter(fileWriter);

	writer.write("<%@ taglib prefix=\"c\" uri=\"http://java.sun.com/jsp/jstl/core\"%>\n");
	writer.write("<%@ taglib prefix=\"sql\" uri=\"http://java.sun.com/jsp/jstl/sql\"%>\n");
	writer.write("\n");
	writer.write("<sql:query var=\"responses\" dataSource=\"jdbc/AMP-dashboard\">\n");
	writer.write("	select jsonb_pretty(jsonb_agg(data)) from amp." + viewName + " as data;\n");
	writer.write("</sql:query>\n");
	writer.write("{\n");
	writer.write("	    \"headers\": [\n");
	writer.write("	        {\"value\":\"survey_id\" \"label\":\"Survey ID\"}");
	for (String questionIdentifier : questionIdentifiers) {
	    writer.write(",\n	        {\"value\":\"" + questionIdentifier + "\", \"label\":\"" + questionLabelHash.get(questionIdentifier) + "\"}");
	}
	writer.write("\n	    ],\n");
	writer.write("	    \"rows\" : \n");
	writer.write("<c:forEach items=\"${responses.rows}\" var=\"row\" varStatus=\"rowCounter\">\n");
	writer.write("		${row.jsonb_pretty}\n");
	writer.write("</c:forEach>\n");
	writer.write("}\n");

	writer.close();
	
	File displayFile = new File("/Users/eichmann/Documents/Components/workspace/AMP-dashboard/WebContent/dashboard/" + viewName + ".jsp");
	fileWriter = new FileWriter(displayFile);
	writer = new BufferedWriter(fileWriter);
	File displayTemplate = new File("/Users/eichmann/Documents/Components/workspace/AMPTagLib/src/non-packaged-resources/template.txt");
	FileReader fileReader = new FileReader(displayTemplate);
	BufferedReader reader = new BufferedReader(fileReader);
	String buffer = null;
	while ((buffer = reader.readLine()) != null) {
	    if (buffer.startsWith("$")) {
		writer.write(buffer.replace("feed", viewName + "_feed") + "\n");
	    } else if (buffer.contains("columns:")) {
		writer.write(buffer + "\n");
		writer.write("\t\t\t{ data: 'survey_id', visible: true, orderable: true}");
		for (String questionIdentifier : questionIdentifiers) {
		    writer.write(",\n\t\t\t{ data: '" + questionIdentifier + "', visible: true, orderable: true}");
		}
		writer.write("\n");
	    } else {
		writer.write(buffer + "\n");
	    }
	}
	writer.close();
	reader.close();
    }
    
    static String identifier(String schemaName, String rowLabel, String columnName) {
	int identifierMaxLength = 60; // actual limit is 63, but we need to account for potential "count" suffixes
	String source = null;

	logger.debug("schemaName: " + schemaName.length() + " : " + schemaName);
	logger.debug("rowLabel  : " + rowLabel.length() + " : " + rowLabel);
	logger.debug("columnName: " + columnName.length() + " : " + columnName);
	
	// first deal with long attribute names
	if (schemaName.length() + rowLabel.length() + columnName.length() + 2 > identifierMaxLength) {
	    if (columnName.length() == 0)
		// split constraint across the schema and row labels
		source = (schemaName.length() < 30 ? schemaName : schemaName.substring(0, 30))
			+ "_" + (rowLabel.length() < 30 ? rowLabel : rowLabel.substring(0, 30));
	    else
		// give half to the schema label and split the remainder between the row and column labels
		source = (schemaName.length() < 30 ? schemaName : schemaName.substring(0, 30))
			+ "_" + (rowLabel.length() < 15 ? rowLabel : rowLabel.substring(0, 15))
	    		+ "_" + (columnName.length() < 15 ? columnName : columnName.substring(0, 15));
	} else
	    source = schemaName + (rowLabel.length() == 0 ? "" : "_" + rowLabel) + (columnName.length() == 0 ? "" : "_" + columnName);
	
	// map invalid characters
	String candidate = source.replaceAll("[^a-zA-Z0-9]", "_").toLowerCase();
	
	// map invalid first characters
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
	sourceSchema = props.getProperty("jdbc.schema");
	logger.info("schema: " + sourceSchema);
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
