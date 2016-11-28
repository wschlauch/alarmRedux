package org.bitsea.alarmRedux;

import java.text.ParseException;
import java.util.HashMap;

import org.apache.camel.Exchange;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import ca.uhn.hl7v2.HL7Exception;

public class CassandraExceptionHandler {

	 /*
	  * expect the error message to come with the handling
	  * write out from exchange the  message id and patient id, if exists, or grep from database (bed, station, room)
	  * the type of error, the error message, and the complete exchange text
	 */
	
	public static void QVExceptionHandling(String message, MessageDecoder exc, Session s) throws HL7Exception, NullPointerException, ParseException {
		String typeOfError = "Query Validation failed";
		insertError(message, exc, typeOfError, s);
	}
	
	public static void WTExceptionHandling(String message, MessageDecoder exc, Session s) throws HL7Exception, NullPointerException, ParseException {
		String typeOfError = "Write Timeout";
		insertError(message, exc, typeOfError, s);
	}
	
	
	private static void insertError(String message, MessageDecoder exc, String name, Session s) throws HL7Exception, NullPointerException, ParseException {
		// got query valditation exception
		Insert stmt = QueryBuilder.insertInto("error_log");
		
		String typeOfError = name;
		String completeMessage = exc.getMsg();
		String tmp = exc.getPatientID();
		HashMap<String, String> mi = exc.generalInformation();
		// if patientid not in mcode, retrieve generated id 
		// from corresponding table
		int patid = tmp != null ? Integer.parseInt(tmp) : s.execute(QueryBuilder
				.select().column("patid").from("patient_bed_station").where(QueryBuilder
						.eq("roomNr", mi.get("roomInformation"))).and(QueryBuilder
						.eq("station", mi.get("stationInformation"))).and(QueryBuilder
						.eq("bedNr", mi.get("bedInformation")))).one().getInt("patid");
		long time = exc.getOBRTime();
//		stmt.value("patid", patid);
		stmt.value("msgctrlid", mi.get("msgctrlid"));
		stmt.value("errortype", typeOfError);
		stmt.value("error_msg", message);
		stmt.value("sent_content", completeMessage);
		stmt.value("sendTime", time);
		stmt.value("receivedTime", System.currentTimeMillis());
		s.executeAsync(stmt);
	}
	
	// when a transmission error occurred
	public static void TExceptionHandling(String cause, Exchange message, Session s) {
		Insert stmt = QueryBuilder.insertInto("error_log");
		
		String typeOfError = "Transmission error";
		String completeMsg = message.getIn().getBody(String.class);
//		int patId = -1;
		String[] splitMsg = completeMsg.split("\r");
		String msgCtrlId = splitMsg[0].split("\\|")[9];
		String errorMsg = "This type of message has not been implemented yet.";
		long time = splitMsg[1].length() == 2 ? Long.parseLong(splitMsg[1].split("|")[1]) : System.currentTimeMillis();
		
//		stmt.value("patid", patId);
		stmt.value("msgctrlid", msgCtrlId);
		stmt.value("errortype", typeOfError);
		stmt.value("error_msg", errorMsg);
		stmt.value("sent_content", completeMsg);
		stmt.value("receivedTime", System.currentTimeMillis());
		stmt.value("sendTime", time);
		s.executeAsync(stmt);

	}
}
