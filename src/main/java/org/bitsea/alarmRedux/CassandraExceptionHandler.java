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
		Insert stmt = QueryBuilder.insertInto("error");
		
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
		stmt.value("patid", patid);
		stmt.value("msgctrlid", mi.get("msgctrlid"));
		stmt.value("mtype", typeOfError);
		stmt.value("error_msg", message);
		stmt.value("sent_content", completeMessage);
		stmt.value("sendTime", time);
		stmt.value("receivedTime", System.currentTimeMillis());
		s.executeAsync(stmt);
	}
}
