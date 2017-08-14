package org.bitsea.alarmRedux;

import java.text.ParseException;
import java.util.HashMap;

import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.model.Message;
import io.netty.util.internal.ThreadLocalRandom;

@Component
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
		String completeMsg = message.getIn().getBody().toString();
		String[] splitMsg = completeMsg.split("\r");
		String msgCtrlId = "";
		msgCtrlId = splitMsg[0].split("\\|").length > 0 ? splitMsg[0].split("\\|")[9] : "" + ThreadLocalRandom.current().nextInt(1000, 100000000);
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

	
	/*
	 * message received that had no MSH line
	 * write out details that are given, set empty/random values for rest of message
	 */
	public static void HExceptionHandling(String string, Exchange exchange, Session session) {
		String msgctrlid = "" + ThreadLocalRandom.current().nextInt(1000, 10000);
		Throwable caused = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
		String errortype = caused.toString();
		String error_msg = caused.getMessage();
		String content = exchange.getIn().getBody().toString();

		Insert stmt = QueryBuilder.insertInto("error_log");
		stmt.value("msgCtrlId", msgctrlid);
		stmt.value("errortype", errortype);
		stmt.value("error_msg", error_msg);
		stmt.value("sent_content", content);
		stmt.value("receivedTime", System.currentTimeMillis());
		stmt.value("sendTime", System.currentTimeMillis());
		session.executeAsync(stmt);
	}
	
	public static void weirdException(Exchange exchange, Session s) {
		String msgctrlid = "" + ThreadLocalRandom.current().nextInt(1000, 10000);
		String errortype = "Strange Error";
		String error_msg = "Cannot be helped";
		String content = "";
		try {
			content = exchange.getIn().getBody().toString();
		} catch (Exception q) {
			content = "No content";
		}
		Insert stmt = QueryBuilder.insertInto("error_log");
		stmt.value("msgCtrlId", msgctrlid);
		stmt.value("errortype", errortype);
		stmt.value("error_msg", error_msg);
		stmt.value("sent_content", content);
		stmt.value("receivedTime", System.currentTimeMillis());
		stmt.value("sendTime", System.currentTimeMillis());
		s.executeAsync(stmt);
	}

	public static void badMessage(Exchange ex, Session session) {
		System.out.println("Some message went wrong\nattempting to print message");
		String m = ex.getIn().getBody(String.class);
		System.out.println(m);

	}


}
