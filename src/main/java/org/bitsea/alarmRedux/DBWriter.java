package org.bitsea.alarmRedux;

import java.text.ParseException;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.QueryValidationException;

import ca.uhn.hl7v2.HL7Exception;
import io.netty.handler.timeout.WriteTimeoutException;

public class DBWriter {

	static Session session;
	
	public DBWriter(Session ses) {
		this.session = ses;
	}
	
	public static void processStatement(Statement stmt, MessageDecoder mc) throws HL7Exception, NullPointerException, ParseException {
		try {
			session.executeAsync(stmt);
		} catch (QueryValidationException e) {
			CassandraExceptionHandler.QVExceptionHandling(e.toString(), mc, session);
		} catch (WriteTimeoutException e) {
			CassandraExceptionHandler.WTExceptionHandling(e.toString(), mc, session);
		}
	}

}
