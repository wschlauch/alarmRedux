package org.bitsea.alarmRedux;

import java.text.ParseException;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import ca.uhn.hl7v2.HL7Exception;
import io.netty.handler.timeout.WriteTimeoutException;

public class DBWriter {

	static Session session;
	
	public DBWriter(Session ses) {
		this.session = ses;
	}
	
	
	/*
	 * attempt to gather information whether this entry already exists
	 * to do this, check whether message has been sent at same time
	 * and if msgctrlid exists already
	 */
	private static boolean testIfExists(MessageDecoder mdc) throws HL7Exception, NullPointerException, ParseException {
		String msgctrlid = mdc.getMsgId();
		long sendtime = mdc.getOBRTime();
		boolean result = false;
		Statement srx = QueryBuilder.select().countAll().from("oru_messages").allowFiltering()
				.where(QueryBuilder.eq("msgctrlid", msgctrlid)).and(QueryBuilder.eq("sendtime", sendtime));
		ResultSetFuture rs = session.executeAsync(srx);
		if (rs.getUninterruptibly().one().getLong(0) > 0) {
			result = true;
		}
		return result;
	}
	
	
	public static boolean processStatement(Statement stmt, MessageDecoder mc) throws HL7Exception, NullPointerException, ParseException {
		boolean done = false;
		try {
			if (!testIfExists(mc)) {
				session.executeAsync(stmt);
				done = true;
			}
		} catch (QueryValidationException e) {
			CassandraExceptionHandler.QVExceptionHandling(e.toString(), mc, session);
		} catch (WriteTimeoutException e) {
			CassandraExceptionHandler.WTExceptionHandling(e.toString(), mc, session);
		}
		return done;
	}

}
