package org.bitsea.alarmRedux;

import java.util.HashMap;

import org.apache.camel.Exchange;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import io.netty.util.internal.ThreadLocalRandom;

public class ADTProcessor {

	Session session;
	HashMap<String, PreparedStatement> psCache = new HashMap<String, PreparedStatement>();
	
	public ADTProcessor(Session se) {
		this.session = se;
		
		PreparedStatement ps_adtChangeCurrent = session.prepare(
				QueryBuilder.update("adt_messages")
				.with(QueryBuilder.set("current", false))
				.and(QueryBuilder.set("receivedTime", QueryBuilder.bindMarker("time")))
				.where(QueryBuilder.eq("PatID", QueryBuilder.bindMarker("p"))));
		psCache.put("adtChangeCurrent", ps_adtChangeCurrent);
		



	}
	
	public void processADT(Exchange exchange) throws Exception {
		MessageDecoder mdecode = new MessageDecoder(exchange);
		
		Insert insert = QueryBuilder.insertInto("adt_messages");
		HashMap<String,String> general = mdecode.generalInformation();
		
		String patid = mdecode.getPatientID();
		int pat_id;
		if (patid.isEmpty()) {
			pat_id = ThreadLocalRandom.current().nextInt(1000, 10000);
		} else {
			pat_id = Integer.parseInt(patid);
		}
		
		long timeSent = mdecode.transformMSHTime();

		String dob = mdecode.getDateOfBirth();
		boolean current = true;
		String srbInfo = general.get("stationInformation") + "^" + general.get("roomInformation") + "^" +general.get("bedInformation");
		insert.value("PatID", pat_id).value("msgCtrlID", general.get("msgctrlid"))
		.value("dob", dob).value("current", current)
		.value("bedLocation",  srbInfo);
		insert.value("receivedTime", System.currentTimeMillis());
		insert.value("sendTime", timeSent);
		DBWriter.processStatement(insert, mdecode);
		//session.executeAsync(insert);
		
		Insert bedInformation = QueryBuilder.insertInto("patient_bed_station")
				.value("bedInformation", general.get("bedInformation"))
				.value("PatID", pat_id);
		bedInformation.value("station", general.get("stationInformation"));
		bedInformation.value("roomNr", general.get("roomInformation"));
		bedInformation.value("receivedTime", System.currentTimeMillis());
		bedInformation.value("sendTime", timeSent);
		//session.executeAsync(bedInformation);
		DBWriter.processStatement(bedInformation, mdecode);
		
		// check whether station has already an ID - if not, insert a new id to the DB
		// to have no problem later on when querying the DB
		// currently using
		Insert stmt = QueryBuilder.insertInto("station_name").ifNotExists()
				.value("stationName", general.get("stationInformation"))
				.value("stationID", ThreadLocalRandom.current().nextInt(1, 999999));
		session.execute(stmt);
	}
	
	
	public void processA03(Exchange exchange) throws Exception {
		MessageDecoder mdecode = new MessageDecoder(exchange);
		
		String pid = mdecode.getPatientID();
		String bedInfo = mdecode.getBed();
		String station = mdecode.getStation();
		String room = mdecode.getRoom();
		int patID;
		try {
			patID = Integer.parseInt(pid);
		} catch (NumberFormatException e) {
			Statement stmt = QueryBuilder.select("PatID").from("patient_bed_station").allowFiltering()
					.where(QueryBuilder.eq("bedInformation", bedInfo))
					.and(QueryBuilder.eq("roomNr", room))
					.and(QueryBuilder.eq("station", station));
			ResultSet rs = session.execute(stmt);
			patID = rs.one().getInt("PatID");
			
		}
		
		// get old adt information and make invalid
		BoundStatement changeADTInfo = new BoundStatement(psCache.get("adtChangeCurrent"));
		changeADTInfo.setInt("p", patID);
		changeADTInfo.setLong("time", mdecode.transformMSHTime());
		
		//session.executeAsync(changeADTInfo);
		DBWriter.processStatement(changeADTInfo, mdecode);
		
		// remove patient from bed and set bed free (i.e., delete the patient)
		Statement deleteBedInfo = QueryBuilder.delete().from("patient_bed_station")
				.where(QueryBuilder.eq("bedInformation", bedInfo))
				.and(QueryBuilder.eq("roomNr", room))
				.and(QueryBuilder.eq("station", station))
				.and(QueryBuilder.eq("PatID", patID));
		//session.execute(deleteBedInfo);
		DBWriter.processStatement(deleteBedInfo, mdecode);
		
	}
}
