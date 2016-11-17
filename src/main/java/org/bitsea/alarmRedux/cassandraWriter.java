package org.bitsea.alarmRedux;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.core.querybuilder.Update.Assignments;

import ca.uhn.hl7v2.HL7Exception;
import io.netty.util.internal.ThreadLocalRandom;

import org.bitsea.alarmRedux.MessageDecoder;


@Component
public class cassandraWriter {
	Cluster cluster;
	Session session;
	HashMap<String, PreparedStatement> psCache = new HashMap<String, PreparedStatement>();
	
	
	public cassandraWriter() {
		
	}
	
	
	private void connectToSession() {
			String ipAddress = System.getProperty("DBIP");
			int port = Integer.parseInt(System.getProperty("DBPORT"));
			String keyspace = System.getProperty("DBNAME");

			final CassandraConnector client = new CassandraConnector();
			client.connect(ipAddress, port, keyspace);
			session = client.getSession();	
			
			PreparedStatement ps_adtChangeCurrent = session.prepare(
					QueryBuilder.update("adt_messages")
					.with(QueryBuilder.set("current", false))
					.and(QueryBuilder.set("receivedTime", QueryBuilder.bindMarker("time")))
					.where(QueryBuilder.eq("PatID", QueryBuilder.bindMarker("p"))));
			PreparedStatement ps_insertNewORU = session.prepare(QueryBuilder.insertInto("oru_messages")
					.value("PatID", QueryBuilder.bindMarker("p"))
					.value("msgCtrlID", QueryBuilder.bindMarker("m"))
					.value("numeric", QueryBuilder.bindMarker("n"))
					.value("textual", QueryBuilder.bindMarker("t"))
					.value("receivedTime", QueryBuilder.bindMarker("time"))
					.value("bedInformation", QueryBuilder.bindMarker("bed"))
					.value("sendTime", QueryBuilder.bindMarker("send"))
					.value("visitNumber", QueryBuilder.bindMarker("visit"))
					.value("abnormal", QueryBuilder.bindMarker("abnormal")));
			PreparedStatement ps_insertAlarm = session.prepare(
					QueryBuilder.insertInto("alarm_information")
					.value("PatID", QueryBuilder.bindMarker("p"))
					.value("msgCtrlID", QueryBuilder.bindMarker("m"))
					.value("reason", QueryBuilder.bindMarker("r"))
					.value("severness_counter", QueryBuilder.bindMarker("s"))
					.value("receivedTime", QueryBuilder.bindMarker("time")));
			psCache.put("adtChangeCurrent", ps_adtChangeCurrent);
			psCache.put("oruInsert", ps_insertNewORU);
			psCache.put("insertAlarm", ps_insertAlarm);
			PreparedStatement ps_getPatientWithID = session.prepare(
					QueryBuilder.select("parameters", "sendTime")
					.from("patient_standard_values").allowFiltering()
					.where(QueryBuilder.eq("PatID", QueryBuilder.bindMarker("id")))
					.and(QueryBuilder.eq("current", true)));
			psCache.put("getPatientValues", ps_getPatientWithID);
			PreparedStatement ps_setOldPSVInvalid = session.prepare(
					QueryBuilder.update("patient_standard_values")
					.with(QueryBuilder.set("current", false))
					.where(QueryBuilder.eq("PatID", QueryBuilder.bindMarker("PatID")))
					.and(QueryBuilder.eq("receivedTime", QueryBuilder.bindMarker("time"))));
			psCache.put("setPSVInvalid", ps_setOldPSVInvalid);
			PreparedStatement ps_updatePSV = session.prepare(
					QueryBuilder.insertInto("patient_standard_values")
					.value("PatID", QueryBuilder.bindMarker("id"))
					.value("parameters", QueryBuilder.bindMarker("para"))
					.value("current", true)
					.value("receivedTime", QueryBuilder.bindMarker("time")));
			psCache.put("updatePatientStandardValues", ps_updatePSV);
	}
	
	
	public void processADT(Exchange exchange) throws Exception {
		if (session == null) {connectToSession();}
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
		
		session.executeAsync(insert);
		
		Insert bedInformation = QueryBuilder.insertInto("patient_bed_station")
				.value("bedInformation", general.get("bedInformation"))
				.value("PatID", pat_id);
		bedInformation.value("station", general.get("stationInformation"));
		bedInformation.value("roomNr", general.get("roomInformation"));
		bedInformation.value("receivedTime", System.currentTimeMillis());
		bedInformation.value("sendTime", timeSent);
		session.executeAsync(bedInformation);
	}
	
	
	public void processA03(Exchange exchange) throws Exception {
		if (session==null) {connectToSession();}
		
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
		
		session.executeAsync(changeADTInfo);
		
		// remove patient from bed and set bed free (i.e., delete the patient)
		Statement deleteBedInfo = QueryBuilder.delete().from("patient_bed_station")
				.where(QueryBuilder.eq("bedInformation", bedInfo))
				.and(QueryBuilder.eq("roomNr", room))
				.and(QueryBuilder.eq("station", station))
				.and(QueryBuilder.eq("PatID", patID));
		session.execute(deleteBedInfo);

		
	}
	
	
	public void processAsAlarm(String MesID, int PatID, MessageDecoder msg_content, int nrOBX) throws HL7Exception {
		List<String> alarmReasons = new LinkedList<String>();
		HashMap<String, Integer> severness = new HashMap<String, Integer>();
		boolean looping = true;
		int i = 0;
		while (looping) {
			try {
				String ending = msg_content.getObsCoding(i);
				if (ending.toLowerCase().contains("alert")) {
					String name = msg_content.getObsText(i);
					String value = msg_content.getObsValueStr(i);
					alarmReasons.add(value);
					severness.put(name, severness.getOrDefault(name, 0) + 1);
				}
				i++;
			} catch (Exception e) {
				looping = false;
			}
		}
		
		BoundStatement bs = new BoundStatement(psCache.get("insertAlarm"));
		bs.setString("m", MesID);
		bs.setList("r", alarmReasons);
		bs.setMap("s", severness);
		bs.setInt("p", PatID);
		bs.setLong("time", System.currentTimeMillis());
		session.executeAsync(bs);
	}

	
	private TupleValue parseFromTo(String msg) {
		int index = msg.lastIndexOf("-");
		float[] elements = {Float.parseFloat(msg.substring(0, index)), Float.parseFloat(msg.substring(index+1))};
		TupleType boundaryType = session.getCluster().getMetadata().newTupleType(DataType.cfloat(), DataType.cfloat());
		TupleValue nx = boundaryType.newValue();
		nx.setFloat(0, elements[0]);
		nx.setFloat(1, elements[1]);
		return nx;
	}
	
	
	private TupleValue parseCmp(String msg) {
		float[] bounds = new float[2];
		if (msg.indexOf("<") == 0 || msg.indexOf(">") == msg.length()) {
			msg = msg.indexOf("<") == 0 ? msg.substring(1) : msg.substring(0, msg.length() - 1);
			bounds[1] = Float.parseFloat(msg);
			bounds[0] = 0;
		} else {
			msg = msg.indexOf(">") == 0 ? msg.substring(1) : msg.substring(0, msg.length() - 1);
			bounds[0] = Float.parseFloat(msg);
			bounds[1] = bounds[0]*10;
		}
		
		TupleType boundaryType = session.getCluster().getMetadata().newTupleType(DataType.cfloat(), DataType.cfloat());
		TupleValue nx = boundaryType.newValue();
		nx.setFloat(0, bounds[0]);
		nx.setFloat(1, bounds[1]);
		return nx;
	}
	
	
	public void processNewBorders(int PatID, Map<String, String> borders) {
		// cases that may occur are : (-)x-y, <y (theoretically >y ?)
		Map<String, TupleValue> mapping = new HashMap<String, TupleValue>();
		for (Entry<String, String> e: borders.entrySet()) {
			String key = e.getKey();
			String value = e.getValue();
			TupleValue tplVl;
			if (value.contains("<") || value.contains(">")) {
				tplVl = parseCmp(value);
			} else {
				// parse (-)x-y message
				tplVl = parseFromTo(value);
			}
			mapping.put(key, tplVl);
		}
		
		BoundStatement insert = new BoundStatement(psCache.get("updatePatientStandardValues"));
		insert.setInt("id", PatID);
		insert.setMap("para", mapping);
		insert.setLong("time", System.currentTimeMillis());
		
		session.executeAsync(insert);
		
	}
	
	
	public void processLimits(int PatID, Map<String, String> trueBorders) throws ClassNotFoundException {

		BoundStatement getOldValues = new BoundStatement(psCache.get("getPatientValues"));
		getOldValues.setInt("id", PatID);
		ResultSet oldValues = session.execute(getOldValues);
		
		Map<String, TupleValue> newMapping = new HashMap<String, TupleValue>();
		boolean changed = false;
		List<Long> oldTimestamps = new LinkedList<Long>();
		if (!oldValues.isExhausted()) {
			for (Row row: oldValues) {
				oldTimestamps.add(row.getLong("sendTime"));
				Map<String, TupleValue> old = row.getMap("parameters", String.class, TupleValue.class);
				for (Entry<String, String> e: trueBorders.entrySet()) {
					TupleValue bounds;
					String key = e.getKey();
					String val = e.getValue();
					if (val.contains("<") || val.contains(">")) {
						bounds = parseCmp(val);
					} else {
						bounds = parseFromTo(val);
					}
					try {
						TupleValue v = (TupleValue) old.get(key);
						float oldLow = v.getFloat(0);
						float oldHigh = v.getFloat(1);
						if (bounds.getFloat(0) != oldLow || bounds.getFloat(1) != oldHigh) {
							newMapping.put(key, bounds);
							changed = true;
						} else {
							newMapping.put(key, v);
						}
					} catch (Exception z) {
						// pass, just means that this key was not in the old map
					}
				}
			}
		} else {
			processNewBorders(PatID, trueBorders);
		}
		
		if (changed){
			// set old invalid
			BoundStatement changeOld = new BoundStatement(psCache.get("setPSVInvalid"));
			changeOld.setInt("PatID", PatID);
			long max = Collections.max(oldTimestamps);
			changeOld.setLong("time", max);
			session.execute(changeOld);

			// and insert new ones
			BoundStatement bs = new BoundStatement(psCache.get("updatePatientStandardValues"));
			bs.setMap("para", newMapping);
			bs.setInt("id", PatID);
			bs.setLong("time", System.currentTimeMillis());
			session.executeAsync(bs);
		}
	}
	
	
	public void process(Exchange exchange) throws Exception {
		if (session == null) { connectToSession();}
		MessageDecoder mdecode = new MessageDecoder(exchange);
		
		HashMap<String, String> general = mdecode.generalInformation();
		int visitNumber = mdecode.getVisitNumber();

		String pID = mdecode.getPatientID();
		
		if (pID.isEmpty()) {
			// get it from his bedID if possible
			Statement bedSelect = QueryBuilder.select("PatID").from("patient_bed_station").where(QueryBuilder.eq("bedInformation", general.get("bedInformation")))
					.and(QueryBuilder.eq("station", general.get("stationInformation"))).and(QueryBuilder.eq("roomNr", general.get("roomInformation")));
			ResultSet rs = session.execute(bedSelect);
			Row r = rs.one();
			if (r != null) {
				pID = Integer.toString(rs.one().getInt("PatID"));
			} else {
				Statement selectIDs = QueryBuilder.select("patid").from("patient_bed_station");
				rs = session.execute(selectIDs);
				List<Integer> entries = new ArrayList<Integer>();
				for (Row jrow: rs) {
					entries.add(jrow.getInt("patid"));
				}
				int tmp = ThreadLocalRandom.current().nextInt(1000, 10000);
				while (entries.contains(tmp)) {
					tmp = ThreadLocalRandom.current().nextInt(1000, 10000);
				}
				
				Statement bedInsert = QueryBuilder.insertInto("patient_bed_station")
						.value("bedInformation", general.get("bedInformation"))
						.value("roomNr", general.get("roomInformation"))
						.value("station", general.get("stationInformation"))
						.value("time",  mdecode.getOBRTime())
						.value("patid", tmp);
				session.executeAsync(bedInsert);
			}
		}
		
		int pID2 = Integer.parseInt(pID);
		boolean alarmCheck = false;
		
		HashMap<String, TupleValue> numerics = new HashMap<String, TupleValue>();
		HashMap<String, String> textual = new HashMap<String, String>();
		HashMap<String, String> borders = new HashMap<String, String>();
		HashMap<String, String> abnormalMap = new HashMap<String, String>();
		
		TupleType tt = session.getCluster().getMetadata().newTupleType(DataType.cfloat(), DataType.text()); 

		boolean looping = true;
		int i = 0;
		BoundStatement insert = new BoundStatement(psCache.get("oruInsert"));
		while (looping) {
			try {
				String name = mdecode.getObsText(i);
				String ending = mdecode.getObsCoding(i);
				if (ending.toLowerCase().contains("alert")) {
					alarmCheck = true;
				}
				String abnormalMarker = mdecode.getAbnormal(i);
				abnormalMap.put(name, abnormalMarker);
				String linetype = mdecode.getCoding(i);
				if (linetype.equals("NM")) {
					Float value = mdecode.getObsValueFlt(i);
					String unit = mdecode.getObsUnit(i);
					TupleValue tp = tt.newValue(value, unit);
					
					numerics.put(name, tp);
					String borderString = mdecode.getBorderString(i);
					if (!borderString.isEmpty()) {
						borders.put(name, borderString);
					}
				} else if (linetype.equals("ST") || linetype.equals("TX")) {
					String value = mdecode.getObsValueStr(i);
					if (value == null) {
						value = "";
					}
					textual.put(name, value);
				}
				i++;
			} catch (Exception e) {
				looping = false;
		
			}
		}
		
		if (borders.size() > 0) {
			processLimits(pID2, borders);
		}
		
		
		insert.setInt("p", pID2);
		insert.setString("m", general.get("msgctrlid"));
		insert.setMap("n", numerics);
		insert.setMap("t", textual);
		insert.setMap("abnormal", abnormalMap);
		insert.setInt("visit", visitNumber);
		insert.setLong("time", System.currentTimeMillis());
		insert.setLong("send", mdecode.getOBRTime());
		insert.setString("bed", general.get("stationInformation") + "^" + general.get("roomInformation") + "^" + general.get("bedInformation"));
		
 		session.executeAsync(insert);
		
		// check whether person has a bed? or even more simple, make an upsert
 		
 		Statement patInfo = QueryBuilder.insertInto("patient_bed_station")
 				.value("patid", pID2)
 				.value("bedInformation", general.get("bedInformation"))
 				.value("roomNr", general.get("roomInformation"))
 				.value("station", general.get("stationInformation"))
 				.value("time", mdecode.getOBRTime());
 		
 		session.executeAsync(patInfo);

 		if (alarmCheck) {
			processAsAlarm(general.get("msgctrlid"), pID2, mdecode, i);
		}
	}
}
