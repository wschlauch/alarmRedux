package org.bitsea.alarmRedux;

import java.util.ArrayList;
import java.util.HashMap;
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

import io.netty.util.internal.ThreadLocalRandom;

import org.bitsea.alarmRedux.MessageDecoder;


@Component
public class cassandraWriter {
	Cluster cluster;
	Session session;
	HashMap<String, PreparedStatement> psCache = new HashMap<String, PreparedStatement>();
	
	
	//public cassandraWriter() {}
	
	public cassandraWriter(Session sess) {
		this.session = sess;
		PreparedStatement ps_insertNewORU = session.prepare(QueryBuilder.insertInto("oru_messages")
					.value("PatID", QueryBuilder.bindMarker("p"))
					.value("uid", QueryBuilder.uuid())
					.value("msgCtrlID", QueryBuilder.bindMarker("m"))
					.value("numeric", QueryBuilder.bindMarker("n"))
					.value("textual", QueryBuilder.bindMarker("t"))
					.value("receivedTime", QueryBuilder.bindMarker("time"))
					.value("bedInformation", QueryBuilder.bindMarker("bed"))
					.value("sendTime", QueryBuilder.bindMarker("send"))
					.value("visitNumber", QueryBuilder.bindMarker("visit"))
					.value("abnormal", QueryBuilder.bindMarker("abnormal")));
		psCache.put("oruInsert", ps_insertNewORU);
			
		PreparedStatement ps_getPatientWithID = session.prepare(
					QueryBuilder.select("parameters", "sendTime")
					.from("patient_standard_values").allowFiltering()
					.where(QueryBuilder.eq("PatID", QueryBuilder.bindMarker("id")))
					.and(QueryBuilder.eq("current", true)));
		psCache.put("getPatientValues", ps_getPatientWithID);

		PreparedStatement ps_updatePSV = session.prepare(
				QueryBuilder.insertInto("patient_standard_values")
				.value("PatID", QueryBuilder.bindMarker("id"))
				.value("parameters", QueryBuilder.bindMarker("para"))
				.value("current", true)
				.value("receivedTime", QueryBuilder.bindMarker("time"))
				.value("sendTime", QueryBuilder.bindMarker("sending")));
		psCache.put("updatePatientStandardValues", ps_updatePSV);
		
		PreparedStatement ps_setOldPSVInvalid = session.prepare(
				QueryBuilder.update("patient_standard_values")
				.with(QueryBuilder.set("current", false))
				.where(QueryBuilder.eq("PatID", QueryBuilder.bindMarker("PatID")))
				.and(QueryBuilder.eq("sendTime", QueryBuilder.bindMarker("ttime"))));
		psCache.put("setPSVInvalid", ps_setOldPSVInvalid);

	}
	
	

	
	
//	public void processAsAlarm(String MesID, int PatID, MessageDecoder msg_content, int nrOBX) throws HL7Exception, ParseException {
//		List<String> alarmReasons = new LinkedList<String>();
//		HashMap<String, Integer> severness = new HashMap<String, Integer>();
//		boolean looping = true;
//		int i = 0;
//		while (looping) {
//			try {
//				String ending = msg_content.getObsCoding(i);
//				if (ending.toLowerCase().contains("alert")) {
//					String name = msg_content.getObsText(i);
//					String value = msg_content.getObsValueStr(i);
//					alarmReasons.add(value);
//					severness.put(name, severness.getOrDefault(name, 0) + 1);
//				}
//				i++;
//			} catch (Exception e) {
//				looping = false;
//			}
//		}
//		
//		BoundStatement bs = new BoundStatement(psCache.get("insertAlarm"));
//		bs.setString("m", MesID);
//		bs.setList("r", alarmReasons);
//		bs.setMap("s", severness);
//		bs.setInt("p", PatID);
//		bs.setLong("sendAt", msg_content.getOBRTime());
//		bs.setLong("time", System.currentTimeMillis());
//		//session.executeAsync(bs);
//		DBWriter.processStatement(bs, msg_content);
//	}

/*	public void processAsAlarmNew(String MesID, int PatID, MessageDecoder msg_content, int nrOBX) throws HL7Exception, ParseException {
		boolean looping = true;
		int i = 0;
		while (looping) {
			try {
				String ending = msg_content.getObsCoding(i);
				if (ending.toLowerCase().contains("alert")) {
					String name = msg_content.getObsText(i);
					String value = msg_content.getObsValueStr(i);


				}
				i++;
			} catch (Exception e) {
				looping = false;
			}
		}
		
		//session.executeAsync(bs);
		BoundStatement bs = new BoundStatement(psCache.get("insertAlarm"));
		bs.setString("m", MesID);
		
		bs.setInt("p", PatID);
		bs.setLong("sendAt", msg_content.getOBRTime());
		bs.setLong("time", System.currentTimeMillis());
		DBWriter.processStatement(bs, msg_content);
	}
	*/
	
	private TupleValue parseFromTo(String msg) {
		// the last index of "-" is where the message (-)a-b is going to be split
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
		// if message say "<X" or "X>" it implies that the measured value is supposed
		// to be smaller than X; the other boundary may be 0
		if (msg.indexOf("<") == 0 || msg.indexOf(">") == msg.length()) {
			// if '<' is first sign, take everything from the second sign, otherwise everything but the last
			msg = msg.indexOf("<") == 0 ? msg.substring(1) : msg.substring(0, msg.length() - 1);
			bounds[1] = Float.parseFloat(msg);
			bounds[0] = 0;
		} else {
			// message might be ">X" or "X<", i.e. X be the lower boundary
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
	
	
	public void processNewBorders(int PatID, Map<String, String> borders, long sendTime) {
		// cases that may occur are : (-)x-y, <y (theoretically >y ?)
		// but first, get the old params such that we can overwrite/add the new params
		// but keep the old
		Statement stmt = QueryBuilder.select("parameters").from("patient_standard_values")
				.where(QueryBuilder.eq("patid", PatID)).and(QueryBuilder.lt("sendTime", System.currentTimeMillis()));
		ResultSet rs = session.execute(stmt);
		Map<String, TupleValue> mapping;
		if (!rs.isExhausted()) {
			Row row = rs.one();
			mapping = row.getMap("parameters", String.class, TupleValue.class);
		} else {
			mapping = new HashMap<String, TupleValue>();
		}
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
		insert.setLong("sending", sendTime);
		
		session.executeAsync(insert);
		
	}
	
	
	public void processLimits(int PatID, Map<String, String> trueBorders, long time) throws ClassNotFoundException {
		// take the formerly current patient standard values
		BoundStatement getOldValues = new BoundStatement(psCache.get("getPatientValues"));
		getOldValues.setInt("id", PatID);
		ResultSet oldValues = session.execute(getOldValues);
		
		// build a new mapping
		Map<String, TupleValue> newMapping = new HashMap<String, TupleValue>();
		
		// if nothing changes, update can be dismissed
		boolean changed = false;
		long oldTimestamp = System.currentTimeMillis();
		
		// got one entry that the new values are compared against
		if (!oldValues.isExhausted()) {	
			Row row = oldValues.one();
			// at this point in time we got the message
			oldTimestamp = row.getLong("sendTime");
			// get the associated data
			Map<String, TupleValue> oldData = row.getMap("parameters", String.class, TupleValue.class);
		
			// for each new datafield
			for (Entry<String, String> e: trueBorders.entrySet()) {
				TupleValue bounds;
				String key = e.getKey();
				String val = e.getValue();
				// parse the text that contains the boundaries
				if (val.contains("<") || val.contains(">")) {
					bounds = parseCmp(val);
				} else {
					bounds = parseFromTo(val);
				}
				if (oldData.containsKey(key) && !changed) {
					TupleValue v = oldData.get(key);
					float oldLow = v.getFloat(0);
					float oldHigh = v.getFloat(1);
					if (bounds.getFloat(0) != oldLow || bounds.getFloat(1) != oldHigh) {
						changed = true;
					}
				} 
					
				newMapping.put(key, bounds);

			}
			// fill newMapping with values from oldData that are not already updated
			for (String key : oldData.keySet()) {
				if (!newMapping.containsKey(key)) {
					newMapping.put(key, oldData.get(key));
				}
			}
			if (newMapping.size() != oldData.size()) {
				changed = true;
			}

		} else {
			processNewBorders(PatID, trueBorders, time);
		}
		
				
		if (changed){
			// set old invalid
			BoundStatement changeOld = new BoundStatement(psCache.get("setPSVInvalid"));
			changeOld.setInt("PatID", PatID);
			changeOld.setLong("ttime", oldTimestamp);
			session.execute(changeOld);

			// and insert new ones
			BoundStatement bs = new BoundStatement(psCache.get("updatePatientStandardValues"));
			bs.setMap("para", newMapping);
			bs.setInt("id", PatID);
			bs.setLong("time", System.currentTimeMillis());
			bs.setLong("sending", time);
			session.executeAsync(bs);
		}
	}
	
	
	public void process(Exchange exchange) throws Exception {
//		if (session == null) { connectToSession();}
		MessageDecoder mdecode = new MessageDecoder(exchange);
		HashMap<String, String> general = mdecode.generalInformation();

		int visitNumber = mdecode.getVisitNumber();

		String pID = mdecode.getPatientID();
		int pID2;

		try {
			pID2 = Integer.parseInt(pID);
		} catch (NumberFormatException e) {
			// get it from his bedID if possible
			Statement bedSelect = QueryBuilder.select("PatID").from("patient_bed_station").allowFiltering()
					.where(QueryBuilder.eq("bedInformation", general.get("bedInformation")))
					.and(QueryBuilder.eq("station", general.get("stationInformation")));
			ResultSet rs = session.execute(bedSelect);
			Row r = rs.one();
			if (r != null) {
				int tmp = r.getInt("PatID");
				pID = ""+tmp;
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

				pID = ""+tmp;
				Statement bedInsert = QueryBuilder.insertInto("patient_bed_station")
						.value("bedInformation", general.get("bedInformation"))
						.value("roomNr", general.get("roomInformation"))
						.value("station", general.get("stationInformation"))
						.value("time",  mdecode.getOBRTime())
						.value("patid", tmp);
				session.executeAsync(bedInsert);
		
				Insert stmt = QueryBuilder.insertInto("station_name").ifNotExists()
						.value("stationName", general.get("stationInformation"))
						.value("stationID", ThreadLocalRandom.current().nextInt(1, 999999));
				session.execute(stmt);
		
			}
		}

		pID2 = Integer.parseInt(pID);
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

					if (!borderString.isEmpty() && borderString != null ) {
						borders.put(name, borderString);
					} else {
						borders.put(name, "-1000-1000");
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
			processLimits(pID2, borders, mdecode.getOBRTime());
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
		
		DBWriter.processStatement(insert, mdecode);
		// check whether person has a bed? or even more simple, make an upsert
 		
 		Statement patInfo = QueryBuilder.insertInto("patient_bed_station")
 				.value("patid", pID2)
 				.value("bedInformation", general.get("bedInformation"))
 				.value("roomNr", general.get("roomInformation"))
 				.value("station", general.get("stationInformation"))
 				.value("time", mdecode.getOBRTime());
 		
 		DBWriter.processStatement(patInfo, mdecode);

 		if (alarmCheck) {
			AlarmMessage.processAsAlarm(general.get("msgctrlid"), pID2, mdecode, i);
		}
	}


	public void patchThrough(Exchange exchange) throws Exception {
		CassandraExceptionHandler.TExceptionHandling("Bad message",
				exchange, session);
		exchange.getIn().setBody(null);
		exchange.getOut().setBody(null);
	}
 	
	public void withoutHeader(Exchange exchange) throws Exception {
		CassandraExceptionHandler.HExceptionHandling("No MSH", exchange, session);
		exchange.getIn().setBody(null);
		exchange.getOut().setBody(null);
	}
	
	public void weirdException(Exchange ex) throws Exception {
		CassandraExceptionHandler.weirdException(ex, session);
	}
	
	public void notDecodedMessage(Exchange ex) throws Exception {
		CassandraExceptionHandler.badMessage(ex, session);
	}
	
}
