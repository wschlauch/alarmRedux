package org.bitsea.alarmRedux;

import java.util.Date;
import java.util.HashMap;
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


import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.util.Terser;

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
			PreparedStatement ps_adtChangeCurrent = session.prepare(QueryBuilder.update("adt_messages").with(QueryBuilder.set("current", false)).and(QueryBuilder.set("tstamp", QueryBuilder.bindMarker("time"))).where(QueryBuilder.eq("PatID", QueryBuilder.bindMarker("p"))));
			PreparedStatement ps_insertNewORU = session.prepare(QueryBuilder.insertInto("oru_messages").value("PatID", QueryBuilder.bindMarker("p")).value("msgCtrlID", QueryBuilder.bindMarker("m")).value("numeric", QueryBuilder.bindMarker("n")).value("textual", QueryBuilder.bindMarker("t")).value("tstamp", QueryBuilder.bindMarker("time")));
			PreparedStatement ps_insertAlarm = session.prepare(QueryBuilder.insertInto("alarm_information").value("PatID", QueryBuilder.bindMarker("p")).value("msgCtrlID", QueryBuilder.bindMarker("m")).value("reason", QueryBuilder.bindMarker("r")).value("severness_counter", QueryBuilder.bindMarker("s")).value("tstamp", QueryBuilder.bindMarker("time")));
			psCache.put("adtChangeCurrent", ps_adtChangeCurrent);
			psCache.put("oruInsert", ps_insertNewORU);
			psCache.put("insertAlarm", ps_insertAlarm);
			PreparedStatement ps_getPatientWithID = session.prepare(QueryBuilder.select("parameters").from("patient_standard_values").allowFiltering().where(QueryBuilder.eq("PatID", QueryBuilder.bindMarker("id"))).and(QueryBuilder.eq("current", true)));
			psCache.put("getPatientValues", ps_getPatientWithID);
			PreparedStatement ps_setOldPSVInvalid = session.prepare(QueryBuilder.update("patient_standard_values").with(QueryBuilder.set("current", false)).where(QueryBuilder.eq("PatID", QueryBuilder.bindMarker("PatID"))));
			psCache.put("setPSVInvalid", ps_setOldPSVInvalid);
			PreparedStatement ps_updatePSV = session.prepare(QueryBuilder.insertInto("patient_standard_values").value("PatID", QueryBuilder.bindMarker("id")).value("parameters", QueryBuilder.bindMarker("para")).value("current", true).value("tstamp", QueryBuilder.bindMarker("time")));
			psCache.put("updatePatientStandardValues", ps_updatePSV);
	}
	
	
	public void processADT(Exchange exchange) throws Exception {
		if (session == null) {connectToSession();}
		
		Message msg = exchange.getIn().getBody(Message.class);
		Terser terse = new Terser(msg);
		
		Insert insert = QueryBuilder.insertInto("adt_messages");
		
		String mId = terse.get("/.MSH-10");
		String PatID = terse.get("/.PID-3-1");
		String bedNR = terse.get("/.PV1-3");
		if (PatID == null) {
			Statement bedPatientId = QueryBuilder.select("PatID").from("patient_bed").where(QueryBuilder.eq("bed_information", bedNR));
			PatID = session.execute(bedPatientId).one().getString("PatID");
		}
		String pointOfCare = bedNR.split("^")[0];
		int PatID2 = Integer.parseInt(PatID);
		String dob = terse.get("/.PID-7");
		boolean current = true;
		
		insert.value("PatID", PatID2).value("msgCtrlID", mId).value("dob", dob).value("current", current).value("bedLocation", bedNR);
		insert.value("tstamp", new Date().getTime());
		session.executeAsync(insert);
		
		Insert bedInformation = QueryBuilder.insertInto("patient_bed_station").value("bedInformation", bedNR).value("PatID", PatID2);
		bedInformation.value("station", pointOfCare);
		session.executeAsync(bedInformation);
	}
	
	
	public void processA03(Exchange exchange) throws Exception {
		if (session==null) {connectToSession();}
		
		Message msg = exchange.getIn().getBody(Message.class);
		Terser terse = new Terser(msg);
		
		String pid = terse.get("/.PID-3-1");
		String bedInfo = terse.get("/.PV1-3");
		
		int patID = Integer.parseInt(pid);
		// get old adt information and make invalid
		BoundStatement changeADTInfo = new BoundStatement(psCache.get("adtChangeCurrent"));
		changeADTInfo.setInt("p", patID);
		changeADTInfo.setString("time", ""+System.currentTimeMillis());
		session.executeAsync(changeADTInfo);
		
		// remove patient from bed and set bed free (i.e., delete the patient)
		// TODO: decide whether to delete or only mark as not current s.t. later less to enter
		Statement deleteBedInfo = QueryBuilder.delete().from("patient_bed").where(QueryBuilder.eq("bedInformation", bedInfo)).and(QueryBuilder.eq("PatID", patID));
		session.execute(deleteBedInfo);

		
	}
	
	
	public void processAsAlarm(String MesID, int PatID, Message msg_content, int nrOBX) throws HL7Exception {
		HashMap<String, String> alarmReasons = new HashMap<String, String>();
		HashMap<String, Integer> severness = new HashMap<String, Integer>();
		
		Terser terse = new Terser(msg_content);
				
		boolean looping = true;
		int i = 0;
		while (looping) {
			try {
				String ending = terse.get("/.OBSERVATION(" + i + ")/OBX-3-3");
				if (ending.toLowerCase().contains("alert")) {
					String name = terse.get("/.OBSERVATION(" + i + ")/OBX-3-2");
					String value = terse.get("/.OBSERVATION(" + i + ")/OBX-5");
					alarmReasons.put(name, value);
					severness.put(name, severness.getOrDefault(name, 0) + 1);
				}
				i++;
			} catch (Exception e) {
				looping = false;
			}
		}
		
		BoundStatement bs = new BoundStatement(psCache.get("insertAlarm"));
		bs.setString("m", MesID);
		bs.setMap("r", alarmReasons);
		bs.setMap("s", severness);
		bs.setInt("p", PatID);
		bs.setString("time", ""+System.currentTimeMillis());
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
		insert.setString("time", ""+System.currentTimeMillis());
		
		session.executeAsync(insert);
		
	}
	
	
	public void processLimits(int PatID, Map<String, String> trueBorders) throws ClassNotFoundException {

		BoundStatement getOldValues = new BoundStatement(psCache.get("getPatientValues"));
		getOldValues.setInt("id", PatID);
		ResultSet oldValues = session.execute(getOldValues);
		
		Map<String, TupleValue> newMapping = new HashMap<String, TupleValue>();
		boolean changed = false;
		
//		TupleType boundaryType = session.getCluster().getMetadata().newTupleType(DataType.cfloat(), DataType.cfloat());
		if (!oldValues.isExhausted()) {
			for (Row row: oldValues) {
				Map<?, ?> old = row.getMap("parameters", String.class, TupleValue.class);
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
			BoundStatement changeOld = new BoundStatement(psCache.get("setPSVInvalid"));
			changeOld.setInt("PatID", PatID);
			session.execute(changeOld);
		
			BoundStatement bs = new BoundStatement(psCache.get("updatePatientStandardValues"));
			bs.setMap("para", newMapping);
			bs.setInt("id", PatID);
			bs.setString("tstamp", ""+System.currentTimeMillis());
			session.executeAsync(bs);
		}
	}
	
	
	public void process(Exchange exchange) throws Exception {
		if (session == null) { connectToSession();}

		Message msg_content = exchange.getIn().getBody(Message.class);
		Terser terse = new Terser(msg_content);
		
		String idx = terse.get("/.MSH-10");  
		
		String pID = terse.get("/.PID-3-1");
		if (pID == null) {
			// get it from his bedID if possible
			String bedInfo = terse.get("/.PV1-3");
			Statement bedSelect = QueryBuilder.select("PatID").from("patient_bed").where(QueryBuilder.eq("bedInformation", bedInfo));
			ResultSet rs = session.execute(bedSelect);
			pID = Integer.toString(rs.one().getInt("PatID"));
			
		}
		int pID2 = Integer.parseInt(pID);
		
		boolean alarmCheck = false;
		
		HashMap<String, Float> numerics = new HashMap<String, Float>();
		HashMap<String, String> textual = new HashMap<String, String>();
		HashMap<String, String> borders = new HashMap<String, String>();
		
		boolean looping = true;
		int i = 0;
		while (looping) {
			try {
				String name = terse.get("/.OBSERVATION(" + i + ")/OBX-3-2");
				String ending = terse.get("/.OBSERVATION(" + i + ")/OBX-3-3");
				if (ending.toLowerCase().contains("alert")) {
					alarmCheck = true;
				}
				String linetype = terse.get("/.OBSERVATION(" + i + ")/OBX-2");
				if (linetype.equals("NM")) {
					Float value = Float.parseFloat(terse.get("/.OBSERVATION(" + i + ")/OBX-5-1"));
					numerics.put(name, value);
					String borderString = terse.get("/.OBSERVATION(" + i + ")/OBX-7");
					if (!borderString.isEmpty()) {
						borders.put(name, borderString);
					}
				} else if (linetype.equals("ST") || linetype.equals("TX")) {
					String value = terse.get("/.OBSERVATION(" + i + ")/OBX-5");
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
		
		BoundStatement insert = new BoundStatement(psCache.get("oruInsert"));
		
		insert.setInt("p", pID2);
		insert.setString("m", idx);
		insert.setMap("n", numerics);
		insert.setMap("t", textual);
		insert.setString("time", ""+System.currentTimeMillis());
		
 		session.executeAsync(insert);
 		
		if (alarmCheck) {
			processAsAlarm(idx, pID2, msg_content, i);
		}
	}
}
