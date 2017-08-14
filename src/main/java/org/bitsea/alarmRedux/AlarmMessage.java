package org.bitsea.alarmRedux;

import java.text.ParseException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;

import ca.uhn.hl7v2.HL7Exception;

public class AlarmMessage {

	static Session session;
	static HashMap<String, PreparedStatement> psCache = new HashMap<String, PreparedStatement>();

	public AlarmMessage(Session q) {
		this.session = q;
		
		PreparedStatement ps_insertAlarm = session.prepare(QueryBuilder.insertInto("alarm_messages")
				.value("PatID", QueryBuilder.bindMarker("PatID")) 
				.value("msgCtrlID", QueryBuilder.bindMarker("MessageID")) 
				.value("receivedTime", QueryBuilder.bindMarker("time")) 
				.value("sendTime", QueryBuilder.bindMarker("sendAt"))
				.value("uid", QueryBuilder.bindMarker("uuid"))
				.value("deviceid", null) 
				.value("devicetype", null)
				.value("ap_sf_code", QueryBuilder.bindMarker("apsfc")) // check
				.value("ap_sf_label", QueryBuilder.bindMarker("apsfl")) // check
				.value("ap_ar_code", null) 
				.value("ap_ar_label", null)
				.value("ap_threshold_direction", QueryBuilder.bindMarker("thresh"))
				.value("ap_threshold_value", QueryBuilder.bindMarker("threshVal"))
				.value("ap_threshold_unit", QueryBuilder.bindMarker("threshUnit")) 
				.value("ap_causing_parameter_value", QueryBuilder.bindMarker("causingValue")) 
				.value("ap_causing_parameter_unit", QueryBuilder.bindMarker("causingUnit"))
				.value("ap_sf_level", QueryBuilder.bindMarker("levelID"))
				.value("ap_sf_alarm_code", QueryBuilder.bindMarker("alertMsg"))
				);
		psCache.put("insertAlarm", ps_insertAlarm);
		
	}

	/*
	 * purpose: each line that contains an alarm should be extracted and compiled to an alarm entry
	 * for the database. By looping through the OBX parts every line can be checked whether
	 * it contains an alarm and if so, it can be transformed to a message
	 */
	public static void processAsAlarm(String MesID, int PatID, MessageDecoder msg_content, int nrOBX) throws HL7Exception, ParseException {
				
		// Pattern to get first word different from special signs and numbers
		Pattern keywordPattern = Pattern.compile("\\*{0,3}\\s*(\\w*?)\\s+[-]{0,1}\\s*\\d+\\s*[<|>]\\s*\\d+");
		// Pattern to discard special signs and message, only focus on comparison that usually occurs at the end
		Pattern borderBreakPattern = Pattern.compile("\\*{0,3}\\s*\\w*?\\s+(\\d+\\s*[<|>]\\s*\\d+)");

		for(int i=0; i<nrOBX; i++) {
			BoundStatement bs = new BoundStatement(psCache.get("insertAlarm"));
			bs.setInt("PatID", PatID);
			bs.setString("MessageID", MesID);

			String ending = msg_content.getObsCoding(i);
			String obx5 = msg_content.getObsValueStr(i);
			// get the coding and the text of the actual message part
			// even though this only contains the alert level and alert itself

			String lvl = msg_content.getObsIdentifier(i);
			String alert = msg_content.getObsText(i);

			bs.setString("alertMsg", alert);
			bs.setString("levelID", lvl);

			bs.setLong("time", System.currentTimeMillis());
			boolean alertContained = ending.toLowerCase().contains("alert");
			long timeSend = msg_content.getOBRTime();
			bs.setLong("sendAt", timeSend);

			if (alertContained && (obx5.contains("<") || obx5.contains(">"))) {
				
				String tmpOBX = "";
				boolean found = false;
				Matcher keyword = keywordPattern.matcher(obx5);
				Matcher boundaries = borderBreakPattern.matcher(obx5);
		
				while (keyword.find()) {
					String matchedPhrase = keyword.group(1);
		
					for (int j=0;j<nrOBX && !found;j++) {
						// go through all obx parts and look for keyword
						tmpOBX = msg_content.getObsText(j);
		
						if (tmpOBX.contains(matchedPhrase)) {
							found = true;
							String borderString = msg_content.getBorderString(j); 
							float value = msg_content.getObsValueFlt(j); // get currently observed value
		
							float[] borders = parseBorders(borderString);

							bs.setUUID("uuid", UUID.randomUUID());
							bs.setFloat("causingValue", value);

							String unit = msg_content.getObsUnit(j);
							
							bs.setString("causingUnit", unit);
							bs.setString("threshUnit", unit);

							// get the alarm_parameter_sf_code/label
							String identifier = msg_content.getObsIdentifier(j);
							String label = msg_content.getObsText(j);
							
							bs.setString("apsfc", identifier);
							bs.setString("apsfl", label);
							if (value < borders[0]) {
								bs.setFloat("threshVal", borders[0]);
								bs.setString("thresh", "low");
							} else {
								bs.setFloat("threshVal", borders[1]);
								bs.setString("thresh", "high");
							}
							session.execute(bs);
						} 
					}
					if (!found) {
						// this is for cases that are alerted at different points in time
						// in this case, fetch the most recent entries from the db that contains 
						// a message denoting this value for this patient and compare
						// theoretically, can only be in the last message of this patient?
						// select numeric from oru_messages where patid = patid and sendTime = thisTime - 5s; ?
						Select getLostSon = QueryBuilder.select().from("oru_messages").where(QueryBuilder.eq("patid", PatID)).limit(10);
						ResultSet rs = session.execute(getLostSon);
						for (Row row : rs) {
							Map<String, TupleValue> tmp = row.getMap("numeric", String.class, TupleValue.class);
							
							// worst solution ever ...
							if (matchedPhrase.contains("Tachy")) {
								matchedPhrase = "HR";
							}
							if (tmp.containsKey(matchedPhrase) && !found) {
								TupleValue tv = tmp.get(matchedPhrase); // measured value // obsunit & thresunit ^
								float value = tv.getFloat(0);
								String unit = tv.getString(1);
								found = true;
								bs.setString("causingUnit", unit);
								bs.setString("threshUnit", unit);
								bs.setFloat("causingValue", value);
								// borders
								Where paraQ = QueryBuilder.select().from("patient_standard_values").allowFiltering().where(QueryBuilder.eq("patid", PatID))
								.and(QueryBuilder.eq("current", true));
								ResultSet paraRS = session.execute(paraQ);
								Row r = paraRS.one();
								Map<String, TupleValue> parameters = r.getMap("parameters", String.class, TupleValue.class);
								TupleValue borders = parameters.get(matchedPhrase);
								// high || low
								String thresh = value < borders.getFloat(0) ? "low" : "high";
								Float tValue = thresh.equalsIgnoreCase("low") ? borders.getFloat(0) : borders.getFloat(1);
								// uuid
								bs.setUUID("uuid", UUID.randomUUID());
								bs.setFloat("threshVal", tValue);
								bs.setString("thresh", thresh);
								// apsfc
								// apsfl
								
								bs.setString("apsfc", lvl);
								bs.setString("apsfl", alert);
								session.execute(bs);
							}
						}
					}
				}
			} else if (alertContained) {
				// alarm indicates that something is not measured, no 
				// values given, soft or hard inop or something disconnected
				String identifier = msg_content.getObsIdentifier(i);
				String label = msg_content.getObsText(i);
				bs.setUUID("uuid", UUID.randomUUID());
				bs.setString("apsfc", identifier);
				bs.setString("apsfl", label);

				String obs5 = msg_content.getObsMsgTxt(i);
				bs.setString("thresh", obs5);
				bs.setFloat("causingValue", -1.0f);
				bs.setFloat("threshVal", -1.f);
				bs.setString("threshUnit", "none");
				bs.setString("causingUnit", "none");
				session.execute(bs);
			} // else no alert is contained 
		}
		
	}
	
	private static float[] parseBorders(String borders) {
		float[] b = new float[2];

		if (borders.contains("-")) {
			int index = borders.lastIndexOf("-");
			b[0] = Float.parseFloat(borders.substring(0, index));
			b[1] = Float.parseFloat(borders.substring(index+1));		
		} else if (borders.startsWith("<")||borders.endsWith(">")) {
			String msg = borders.startsWith("<") ? borders.substring(1) : borders.substring(0, borders.length() - 1);
			b[0] = 0;
			b[1] = Float.parseFloat(msg);
		} else if (borders.startsWith(">")||borders.endsWith("<")) {
			String msg = borders.startsWith(">") ? borders.substring(1) : borders.substring(0, borders.length() - 1);
			b[0] = Float.parseFloat(msg);
			b[1] = Float.parseFloat(msg)*10;			
		}
	
		return b;
	}
}