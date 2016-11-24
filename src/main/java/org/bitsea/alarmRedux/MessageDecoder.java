package org.bitsea.alarmRedux;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import org.apache.camel.Exchange;

import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.util.Terser;

public class MessageDecoder {
	
	private static Terser terser;
	private static Message msg;
	
	MessageDecoder(Exchange ex) {
		Message msg = ex.getIn().getBody(Message.class);
		terser = new Terser(msg);
	}
	
	
	public long transformMSHTime() throws HL7Exception {
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
		long time;
		try {
			Date date = format.parse(terser.get("/.MSH-7"));
			time = date.getTime();
		} catch (ParseException e) {
			time = System.currentTimeMillis();
		} catch (NullPointerException e) {
			time = System.currentTimeMillis();
		}
		return time;
	}
	
	
	public String getMsg() {
		return msg.toString();
	}
	
	public String getMsgId() throws HL7Exception {
		return terser.get("/.MSH-10");
	}
	
	
	public String getBed() throws HL7Exception {
		return terser.get("/.PV1-3-3");		
	}
	
	
	public String getRoom() throws HL7Exception {
		return terser.get("/.PV1-3-2");
	}
	
	
	public String getStation() throws HL7Exception {
		return terser.get("/.PV1-3-1");
	}
	
	
	public String getPatientID() throws HL7Exception {
		return terser.get("/.PID-3-1");
	}
	
	
	public String getObsText(int i) throws HL7Exception {
		return terser.get("/.OBSERVATION(" + i + ")/OBX-3-2");
	}
	
	
	public String getObsIdentifier(int i) throws HL7Exception {
		return terser.get("/.OBSERVATION(" + i + ")/OBX-3-1");
	}
	
	
	public String getObsCoding(int i) throws HL7Exception {
		return terser.get("/.OBSERVATION(" + i + ")/OBX-3-3");
	}
	
	
	public String getCoding(int i) throws HL7Exception {
		return terser.get("/.OBSERVATION(" + i + ")/OBX-2");
	}
	
	
	public Float getObsValueFlt(int i) throws HL7Exception {
		return Float.parseFloat(terser.get("/.OBSERVATION(" + i + ")/OBX-5"));
	}
	
	
	public String getObsValueStr(int i) throws HL7Exception {
		return terser.get("/.OBSERVATION(" + i + ")/OBX-5");
	} 
	
	
	public String getBorderString(int i) throws HL7Exception {
		return terser.get("/.OBSERVATION(" + i + ")/OBX-7");
	}
	
	
	public String getObsUnit(int i) throws HL7Exception {
		String first = terser.get("/.OBSERVATION(" + i + ")/OBX-6");
		String second = terser.get("/.OBSERVATION(" + i + ")/OBX-6-2");
		String third = terser.get("/.OBSERVATION(" + i + ")/OBX-6-3");
		return first + "^" + second + "^" + third;
	}
	
	
	public HashMap<String, String> generalInformation() throws HL7Exception {
		HashMap<String, String> information = new HashMap<String, String>();
		information.put("msgctrlid", getMsgId());
		information.put("bedInformation", getBed());
		information.put("roomInformation", getRoom());
		information.put("stationInformation", getStation());
		return information;
	}
	
	
	public long getOBRTime() throws HL7Exception, ParseException, NullPointerException {
		String timeTest = terser.get("/.OBR-7");
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
		long time;
		if (!timeTest.isEmpty() && timeTest != null) {
			Date date = format.parse(timeTest);
			time = date.getTime();
		} else {
			time = transformMSHTime();
		} 
		return time;
	}
	
		
	public String getAbnormal(int i) throws HL7Exception {
		String abnorm = "no value";
		try {
			abnorm = terser.get("/.OBSERVATION(" + i + ")/OBX-10");
		} catch (Exception e) {
//			pass
		}
		if (abnorm == null) {
			abnorm = "no value";
		}
		return abnorm;
	}
	
	
	public int getVisitNumber() throws HL7Exception {
		int result = 0;
		try {
			String r = terser.get("/.PV1-19");
			result = Integer.parseInt(r);
		} catch (NumberFormatException e) {
			// pass through
		}
		return result;
	}
	
	
	public String getDateOfBirth() throws HL7Exception {
		return terser.get("/.PID-7");
	}
}
