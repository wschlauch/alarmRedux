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
	
	public MessageDecoder(Exchange ex) {
		this.msg = ex.getIn().getBody(Message.class);
		this.terser = new Terser(msg);
	}
	
	
	public MessageDecoder(Message m) {
		this.msg = m;
		this.terser = new Terser(msg);
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
	
	public String getMonitorAlarmId() throws HL7Exception {
		return terser.get("/.MSH-3");
	}
	
		
	public String getBed() throws HL7Exception {
		String bed = terser.get("/.PV1-3-3");
		if (bed.isEmpty() || bed == "null") {
			bed = terser.get("/.PV1-3-1");
		}
		return bed;
	}
	
	
	public String getRoom() throws HL7Exception {
		String room = terser.get("/.PV1-3-2");
		if (room == null) {
			room = "null";
		}
		return room;
	}
	
	
	public String getStation() throws HL7Exception {
		return terser.get("/.PV1-3-1");
	}
	
	
	public String getPatientID() throws HL7Exception {
		String pid = null;
		try {
			pid = terser.get("/.PID-3-1");
			try {
				int t = Integer.parseInt(pid);
			} catch (NumberFormatException e) {
				pid = null;
			}
		} catch (Exception e) {
			// pass
		}
		return pid;
	}
	
	
	public String getObsText(int i) throws HL7Exception {
		String content = terser.get("/.OBSERVATION(" + i + ")/OBX-3-2");
		if (content.isEmpty() || content == null) {
			content = "null";
		}
		return content;
	}
	
	
	public String getObsIdentifier(int i) throws HL7Exception {
		String content = terser.get("/.OBSERVATION(" + i + ")/OBX-3-1");
		if (content.isEmpty() || content == null) {
			content = "null";
		} 
		return content;
	}
	
	
	public String getObsCoding(int i) throws HL7Exception {
		String content = terser.get("/.OBSERVATION(" + i + ")/OBX-3-3");
		if (content.isEmpty() || content == null) {
			content = "null";
		}
		return content;
	}
	
	
	public String getCoding(int i) throws HL7Exception {
		return terser.get("/.OBSERVATION(" + i + ")/OBX-2");
	}
	
	public String getObsAlarmingDevice(int i) throws HL7Exception {
		return terser.get("/.OBSERVATION(" + i  + ")/OBX-4");
	}
	
	public Float getObsValueFlt(int i) throws HL7Exception {
		return Float.parseFloat(terser.get("/.OBSERVATION(" + i + ")/OBX-5"));
	}
	
	
	public String getObsValueStr(int i) throws HL7Exception {
		return terser.get("/.OBSERVATION(" + i + ")/OBX-5");
	} 
	
	
	public String getBorderString(int i) throws HL7Exception {
		String answer = "";
		try {
			answer = terser.get("/.OBSERVATION(" + i + ")/OBX-7");
		} catch (HL7Exception e) {
			// pass
		}
		if (answer == null) {
			answer = "-100-100";
		}
		return answer;
	}
	
	public long getObsTime(int i) throws HL7Exception, ParseException {
		String timeTest = terser.get("/.OBSERVATION(" + i + ")/OBX-14");
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
	
	
	public String getObsUnit(int i) throws HL7Exception {
		String first="";
		String second="";
		String third = "";
		try {
			first = terser.get("/.OBSERVATION(" + i + ")/OBX-6-1");
		} catch (HL7Exception e) {}
		try {
			second = terser.get("/.OBSERVATION(" + i + ")/OBX-6-2");
		} catch (HL7Exception lam) {}
		try {
			third = terser.get("/.OBSERVATION(" + i + ")/OBX-6-3");
		} catch (HL7Exception e) {}
		if (first == null) {
			first = "null";
		} 
		if (second == null) {
			second = "null";
		}
		if (third == null) {
			third = "null";
		}
		return !(second.isEmpty() || second == null) ? second  : first;
	}
	
	
	public String getObsMsgTxt(int i) throws HL7Exception {
		String first = terser.get("/.OBSERVATION(" + i + ")/OBX-5");
		return first;
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
