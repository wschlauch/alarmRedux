package org.bitsea.alarmRedux;

import java.util.HashMap;

import org.apache.camel.Exchange;

import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.parser.PipeParser;
import ca.uhn.hl7v2.util.Terser;


public class ParserOnDB {

	
	private Message modifyMsg(Message m) throws HL7Exception {
		Terser n = new Terser(m);
		n.set("/.PID-3-1", "0118");
		n.set("/.PID-5-1", "Bob");
		n.set("/.PID-5-2", "Sedgewick");

		return m;
	}
	
	/*
	 * input is a hashmap that describes the current entry as 
	 * <String, Object> with the entries <id, String(INT)>, <message, STRING>
	 */
	public void testMessage(Exchange ex) {
		HashMap<String, Object> content = ex.getIn().getBody(HashMap.class);
		String messageContent = (String) content.get("message");
		
		// test whether MSH-10 is complete
		String[] splitted = messageContent.split("\\|");

		// other lines are more or less do not care, first of all, MSH
		// it is MSH-9 that concerns, thus [8]
		String header = splitted[8];
		
		// supposed to be MSG-Type^Subtype
		String[] types = header.split("\\^");
		
		// depending on the Headers I know, let's add something
		if (types.length<2) {
			String msh91 = types[0];
			String toReplace = "A01";
			if (msh91.equals("NMD")) {
				toReplace = "N02";
			} else if (msh91.equals("ORU")) {
				toReplace = "R01";
			} 
			types = new String[] {types[0], toReplace};
		}
		// join types, might be unnecessary depending on whether we entered the if-block
		String header_new = String.join("^", types);
		messageContent = messageContent.replace(header, header_new);

		// try to parse it as a message; if this works, the MSH seems to be correct
		// and we may proceed with adding some necessary stuff to make it a valid HL7 message
		final PipeParser piper = new PipeParser();
		Message msg;
		try {
			msg = (Message) piper.parse(messageContent);
			msg = modifyMsg(msg);
			messageContent = msg.toString();
		} catch (HL7Exception exc) {
			messageContent = exc.getMessage();
		}	
		content.put("message", messageContent);
		ex.getIn().setBody(content);
	}
	
}
