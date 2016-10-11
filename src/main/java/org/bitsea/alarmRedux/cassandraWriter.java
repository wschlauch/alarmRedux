package org.bitsea.alarmRedux;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;

import ca.uhn.hl7v2.model.v25.message.ADT_A01;
import ca.uhn.hl7v2.model.v25.message.ORU_R01;
import ca.uhn.hl7v2.model.Message;


@Component
public class cassandraWriter {
	Cluster cluster;
	Session session;
	
	public cassandraWriter() {
		
	}
	
	private void connectToSession(Exchange exchange) {
			List<String> collAddr = (List<String>) exchange.getIn().getHeader("connector");
			String ipAddress = collAddr.get(0);
			int port = Integer.parseInt(collAddr.get(1));
			String keyspace = collAddr.get(2);

			final CassandraConnector client = new CassandraConnector();
			client.connect(ipAddress, port, keyspace);
			session = client.getSession();
	}
	
	public void processADT(Exchange exchange) throws Exception {
		if (session == null) {connectToSession(exchange);}
		
		ADT_A01 msg = exchange.getIn().getBody(ADT_A01.class);
		String idx = msg.getMSH().getMsh10_MessageControlID().getValue();
		String pID = msg.getPID().getPatientID().getCx1_IDNumber().toString();
		if (pID == null) {
			pID = "mistverdammter";
		}
		
		String content = msg.encode();

		SimpleStatement stmt = new SimpleStatement("INSERT INTO adt_messages (id, patientid, content) VALUES (?, ?, ?)", idx, pID, content);
		session.execute(stmt);
		
		Message ack = msg.generateACK();
		exchange.getIn().setBody(ack);
	}
	
	public void process(Exchange exchange) throws Exception {
		if (session == null) { connectToSession(exchange);}

		// jetzt kommt das Handling der ORU Nachricht
		ORU_R01 msg = exchange.getIn().getBody(ORU_R01.class);
		
		// need the id, patient_id, and the complete text of the message;
		String idx = msg.getMSH().getMsh10_MessageControlID().getValue();
		// for 2.3
		//String pID = msg.getRESPONSE().getPATIENT().getPID().getPatientIDInternalID(0).getCx1_ID().toString();
		
		// for 2.5 
		String pID = msg.getPATIENT_RESULT().getPATIENT().getPID().getPatientID().getIDNumber().toString();
		if (pID == null) {
			// input some form of random interger ID with 6 places
			pID = Integer.toString(ThreadLocalRandom.current().nextInt(1, 999999));
		}
		
		String msgText = msg.encode();
		
		// Input new entry to DB
		SimpleStatement doThis = new SimpleStatement("INSERT INTO oru_message_by_patient (id, patientid, content) VALUES (?, ?, ?)", idx, pID, msgText);
		session.execute(doThis);
		// generate ack and add to msg
		Message ack = msg.generateACK();
		
		exchange.getIn().setBody(ack);
	}
}
