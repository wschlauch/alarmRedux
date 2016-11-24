package org.bitsea.alarmRedux;

import java.io.IOException;

import org.springframework.stereotype.Component;

import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.model.v25.message.ACK;

@Component
public class processManager {
	
//	public Message processGeneral(Message in) throws Exception {
//		Message out = in.generateACK();
//		System.out.println("General handling routine");
//		return out;
//		
//	}
//	
//	public Message processADT(Message in) throws Exception {
//		System.out.println("The message was in fact an ADT message!");
//		Message out = in.generateACK();
//		return out;
//	}
	
	public void badMessage(Message in) throws Exception {
		System.out.println("Do not know how to handle this one");
	}
	
	public Message generateACK(Message in) throws Exception {
		ACK ack = new ACK();
        try {
                ack.initQuickstart("ACK", null, null);
        } catch (IOException e1) {
                throw new HL7Exception(e1);
        }

        ack.getMSA().getMsa1_AcknowledgmentCode().setValue("AA");
        ack.getMSA().getMsa2_MessageControlID().setValue("12123321223111234");
        
        return ack;

	}
}
