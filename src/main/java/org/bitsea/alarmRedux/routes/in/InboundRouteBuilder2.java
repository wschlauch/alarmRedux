package org.bitsea.alarmRedux.routes.in;


import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.hl7.HL7DataFormat;
import org.apache.camel.spi.DataFormat;
import org.springframework.stereotype.Component;

@Component
public class InboundRouteBuilder2 extends RouteBuilder {

	
	@Override
	public void configure() throws Exception {
		DataFormat hl7 = new HL7DataFormat();
		from("mina2:udp://" + System.getProperty("MINAHOST") + ":" + System.getProperty("PORT") + "?sync=false&codec=#hl7codec")
		    .to("jms:queue:awaitConsuming?disableReplyTo=true").unmarshal(hl7)
		.end();
		
		from("mina2:tcp://" + System.getProperty("MINAHOST") + ":" + System.getProperty("ADTPORT") + "?sync=true&codec=#hl7codec")
			.to("jms:queue:awaitConsuming?disableReplyTo=true").unmarshal(hl7)
			.to("bean:processManager?method=generateACK").marshal(hl7)
			.end();
	}

}
