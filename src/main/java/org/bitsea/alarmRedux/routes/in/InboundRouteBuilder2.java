package org.bitsea.alarmRedux.routes.in;



import org.apache.camel.ExchangePattern;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.hl7.HL7DataFormat;
import org.apache.camel.spi.DataFormat;
import org.bitsea.alarmRedux.alarmListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class InboundRouteBuilder2 extends RouteBuilder {

	static Logger LOG = LoggerFactory.getLogger(alarmListener.class);

	@Override
	public void configure() throws Exception {
		DataFormat hl7 = new HL7DataFormat();

		//clientMode=true&
		from("mina2:tcp://" + System.getProperty("MINAHOST") + ":" + System.getProperty("PORT") + "?clientMode=true&sync=false&textline=false&codec=#hl7codec")		
		//		.unmarshal().hl7(false)
		.to("jms:queue:awaitConsuming?disableReplyTo=true")
		.end();
		
//		from("mina2:tcp://" + System.getProperty("ADTHOST") + ":" + System.getProperty("ADTPORT") + "?sync=false&codec=#hl7codec")
////		.unmarshal().hl7(false)
//		.to("jms:queue:awaitConsuming?disableReplyTo=true").unmarshal().hl7(false)
//		.to("bean:processManager?method=generateACK").marshal().hl7(false)
//		.end();

		from("timer://foo?fixedRate=true&period=900s").to("bean:timeBean?method=callOut");
		

		
	}

	
}


//from("netty4:udp://" + System.getProperty("MINAHOST") + ":8005?sync=false&encoder=#hl7encoder&decoder=#hl7decoder") //&clientMode=true") //&reconnectInterval=1000")
// .to("jms:queue:awaitConsuming?disableReplyTo=true")//.unmarshal().hl7(false)
//.end();

////from("netty4:tcp://" + System.getProperty("MINAHOST") + ":" + System.getProperty("ADTPORT") + "?sync=true&encoder=#hl7encoder&decoder=#hl7decoder")
//from("jms:queue:receiveADT") //?encoder=#hl7encoder&decoder=#hl7decoder")
//.log("foobar")
//.to("jms:queue:awaitConsuming?disableReplyTo=true").unmarshal().hl7(false)
//.to("bean:processManager?method=generateACK").marshal().hl7(false)
//.end();
