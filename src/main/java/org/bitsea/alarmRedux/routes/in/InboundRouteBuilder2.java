package org.bitsea.alarmRedux.routes.in;



import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.hl7.HL7DataFormat;
import org.apache.camel.spi.DataFormat;
//import org.apache.mina.filter.codec.ProtocolDecoderException;
import org.bitsea.alarmRedux.alarmListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class InboundRouteBuilder2 extends RouteBuilder {

	static Logger LOG = LoggerFactory.getLogger(alarmListener.class);

	private Properties prop = new Properties();
	
	private void loadProperties() {
		InputStream input = null;
		try {
			input = new FileInputStream("connections.properties");
			prop.load(input);
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {input.close(); }
				catch (IOException ex) {
					ex.printStackTrace();
				}
			}
		}
	}
	
	@Override
	public void configure() throws Exception {
		HL7DataFormat hl7 = new HL7DataFormat();
		hl7.setValidate(false);
		loadProperties();
		
		onException(java.lang.IllegalStateException.class).process(new Processor() {
			public void process(Exchange arg0) throws Exception {
			System.out.println("message arrived in wrong pipe");	
			}
			
		}).handled(true).to("bean:timeBean?method=callOut");
		//clientMode=true&

		from("netty4:tcp://" + prop.getProperty("MINAHOST") + ":"+ prop.getProperty("PORT")+"?sync=false&textline=false&encoder=#hl7encoder&decoder=#hl7decoder&clientMode=true") //&reconnectInterval=1000")
		.to("jms:queue:awaitConsuming?disableReplyTo=true")//.unmarshal().hl7(false)
		.end();

		from("timer://pollDatabase?delay=3000").setBody(constant("select * from oru"))
		.to("jdbc:DBReader")
		//.to("bean:databaseBean?method=getMessages")
		.process(new Processor() {
			public void process(Exchange arg) throws Exception {
				System.out.println(arg.getIn().getBody().toString());
			}
		});
		
		from("timer://foo?fixedRate=true&period=10s").to("bean:timeBean?method=callOut");
		

		
	}

	
}




////from("netty4:tcp://" + System.getProperty("MINAHOST") + ":" + System.getProperty("ADTPORT") + "?sync=true&encoder=#hl7encoder&decoder=#hl7decoder")
//from("jms:queue:receiveADT") //?encoder=#hl7encoder&decoder=#hl7decoder")
//.log("foobar")
//.to("jms:queue:awaitConsuming?disableReplyTo=true").unmarshal().hl7(false)
//.to("bean:processManager?method=generateACK").marshal().hl7(false)
//.end();

//MINA 2
//clientMode=true&
//from("mina2:tcp://" + System.getProperty("MINAHOST") + ":" + System.getProperty("PORT") + "?clientMode=true&sync=false&textline=false&codec=#hl7codec")		
//		.unmarshal().hl7(false)
//.to("jms:queue:awaitConsuming?disableReplyTo=true")
//.end();

//from("mina2:tcp://" + System.getProperty("ADTHOST") + ":" + System.getProperty("ADTPORT") + "?sync=false&codec=#hl7codec")
////.unmarshal().hl7(false)
//.to("jms:queue:awaitConsuming?disableReplyTo=true").unmarshal().hl7(false)
//.to("bean:processManager?method=generateACK").marshal().hl7(false)
//.end();