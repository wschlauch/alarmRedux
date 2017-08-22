package org.bitsea.alarmRedux.routes.in;



import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

import javax.jms.JMSException;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ShutdownRunningTask;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.hl7.HL7DataFormat;
import org.bitsea.alarmRedux.ParserOnDB;
import org.bitsea.alarmRedux.alarmListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.parser.PipeParser;

@Component
public class InboundRouteBuilder2 extends RouteBuilder {

	static Logger LOG = LoggerFactory.getLogger(alarmListener.class);
	private long tmpEntryId = 0L;
//	private long tmpMinError = 0;

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
		final PipeParser piper = new PipeParser();

		loadProperties();
		
		onException(java.lang.IllegalStateException.class).process(new Processor() {
			public void process(Exchange arg0) throws Exception {
			System.out.println("message arrived in wrong pipe");	
			}
			
		}).handled(true).to("bean:timeBean?method=callOut");

		from("netty4:tcp://" + prop.getProperty("MINAHOST") + ":"+ prop.getProperty("PORT")+"?sync=false&textline=false&encoder=#hl7encoder&decoder=#hl7decoder&clientMode=true") //&reconnectInterval=1000")
		.shutdownRunningTask(ShutdownRunningTask.CompleteAllTasks)
		.doTry()
			.to("jms:queue:awaitConsuming?disableReplyTo=true")
		.doCatch(JMSException.class)
			.process(new Processor() {
				public void process(Exchange arg0) throws Exception {
					arg0.getException().printStackTrace();
				}
			})
		.endDoTry()
		.end();

		from("timer://pollDatabase?fixedRate=true&period=60s")
		.process(new Processor() {
			public void process(Exchange ex) throws Exception {
				ex.getIn().setBody("select id, message from oru where id > " + tmpEntryId);
			}
		})
		.to("jdbc:DBReader")
		.split(body()).stopOnException().bean(ParserOnDB.class, "testMessage")
		.process(new Processor() {
			public void process(Exchange ex) throws Exception {
				HashMap<String, Object> rs = (HashMap<String, Object>) ex.getIn().getBody();
				String message = (String) rs.get("message");
				Message msg = piper.parse(message);
				Integer tmpi = (Integer) rs.get("id");
				long id =  Long.valueOf(tmpi.longValue());
				ex.getIn().setBody(msg);
				ex.getIn().setHeader("mid", id);;
			}
		})
		.marshal().hl7(false)
			.to("jms:queue:awaitConsuming?disableReplyTo=true")
			.process(new Processor() {
				public void process(Exchange ex) throws Exception {
					long id = (Long) ex.getIn().getHeader("mid");

					if (id > tmpEntryId) {
						tmpEntryId = id;
					}
				}
			})
		 .end();		
		
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