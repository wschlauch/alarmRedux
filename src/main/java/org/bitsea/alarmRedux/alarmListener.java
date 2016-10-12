package org.bitsea.alarmRedux;

import javax.jms.ConnectionFactory;
import java.net.URI;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.camel.component.hl7.HL7MLLPCodec;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.main.Main;
import org.bitsea.alarmRedux.routes.in.InboundRouteBuilder2;
import org.bitsea.alarmRedux.routes.out.OutboundRouteBuilder;


public class alarmListener {

	private Main main;
	public static final String URI = "tcp://127.0.0.1:61616";
	public static final String JMSPORT = "61616";
	
	public static void main(String args[]) throws Exception {
		alarmListener app = new alarmListener();
		final String port = (args.length >= 1 ? args[0] : "8000");
		app.boot(port);
	}
	
	public void boot(String port) throws Exception {
		System.setProperty("PORT", port);
		System.setProperty("MINAHOST", "127.0.0.1");
		main = new Main();
		
		main.bind("hl7codec", new HL7MLLPCodec());
		main.enableHangupSupport();
		
		final BrokerService jmsService = BrokerFactory.createBroker(new URI("broker:" + URI));
		jmsService.start();
		
		final JmsComponent jmsComponent = new JmsComponent();
		ConnectionFactory connFactory = new ActiveMQConnectionFactory(URI);
		jmsComponent.setConnectionFactory(connFactory);
		main.bind("jms", jmsComponent);
		main.bind("cassandraWriter", new cassandraWriter());
		main.bind("processManager", new processManager());
		main.addRouteBuilder(new InboundRouteBuilder2());
		main.addRouteBuilder(new OutboundRouteBuilder());
		main.run();

	}
}
