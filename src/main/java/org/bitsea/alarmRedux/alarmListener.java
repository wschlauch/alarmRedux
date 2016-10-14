package org.bitsea.alarmRedux;

import javax.jms.ConnectionFactory;
import java.net.URI;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.camel.component.hl7.HL7MLLPCodec;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.main.Main;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.bitsea.alarmRedux.routes.in.InboundRouteBuilder2;
import org.bitsea.alarmRedux.routes.out.OutboundRouteBuilder;


public class alarmListener {

	private Main main;
	// these are for internal communication to the queue that feeds Cassandra
	public static final String URI = "tcp://127.0.0.1:61616";
	public static final String JMSPORT = "61616";
	
	public static void readCMD(String[] args) throws ParseException  {
		Options options = new Options();
		options.addOption("h", "help", false, "shows this message.");
		options.addOption("a", "adt", true, "The port ADT messages are expected to arrive at. If not otherwise defined it is set to 22400.");
		options.addOption("o", "oru", true, "The port ORU messages are expected to arrive at. If not otherwise defined it is set to 8000.");
		options.addOption("i", "ip", true, "The IP the system is supposed to listend on (must be within available IPs). If not otherwise defined it is set to 127.0.0.1");
		options.addOption("d", "dbName", true, "Database results are written to. If not otherwise defined it is set to message_database.");
		options.addOption("p", "dbPort", true, "Port of the database messages are going to. If not otherwise defined it is set to 9042.");
		options.addOption("e", "dbIP", true, "IP of the database messages are to be sent to. If not otherwise defined it is set to 127.0.0.1");
		
		CommandLineParser parser = new BasicParser();
		CommandLine line = parser.parse(options, args);
		
		if (line.hasOption("help") || line.hasOption("h")) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("Tipps", options);
			System.exit(1);
		}
		System.out.println("HERE");
		System.setProperty("PORT", line.getOptionValue("PORT", "8000") );
		System.setProperty("ADTPORT", line.getOptionValue("adt", "22400"));
		System.setProperty("MINAHOST", line.getOptionValue("ip", "127.0.0.1"));
		
		System.setProperty("DBNAME", line.getOptionValue("dbName", "message_database"));
		System.setProperty("DBPORT", line.getOptionValue("dpPort", "9042"));
		System.setProperty("DBIP", line.getOptionValue("dbIP", "127.0.0.1"));
	}
	
	
	public static void main(String args[]) throws Exception {
		alarmListener app = new alarmListener();
		readCMD(args);
		app.boot();
	}
	
	public void boot() throws Exception {
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
