package org.bitsea.alarmRedux;

import java.net.URI;

import javax.jms.ConnectionFactory;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.camel.component.ActiveMQComponent;
import org.apache.camel.CamelContext;
import org.apache.camel.component.hl7.HL7MLLPCodec;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.main.Main;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.bitsea.alarmRedux.routes.in.InboundRouteBuilder2;
import org.bitsea.alarmRedux.routes.out.OutboundRouteBuilder;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;


public class alarmListener {

	private Main main;
	// these are for internal communication to the queue that feeds Cassandra
	public static final String URI = "tcp://0.0.0.0:61616";
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
		options.addOption("q", "adthost", true, "The host of the ADT messages. If not otherwise defined, use 127.0.0.1");
		//options.addOption("r", "intern", true, "internal port communication?");
		
		CommandLineParser parser = new BasicParser();
		CommandLine line = parser.parse(options, args);
		
		if (line.hasOption("help") || line.hasOption("h")) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("Tipps", options);
			System.exit(1);
		}

		System.setProperty("PORT", line.getOptionValue("PORT", "8000") );
		System.setProperty("ADTPORT", line.getOptionValue("adt", "22400"));
		System.setProperty("MINAHOST", line.getOptionValue("ip", "127.0.0.1"));
		System.setProperty("ADTHOST", line.getOptionValue("adthost", "127.0.0.1"));
		System.setProperty("DBNAME", line.getOptionValue("dbName", "message_database"));
		System.setProperty("DBPORT", line.getOptionValue("dpPort", "9042"));
		System.setProperty("DBIP", line.getOptionValue("dbIP", "127.0.0.1"));
	}
	
	
	public static void main(String args[]) throws Exception {
		alarmListener app = new alarmListener();
		readCMD(args);
		app.boot();
	}
	
	static Logger LOG = LoggerFactory.getLogger(alarmListener.class);
	
	public void boot() throws Exception {
		main = new Main();

/*
 * the code as is is the code that you received;
 * I would do the following:
 * - test with setValidate commented out
 * - test with setConvertLFtoCR commented out
 * - test with utf8
 * - test with latin1
 * - test with all myCoded commented out, instead with "hl7coded", new HL7MLLPCodec()
 * and after this, test with myCodec, iso-8850-1, setConvertLFtoCR true, and the large block at the end in...
 * 
 * otherwise, I dunno. Did not see anything else that might be wrong or were the error originated.
 * 
 * compiling with maven to a jar
 * in cmd:
 * change to directory.
 * type "mvn clean package"
 * that's it; it should (!) start loading some mvn jars, most likely too many.
 */
		
        HL7MLLPCodec myCodec = new HL7MLLPCodec();
		myCodec.setCharset("iso-8859-1");
//		myCodec.setCharset("utf8");
//		myCodec.setCharset("latin1");
		myCodec.setConvertLFtoCR(true);
		myCodec.setValidate(false);
		main.bind("hl7codec", myCodec);
//		main.bind("hl7codec", new HL7MLLPCodec());
		main.enableHangupSupport();

/*		final org.apache.camel.impl.SimpleRegistry registry = new org.apache.camel.impl.SimpleRegistry();
        final org.apache.camel.impl.CompositeRegistry compositeRegistry = new org.apache.camel.impl.CompositeRegistry();
        CamelContext camelContext = main.getOrCreateCamelContext();
        compositeRegistry.addRegistry(camelContext.getRegistry());
        compositeRegistry.addRegistry(registry);
        ((org.apache.camel.impl.DefaultCamelContext) camelContext).setRegistry(compositeRegistry);
        //registry.put("hl7codec", hl7codec);
		registry.put("hl7codec", myCodec);
*/
		
		
		// only used to transport received data to a queue to go to the DB
		final BrokerService jmsService = BrokerFactory.createBroker(new URI("broker:" + URI));
		jmsService.start();
		
		final JmsComponent jmsComponent = new JmsComponent();
		ConnectionFactory connFactory = new ActiveMQConnectionFactory(URI);
		
//		jmsComponent.setConnectionFactory(connFactory);

		String ipAddress = System.getProperty("DBIP");
		int port = Integer.parseInt(System.getProperty("DBPORT"));
		String keyspace = System.getProperty("DBNAME");
		
		final CassandraConnector client = new CassandraConnector();
		client.connect(ipAddress, port, keyspace);
		Session session = client.getSession();	
		
		main.bind("jms", JmsComponent.jmsComponentAutoAcknowledge(connFactory));
		main.bind("cassandraWriter", new cassandraWriter(session));
		main.bind("processManager", new processManager());
		main.bind("AlarmWriter", new AlarmMessage(session));
		main.bind("DBWriter", new DBWriter(session));
		main.bind("timeBean", new timeBean(session));
		main.addRouteBuilder(new InboundRouteBuilder2());
		main.addRouteBuilder(new OutboundRouteBuilder());

		main.run();
				
	}
}
