package org.bitsea.alarmRedux;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Properties;

import javax.jms.ConnectionFactory;
import javax.sql.DataSource;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.camel.component.hl7.HL7MLLPNettyDecoderFactory;
import org.apache.camel.component.hl7.HL7MLLPNettyEncoderFactory;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.main.Main;
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

	public static void main(String args[]) throws Exception {
		alarmListener app = new alarmListener();
		app.boot();
	}
	
	static Logger LOG = LoggerFactory.getLogger(alarmListener.class);
	
	public void boot() throws Exception {
		main = new Main();
		Properties prop = new Properties();

		main.bind("hl7decoder", new HL7MLLPNettyDecoderFactory());
		main.bind("hl7encoder", new HL7MLLPNettyEncoderFactory());
		main.enableHangupSupport();

		InputStream input = null;
		try {
			input = new FileInputStream("database.properties");
			prop.load(input);
		} catch (IOException ex) {
			ex.printStackTrace();
		} catch (NullPointerException e) {
			System.out.println("HERE");
			e.printStackTrace();
			
		} finally {
			if (input != null) {
				try {input.close(); }
				catch (IOException ex) {
					ex.printStackTrace();
				}
			}
		}
		// only used to transport received data to a queue to go to the DB
		final BrokerService jmsService = BrokerFactory.createBroker(new URI("broker:" + URI));
		jmsService.start();
		
		ConnectionFactory connFactory = new ActiveMQConnectionFactory(URI);
		
		String ipAddress = prop.getProperty("DBIP");
		int port = Integer.parseInt(prop.getProperty("DBPORT"));
		String keyspace = prop.getProperty("DBNAME");
		
		DBRConnector dbr = new DBRConnector();
		dbr.connect(prop);
		DataSource ds = dbr.getDataSource();
		main.bind("DBReader", ds);
		
		
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
