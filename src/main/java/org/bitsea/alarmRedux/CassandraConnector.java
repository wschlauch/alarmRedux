package org.bitsea.alarmRedux;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

class CassandraConnector {

	private Cluster cluster;
	private Session session;
	
	/*
	 * Connect to Cassandra cluster specified by node ip and port number
	 */
	public void connect(final String node, final int port, final String keyspace) {
		this.cluster = Cluster.builder().addContactPoint(node).withPort(port).build();
		session = cluster.connect(keyspace);
	}
	
	/* 
	 * provide Session
	 */
	public Session getSession() {
		return session;
	}
	
	/*
	 * close Cluster
	 */
	
	public void close() {
		cluster.close();
	}
}
