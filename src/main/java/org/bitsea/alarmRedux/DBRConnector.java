package org.bitsea.alarmRedux;

import java.util.Properties;

import javax.sql.DataSource;

//import org.apache.camel.language.Bean;
import org.apache.commons.dbcp.BasicDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

public class DBRConnector {

	private DataSource ds;

	public void connect(Properties prop) {
		BasicDataSource ds = new BasicDataSource();
		ds.setDriverClassName(prop.getProperty("datasource.driverClassName")); // we do this general such that there is no problem for future references
		ds.setUsername(prop.getProperty("datasource.username"));
		ds.setPassword(prop.getProperty("datasource.password"));
		ds.setUrl(prop.getProperty("datasource.url"));
		this.ds = ds;
	}
	
	public DataSource getDataSource() {
		return this.ds;
	}
	
}
