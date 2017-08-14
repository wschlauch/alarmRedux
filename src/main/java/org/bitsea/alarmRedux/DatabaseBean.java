package org.bitsea.alarmRedux;

import java.sql.DriverManager;

import org.springframework.stereotype.Component;

@Component
public class DatabaseBean {
//	private Connetion connection;
	
	/*
	 * suppose, that we stay with the current setup and get the HL7 messages, put them into some DB
	 * such that later on the data is retrieved and put forth into the next db,
	 * then 
	 */
	public DatabaseBean(String pw) {
		
//		this.connection = DriverManager.getConnection(System.getProperty(""));
	}
	
}
