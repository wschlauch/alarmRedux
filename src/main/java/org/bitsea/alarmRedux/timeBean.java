package org.bitsea.alarmRedux;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

@Component
public class timeBean {

	Session session;
	
	public timeBean(Session sess) {
		this.session = sess;
	}
	
	public void callOut(Exchange exchange) {
		Select query = QueryBuilder.select().countAll().from("oru_messages");
		ResultSet rs = session.execute(query);
		
		long num = rs.all().get(0).getLong(0);
		long systime = System.currentTimeMillis();
		SimpleDateFormat sddf = new SimpleDateFormat("dd.MM.yyyy HH:mm");
		Date result = new Date(systime);
		
		System.out.println("System has " + num + " ORU messages at time " + sddf.format(result) );
	}
	
}
