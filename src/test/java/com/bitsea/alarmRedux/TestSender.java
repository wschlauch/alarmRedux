package com.bitsea.alarmRedux;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.hl7.HL7MLLPCodec;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.impl.JndiRegistry;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Test;


public class TestSender extends CamelTestSupport {
	  
	/* 
	 * Route definiton
	 */

	@Override
	protected RouteBuilder createRouteBuilder() throws Exception {
		
		return new RouteBuilder() {
			@Override
			public void configure() throws Exception {
				from("file:src/data").process(new Processor() {
					public void process(Exchange exchange) throws Exception {
						System.out.println("huhu");
					}
				}).end();
			}
		};
	}
	

	@Test
    public void test() throws Exception {
		
	}
	

	    protected JndiRegistry createRegistry() throws Exception {
	        JndiRegistry jndi = super.createRegistry();
	        jndi.bind("hl7codec", new HL7MLLPCodec());
	        return jndi;
	    }
}
