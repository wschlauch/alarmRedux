package org.bitsea.alarmRedux.routes.in;

import java.util.ArrayList;
import java.util.List;

import org.apache.camel.Predicate;
import org.apache.camel.builder.PredicateBuilder;
import org.apache.camel.component.hl7.HL7DataFormat;
import org.apache.camel.spi.DataFormat;
import org.apache.camel.spring.SpringRouteBuilder;
import org.springframework.stereotype.Component;


@Component
class InboundRouteBuilder extends SpringRouteBuilder {
	
	@Override
	public void configure() throws Exception {
	
		Predicate p1 = header("CamelHL7MessageType").isEqualTo("ADT");
		Predicate p2 = header("CamelHL7TriggerEvent").isEqualTo("A01");
		Predicate isADT = PredicateBuilder.and(p1, p2);
		
		Predicate p21 = header("CamelHL7MessageType").isEqualTo("ORU");
		Predicate p22 = header("CamelHL7TriggerEvent").isEqualTo("R01");
		Predicate isORU = PredicateBuilder.and(p21, p22);
		
		DataFormat hl7 = new HL7DataFormat();
		
		String addr = "127.0.0.1";
		List<String> collAddr = new ArrayList<String>();
		collAddr.add(addr);
		collAddr.add("9042");
		collAddr.add("hl7test");
		
		
		from("hl7listener")
		  .routeId("route_hl7listener")
		  		.unmarshal(hl7)
		  			.choice()
		  			.when(isADT)
		  				.setHeader("connector", constant(collAddr))
		  				.to("bean:cassandraWriter?method=processADT")
		  			.endChoice()
		  			.when(isORU)
		  				.setHeader("connector", constant(collAddr))
		  				.to("bean:cassandraWriter?method=process")
		  			.endChoice()
		  			.otherwise()
	  					.to("bean:processManager?method=badMessage").to("bean:processManager?method=generateACK")
		  			.end();
		

		
	}

}
