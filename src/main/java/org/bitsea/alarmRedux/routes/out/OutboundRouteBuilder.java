package org.bitsea.alarmRedux.routes.out;

import org.apache.camel.Predicate;
import org.apache.camel.builder.PredicateBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.hl7.HL7DataFormat;
import org.apache.camel.spi.DataFormat;
import org.springframework.stereotype.Component;

@Component
public class OutboundRouteBuilder extends RouteBuilder {
	
	@Override
	public void configure() throws Exception {
		Predicate p1 = header("CamelHL7MessageType").isEqualTo("ADT");
		Predicate p2 = header("CamelHL7TriggerEvent").isEqualTo("A01");
		Predicate isADT = PredicateBuilder.and(p1, p2);
		
		Predicate p21 = header("CamelHL7MessageType").isEqualTo("ORU");
		Predicate p22 = header("CamelHL7TriggerEvent").isEqualTo("R01");
		Predicate isORU = PredicateBuilder.and(p21, p22);
		
		DataFormat hl7 = new HL7DataFormat();
		
		
	
		from("jms:queue:awaitConsuming?disableReplyTo=true").unmarshal(hl7)
		.choice()
			.when(isADT)
				.to("bean:cassandraWriter?method=processADT")
				.endChoice()
			.when(isORU)
				.to("bean:cassandraWriter?method=process")
				.endChoice()
			.otherwise()
				.to("bean:processManager?method=badMessage").endChoice()
			.end().marshal(hl7);
			
		
	}

	
}
