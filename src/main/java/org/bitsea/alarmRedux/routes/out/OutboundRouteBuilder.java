package org.bitsea.alarmRedux.routes.out;


import org.apache.camel.NoTypeConversionAvailableException;
import org.apache.camel.Predicate;
import org.apache.camel.builder.PredicateBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.hl7.HL7DataFormat;
import org.springframework.stereotype.Component;


@Component
public class OutboundRouteBuilder extends RouteBuilder {
	
	
	@Override
	public void configure() throws Exception {
		
		Predicate p1 = header("CamelHL7MessageType").isEqualTo("ADT");
		Predicate p2 = header("CamelHL7TriggerEvent").isEqualTo("A01");
		Predicate isADT = PredicateBuilder.and(p1, p2);

		Predicate p3 = header("CamelHL7TriggerEvent").isEqualTo("A03");
		Predicate isA03 = PredicateBuilder.and(p1, p3);
		
		Predicate p21 = header("CamelHL7MessageType").isEqualTo("ORU");
		Predicate p22 = header("CamelHL7TriggerEvent").isEqualTo("R01");
		Predicate isORU = PredicateBuilder.or(p21, p22);
		Predicate tooShort = header("CamelHL7TriggerEvent").isNull();
		
		HL7DataFormat hl7 = new HL7DataFormat();
	
		from("jms:queue:awaitConsuming?disableReplyTo=true")
		.doTry().unmarshal().hl7(false)
			.choice()
				.when(tooShort)
					.to("bean:cassandraWriter?method=patchThrough")
					.endChoice()
				.when(isADT)
					.to("bean:ADTProcessor?method=processADT")
					.endChoice()
				.when(isA03)
					.to("bean:ADTProcessor?method=processA03")
					.endChoice()
				.when(isORU).to("bean:cassandraWriter?method=process")
					.endChoice()
				
				.otherwise()
					.to("bean:cassandraWriter?method=weirdException")
			.end()
			.marshal().hl7(false)
		.endDoTry()
		.doCatch(NoTypeConversionAvailableException.class)
			.to("bean:cassandraWriter?method=withoutHeader")
		.doCatch(IllegalStateException.class)
			.to("bean:cassandraWriter?method=withoutHeader")
		.doCatch(Exception.class)
			.to("bean:cassandraWriter?method=notDecodedMessage")
		.endDoTry()
		.end();
	}

	
}
