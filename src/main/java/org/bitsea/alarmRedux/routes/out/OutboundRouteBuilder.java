package org.bitsea.alarmRedux.routes.out;


import org.apache.camel.Exchange;
import org.apache.camel.Predicate;
import org.apache.camel.Processor;
import org.apache.camel.builder.PredicateBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.hl7.HL7DataFormat;
import org.apache.camel.spi.DataFormat;
import org.springframework.stereotype.Component;

import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.model.Message;

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
				
		DataFormat hl7 = new HL7DataFormat();
		
		
	
		from("jms:queue:awaitConsuming?disableReplyTo=true")
		.doTry().unmarshal().hl7(false)
			.choice()
				.when(isADT)
					.to("bean:ADTProcessor?method=processADT")
					.endChoice()
				.when(isA03)
					.to("bean:ADTProcessor?method=processA03")
					.endChoice()
				.when(isORU).to("bean:cassandraWriter?method=process")
					.endChoice()
				.when(body().isInstanceOf(Message.class))
					.to("bean:cassandraWriter?method=patchThrough")
					.endChoice()
				.otherwise()
					.to("bean:cassandraWriter?method=notDecodedMessage")
			.end()
			.marshal().hl7(false)
		.endDoTry()
		.doCatch(Exception.class).process(new Processor() {
			public void process(Exchange ex) {
				ex.getIn().setBody(null);
				ex.getOut().setBody(null);
			}
		}).handled(true).to("bean:cassandraWriter?method=notDecodedMessage")
//		.doCatch(NoTypeConversionAvailableException.class, IllegalStateException.class)
//			.to("bean:cassandraWriter?method=withoutHeader")
//		.doCatch(Exception.class)
//			.to("bean:cassandraWriter?method=weirdException")
		.endDoTry()
		.end();
	}

	
}
