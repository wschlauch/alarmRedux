package org.bitsea.alarmRedux.routes.in;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.spring.SpringRouteBuilder;
import org.springframework.stereotype.Component;


@Component
public class InboundRouteBuilder2 extends RouteBuilder {

	@Override
	public void configure() throws Exception {
		from("mina2://tcp:/127.0.0.1:" + System.getProperty("PORT") + "?sync=true&codec=#hl7codec")
		  .routeId("route_hl7listener")
		  .to("????")
		  .end();
	}

}
