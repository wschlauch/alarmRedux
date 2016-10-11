package org.bitsea.alarmRedux;

import org.apache.camel.component.hl7.HL7MLLPCodec;
import org.apache.camel.main.Main;
import org.bitsea.alarmRedux.routes.in.InboundRouteBuilder2;
import org.bitsea.alarmRedux.routes.out.OutboundRouteBuilder;


public class alarmListener {

	private Main main;
	
	public static void main(String args[]) throws Exception {
		alarmListener app = new alarmListener();
		final String port = (args.length >= 1 ? args[0] : "8000");
		app.boot(port);
	}
	
	public void boot(String port) throws Exception {
		System.setProperty("PORT", port);
		main = new Main();
		
		main.bind("hl7codec", new HL7MLLPCodec());
	
		main.enableHangupSupport();
		main.addRouteBuilder(new OutboundRouteBuilder());
		main.addRouteBuilder(new InboundRouteBuilder2());
		main.run();

	}
}
