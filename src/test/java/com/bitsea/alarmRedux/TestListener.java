package com.bitsea.alarmRedux;

import org.apache.camel.component.hl7.HL7MLLPCodec;
import org.apache.camel.impl.JndiRegistry;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Test;

import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.parser.PipeParser;
import ca.uhn.hl7v2.util.Terser;
import ca.uhn.hl7v2.HL7Exception;


public class TestListener extends CamelTestSupport {
    @Test
    public void testHl7Codec() throws Exception {

		String inMessage = "MSH|^~\\&|||||||ORU^R01|HP0802104801771B02WIN85|T|2.5||||||8859/1\r"
				+ "PID|123|23411|12312411^67731^53757^1121^MR|554145|'bob'^'hope'|23421||124521\r"
				+ "PV1|14542|I|ICU/PCU^^pDemo6&0&0\r"
				+ "OBR|||||||20160802104800\r"
				+ "OBX||ST|0402-f8f5^sMode^MDIL|0|MONITORING||||||F||SETTING\r"
				+ "OBX||ST|0002-f887^Model^MDIL|0|Monitoring Data||||||F||CONFIGURATION\r"
				+ "OBX||TX|5^HardInop^MDIL-ALERT||SpO2T  No Pulse||||||F\r"
				+ "OBX||NM|0002-500a^RR^MDIL|0|30|0004-0ae0^rpm^MDIL|8-30||||F\r"
				+ "OBX||NM|0002-4182^HR^MDIL|0|80|0004-0aa0^bpm^MDIL|50-120||||F\r"
				+ "OBX||NM|0002-4261^PVC^MDIL|0|0|0004-09e0^/min^MDIL|||||F\r"
				+ "OBX||ST|0002-d007^RhySta^MDIL|0|Learning Rhythm||||||F\r"
				+ "OBX||ST|0002-d006^EctSta^MDIL|0|||||||F\r";
		String out = (String) template.requestBody("mina2:tcp://127.0.0.1:8000?sync=true&codec=#hl7codec", inMessage);
        
        assertNotNull(out);
        //Check that the output is an ACK message
        assertEquals("ACK", getMessageType(out));

    }

   
    @Override
	protected JndiRegistry createRegistry() throws Exception {
        JndiRegistry jndi = super.createRegistry();
        jndi.bind("hl7codec", new HL7MLLPCodec());
        return jndi;
    }

    /**
     * We can use Hapi Terser to retrieve the type of Message
     * @param message HL7 Message
     * @return Type of the HL7 Message (e.g. ACK)
     */
    private String getMessageType(String message) throws HL7Exception{
        PipeParser pipeParser = new PipeParser();
        Message hl7Message = pipeParser.parse(message);
        Terser terser = new Terser(hl7Message);

        return terser.get("/MSH-9");

}
}
