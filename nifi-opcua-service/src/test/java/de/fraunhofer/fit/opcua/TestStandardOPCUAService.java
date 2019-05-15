/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.fraunhofer.fit.opcua;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TestStandardOPCUAService {

    private final String endpoint = "opc.tcp://10.223.104.20:48010";
    private TestRunner runner;
    private StandardOPCUAService service;

    @Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(TestProcessor.class);
        service = new StandardOPCUAService();
        runner.addControllerService("test-good", service);
    }

/*    @Test
    public void testServiceInitialization() {

        runner.setProperty(service, StandardOPCUAService.ENDPOINT, endpoint);
        runner.assertValid(service);

        runner.enableControllerService(service);

        runner.disableControllerService(service);

    }

    @Test
    public void testServiceGetNodes() {
        runner.setProperty(service, StandardOPCUAService.ENDPOINT, endpoint);
        runner.assertValid(service);

        runner.enableControllerService(service);

        System.out.println(new String(service.getNodes("--", 3, 10, false,
                "ns=4;s=S71500/ET200MP-Station_2.PLC_1.GlobalVars")));

        runner.disableControllerService(service);
    }

    @Test
    public void testServiceGetValues() {
        runner.setProperty(service, StandardOPCUAService.ENDPOINT, endpoint);
        runner.assertValid(service);

        runner.enableControllerService(service);

        List<String> tagList = Arrays.asList("ns=4;s=S71500/ET200MP-Station_2.PLC_1.GlobalVars.I_MAG1_EXT",
                "ns=4;s=S71500/ET200MP-Station_2.PLC_1.GlobalVars.I_MAG2_EXT");

        byte[] bytes = service.getValue(tagList, "Both", true, "");
        System.out.println(new String(bytes));

        runner.disableControllerService(service);
    }

    @Test
    public void testSecurityAccess() {
        runner.setProperty(service, StandardOPCUAService.ENDPOINT, endpoint);
        runner.setProperty(service, StandardOPCUAService.SECURITY_POLICY, "Basic128Rsa15");
        runner.setProperty(service, StandardOPCUAService.SECURITY_MODE, "SignAndEncrypt");
        runner.setProperty(service, StandardOPCUAService.CLIENT_KS_LOCATION, "src/test/resources/client.jks");
        runner.setProperty(service, StandardOPCUAService.CLIENT_KS_PASSWORD, "password");
        runner.setProperty(service, StandardOPCUAService.REQUIRE_SERVER_AUTH, "true");
        runner.setProperty(service, StandardOPCUAService.TRUSTSTORE_LOCATION, "src/test/resources/trust.jks");
        runner.setProperty(service, StandardOPCUAService.TRUSTSTORE_PASSWORD, "SuperSecret");
        runner.setProperty(service, StandardOPCUAService.AUTH_POLICY, "Anon");

        runner.assertValid(service);

        runner.enableControllerService(service);

        List<String> tagList = Arrays.asList("ns=4;s=S71500/ET200MP-Station_2.PLC_1.GlobalVars.I_MAG1_EXT",
                "ns=4;s=S71500/ET200MP-Station_2.PLC_1.GlobalVars.I_MAG2_EXT");

        byte[] bytes = service.getValue(tagList, "Both", true, "");
        System.out.println(new String(bytes));

        runner.disableControllerService(service);
    }

    @Test(expected = InitializationException.class)
    public void testSecurityAccessWrongTrustStore() throws InitializationException {
        runner.setProperty(service, StandardOPCUAService.ENDPOINT, endpoint);
        runner.setProperty(service, StandardOPCUAService.SECURITY_POLICY, "Basic128Rsa15");
        runner.setProperty(service, StandardOPCUAService.SECURITY_MODE, "SignAndEncrypt");
        runner.setProperty(service, StandardOPCUAService.CLIENT_KS_LOCATION, "src/test/resources/client.jks");
        runner.setProperty(service, StandardOPCUAService.CLIENT_KS_PASSWORD, "password");
        runner.setProperty(service, StandardOPCUAService.REQUIRE_SERVER_AUTH, "true");
        runner.setProperty(service, StandardOPCUAService.TRUSTSTORE_LOCATION, "src/test/resources/trust-wrong.jks");
        runner.setProperty(service, StandardOPCUAService.TRUSTSTORE_PASSWORD, "password");
        runner.setProperty(service, StandardOPCUAService.AUTH_POLICY, "Anon");

        runner.assertValid(service);

        try {
            runner.enableControllerService(service);
        } catch (AssertionError e) {
            throw new InitializationException("");
        }

        List<String> tagList = Arrays.asList("ns=4;s=S71500/ET200MP-Station_2.PLC_1.GlobalVars.I_MAG1_EXT",
                "ns=4;s=S71500/ET200MP-Station_2.PLC_1.GlobalVars.I_MAG2_EXT");

        byte[] bytes = service.getValue(tagList, "Both", true, "");
        System.out.println(new String(bytes));

        runner.disableControllerService(service);
    }*/

    @Test
    public void testUsernameSecurityAccess() {
        runner.setProperty(service, StandardOPCUAService.ENDPOINT, endpoint);
        runner.setProperty(service, StandardOPCUAService.SECURITY_POLICY, "Basic256Sha256");
        runner.setProperty(service, StandardOPCUAService.SECURITY_MODE, "SignAndEncrypt");
        runner.setProperty(service, StandardOPCUAService.APPLICATION_URI, "urn:ibhlinkua_001151:IBHsoftec:IBHLinkUA");
        runner.setProperty(service, StandardOPCUAService.CLIENT_KS_LOCATION, "src/test/resources/client.jks");
        runner.setProperty(service, StandardOPCUAService.CLIENT_KS_PASSWORD, "password");
        runner.setProperty(service, StandardOPCUAService.REQUIRE_SERVER_AUTH, "true");
        runner.setProperty(service, StandardOPCUAService.TRUSTSTORE_LOCATION, "src/test/resources/trust.jks");
        runner.setProperty(service, StandardOPCUAService.TRUSTSTORE_PASSWORD, "SuperSecret");
        runner.setProperty(service, StandardOPCUAService.AUTH_POLICY, "Username");
        runner.setProperty(service, StandardOPCUAService.USERNAME, "test1");
        runner.setProperty(service, StandardOPCUAService.PASSWORD, "password");

        runner.assertValid(service);

        runner.enableControllerService(service);

        List<String> tagList = Arrays.asList("ns=4;s=S71500/ET200MP-Station_2.PLC_1.GlobalVars.I_MAG1_EXT",
                "ns=4;s=S71500/ET200MP-Station_2.PLC_1.GlobalVars.I_MAG2_EXT");

        byte[] bytes = service.getValue(tagList, "Both", true, "");
        System.out.println(new String(bytes));

        runner.disableControllerService(service);
    }

}
