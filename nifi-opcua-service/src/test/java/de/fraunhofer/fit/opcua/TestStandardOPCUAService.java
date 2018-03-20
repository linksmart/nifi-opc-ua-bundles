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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestStandardOPCUAService {

    private final String endpoint = "opc.tcp://10.223.104.20:48010";

    @Before
    public void init() {

    }

    @Test
    public void testServiceInitialization() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final StandardOPCUAService service = new StandardOPCUAService();
        runner.addControllerService("test-good", service);

        runner.setProperty(service, StandardOPCUAService.ENDPOINT, endpoint);
        runner.assertValid(service);

        runner.enableControllerService(service);

        runner.disableControllerService(service);

    }

    @Test
    public void testServiceGetNodes() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final StandardOPCUAService service = new StandardOPCUAService();
        runner.addControllerService("test-good", service);

        runner.setProperty(service, StandardOPCUAService.ENDPOINT, endpoint);
        runner.assertValid(service);

        runner.enableControllerService(service);

        System.out.println(service.getNodes("--", 3, 10, false,
                "ns=4;s=S71500/ET200MP-Station_2.PLC_1.GlobalVars"));

        runner.disableControllerService(service);
    }

    @Test
    public void testServiceGetValues() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final StandardOPCUAService service = new StandardOPCUAService();
        runner.addControllerService("test-good", service);

        runner.setProperty(service, StandardOPCUAService.ENDPOINT, endpoint);
        runner.assertValid(service);

        runner.enableControllerService(service);

        List<String> tagList = Arrays.asList("ns=4;s=S71500/ET200MP-Station_2.PLC_1.GlobalVars.I_MAG1_EXT",
                "ns=4;s=S71500/ET200MP-Station_2.PLC_1.GlobalVars.I_MAG2_EXT");

        byte[] bytes = service.getValue(tagList, "Both", true, "");
        System.out.println (new String(bytes));

        runner.disableControllerService(service);
    }

}
