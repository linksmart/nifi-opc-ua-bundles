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
package de.fraunhofer.fit.processors.opcua;

import de.fraunhofer.fit.opcua.StandardOPCUAService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;


public class SubscribeOPCNodesTest {

    private TestRunner testRunner;
    private final String endpoint = "opc.tcp://10.223.104.20:48010";
    private StandardOPCUAService service;

    @Before
    public void init() throws InitializationException {
        testRunner = TestRunners.newTestRunner(SubscribeOPCNodes.class);
        service = new StandardOPCUAService();
        testRunner.addControllerService("controller", service);

        testRunner.setProperty(service, StandardOPCUAService.ENDPOINT, endpoint);
        testRunner.assertValid(service);

        testRunner.enableControllerService(service);
    }

    @Test
    public void testProcessor() throws Exception {

        String tagFilePath = (new File("src\\test\\resources\\subscribeTags.txt")).getAbsolutePath();

        testRunner.setProperty(SubscribeOPCNodes.OPCUA_SERVICE, "controller");
        testRunner.setProperty(SubscribeOPCNodes.TAG_FILE_LOCATION, tagFilePath);

        testRunner.run(1, false, true);

        Thread.sleep(10000);

        testRunner.run(1, true, false);


        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(GetOPCData.SUCCESS);
        results.forEach((result)->System.out.println(new String(testRunner.getContentAsByteArray(result))));


    }

    @After
    public void shutdown() {
        testRunner.disableControllerService(service);
    }

}
