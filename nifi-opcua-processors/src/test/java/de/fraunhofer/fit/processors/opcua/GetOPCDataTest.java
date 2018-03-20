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
import java.nio.file.Paths;
import java.util.List;


public class GetOPCDataTest {

    private TestRunner testRunner;
    private final String endpoint = "opc.tcp://10.223.104.20:48010";
    private StandardOPCUAService service;

    @Before
    public void init() throws InitializationException {
        testRunner = TestRunners.newTestRunner(GetOPCData.class);
        service = new StandardOPCUAService();
        testRunner.addControllerService("controller", service);

        testRunner.setProperty(service, StandardOPCUAService.ENDPOINT, endpoint);
        testRunner.assertValid(service);

        testRunner.enableControllerService(service);
    }

    @Test
    public void testProcessor() {

        String tagFilePath = (new File("src\\test\\resources\\tags.txt")).getAbsolutePath();

        testRunner.setProperty(GetOPCData.OPCUA_SERVICE, "controller");
        testRunner.setProperty(GetOPCData.RETURN_TIMESTAMP, "Both");
        testRunner.setProperty(GetOPCData.EXCLUDE_NULL_VALUE, "Yes");
        testRunner.setProperty(GetOPCData.TAG_LIST_SOURCE, "Local File");
        testRunner.setProperty(GetOPCData.TAG_LIST_FILE, tagFilePath);

        testRunner.run();

        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(GetOPCData.SUCCESS);
        System.out.println(new String(testRunner.getContentAsByteArray(results.get(0))));

    }

    @After
    public void shutdown() {
        testRunner.disableControllerService(service);
    }

}
