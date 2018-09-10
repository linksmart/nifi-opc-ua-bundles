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
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class ListOPCNodesTest {

    private TestRunner testRunner;
    private final String endpoint = "opc.tcp://10.223.104.20:48010";
    private StandardOPCUAService service;

    @Before
    public void init() throws InitializationException {
        testRunner = TestRunners.newTestRunner(ListOPCNodes.class);
        service = new StandardOPCUAService();
        testRunner.addControllerService("controller", service);

        testRunner.setProperty(service, StandardOPCUAService.ENDPOINT, endpoint);
        testRunner.assertValid(service);

        testRunner.enableControllerService(service);
    }

    @Test
    public void testListingNodes() {
        testRunner.setProperty(ListOPCNodes.OPCUA_SERVICE, "controller");
        testRunner.setProperty(ListOPCNodes.MAX_REFERENCE_PER_NODE, "10");
        testRunner.setProperty(ListOPCNodes.PRINT_INDENTATION, "");
        testRunner.setProperty(ListOPCNodes.STARTING_NODE, "ns=4;s=S71500/ET200MP-Station_2.PLC_1");
        testRunner.setProperty(ListOPCNodes.RECURSIVE_DEPTH, "4");
        testRunner.setProperty(ListOPCNodes.PRINT_NON_LEAF_NODES, "false");

        testRunner.run();

        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(GetOPCData.SUCCESS);
        System.out.println(new String(testRunner.getContentAsByteArray(results.get(0))));
    }

}
