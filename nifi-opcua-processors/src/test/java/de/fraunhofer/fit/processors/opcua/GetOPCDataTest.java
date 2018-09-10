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
import org.mockito.Mockito;

import java.io.File;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.spy;


public class GetOPCDataTest {

    private TestRunner testRunner;
    private StandardOPCUAService service;

    @Before
    public void init() throws InitializationException {
        testRunner = TestRunners.newTestRunner(GetOPCData.class);

        // Use partial mock
        service = spy(new StandardOPCUAService());
        Mockito.doNothing().when(service).onEnabled(any());
        Mockito.doNothing().when(service).shutdown();

        testRunner.addControllerService("controller", service);

        testRunner.setProperty(service, StandardOPCUAService.ENDPOINT, "dummy endpoint");
        testRunner.assertValid(service);

        testRunner.enableControllerService(service);
    }

    @Test
    public void testGetData() {

        String tagFilePath = (new File("src/test/resources/tags.txt")).getAbsolutePath();

        testRunner.setProperty(GetOPCData.OPCUA_SERVICE, "controller");
        testRunner.setProperty(GetOPCData.RETURN_TIMESTAMP, "Both");
        testRunner.setProperty(GetOPCData.EXCLUDE_NULL_VALUE, "Yes");
        testRunner.setProperty(GetOPCData.TAG_LIST_SOURCE, "Local File");
        testRunner.setProperty(GetOPCData.TAG_LIST_FILE, tagFilePath);

        byte[] values = new String(
                "ns=4;s=S71500/ET200MP-Station_2.PLC_1.GlobalVars.I_MAG1_EXT,123456,123456,1,0\n" +
                "ns=4;s=S71500/ET200MP-Station_2.PLC_1.GlobalVars.I_MAG1_RET,123456,123456,2,0").getBytes();

        Mockito.doReturn(values).when(service).getValue(any(), any(), anyBoolean(), any());

        testRunner.run();

        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(GetOPCData.SUCCESS);
        assertEquals(1, results.size());
        results.get(0).assertContentEquals("ns=4;s=S71500/ET200MP-Station_2.PLC_1.GlobalVars.I_MAG1_EXT,123456,123456,1,0\n" +
                "ns=4;s=S71500/ET200MP-Station_2.PLC_1.GlobalVars.I_MAG1_RET,123456,123456,2,0");
        //System.out.println(new String(testRunner.getContentAsByteArray(results.get(0))));

    }

    @Test
    public void testGetDataWithAggregation() {

        String tagFilePath = (new File("src/test/resources/tags.txt")).getAbsolutePath();

        testRunner.setProperty(GetOPCData.OPCUA_SERVICE, "controller");
        testRunner.setProperty(GetOPCData.RETURN_TIMESTAMP, "Both");
        testRunner.setProperty(GetOPCData.EXCLUDE_NULL_VALUE, "Yes");
        testRunner.setProperty(GetOPCData.TAG_LIST_SOURCE, "Local File");
        testRunner.setProperty(GetOPCData.TAG_LIST_FILE, tagFilePath);
        testRunner.setProperty(GetOPCData.AGGREGATE_RECORD, "true");

        byte[] values = new String(
                "ns=4;s=S71500/ET200MP-Station_2.PLC_1.GlobalVars.I_MAG1_EXT,123456,123456,1,0" + System.lineSeparator() +
                        "ns=4;s=S71500/ET200MP-Station_2.PLC_1.GlobalVars.I_MAG1_RET,123456,123456,2,0"+ System.lineSeparator() +
                        "ns=4;s=S71500/ET200MP-Station_2.PLC_1.GlobalVars.I_MAG2_RET,123456,123456,3,0"+ System.lineSeparator() +
                        "ns=4;s=S71500/ET200MP-Station_2.PLC_1.GlobalVars.I_MAG2_RET,123456,123456,4,0"+ System.lineSeparator() +
                        "ns=4;s=S71500/ET200MP-Station_2.PLC_1.GlobalVars.I_MAG3_RET,123456,123456,5,0"+ System.lineSeparator() +
                        "ns=4;s=S71500/ET200MP-Station_2.PLC_1.GlobalVars.I_MAG3_RET,123456,123456,6,0").getBytes();

        Mockito.doReturn(values).when(service).getValue(any(), any(), anyBoolean(), any());

        testRunner.run();

        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(GetOPCData.SUCCESS);
        assertEquals(1, results.size());
        results.get(0).assertContentEquals("123456,1,2,3,4,5,6");
        results.get(0).assertAttributeEquals("csvHeader",
                            "timestamp," +
                                    "ns=4;s=S71500/ET200MP-Station_2.PLC_1.GlobalVars.I_MAG1_EXT," +
                                    "ns=4;s=S71500/ET200MP-Station_2.PLC_1.GlobalVars.I_MAG1_RET," +
                                    "ns=4;s=S71500/ET200MP-Station_2.PLC_1.GlobalVars.I_MAG2_EXT," +
                                    "ns=4;s=S71500/ET200MP-Station_2.PLC_1.GlobalVars.I_MAG3_RET," +
                                    "ns=4;s=S71500/ET200MP-Station_2.PLC_1.GlobalVars.I_MAG3_EXT," +
                                    "ns=4;s=S71500/ET200MP-Station_2.PLC_1.GlobalVars.I_MAG3_RET");

    }

    @After
    public void shutdown() {
        testRunner.disableControllerService(service);
    }

}
