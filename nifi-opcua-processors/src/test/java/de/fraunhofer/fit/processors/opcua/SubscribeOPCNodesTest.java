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
import org.mockito.stubbing.Answer;

import java.io.File;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.spy;


public class SubscribeOPCNodesTest {

    private TestRunner testRunner;
    private StandardOPCUAService service;

    @Before
    public void init() throws InitializationException {
        testRunner = TestRunners.newTestRunner(SubscribeOPCNodes.class);
        // Use partial mock
        service = spy(new StandardOPCUAService());

        Mockito.doNothing().when(service).unsubscribe(anyString());
        Mockito.doNothing().when(service).onEnabled(any());
        Mockito.doNothing().when(service).shutdown();

        testRunner.addControllerService("controller", service);

        testRunner.setProperty(service, StandardOPCUAService.ENDPOINT, "dummy endpoint");
        testRunner.assertValid(service);

        testRunner.enableControllerService(service);
    }


    @Test
    public void testAggregateRecords() throws Exception {

        String tagFilePath = (new File("src\\test\\resources\\husky_tags.txt")).getAbsolutePath();

        String queueString =
                "ns=2;s=47.ProcessVariables.Shot_Length,1528285608582,1528285608582,38.71,0\n" +
                        "ns=2;s=47.ProcessVariables.Shot_Size,1528285608582,1528285608582,42.543697,0\n" +
                        "ns=2;s=47.ProcessVariables.Transition_Position,1528285608582,1528285608582,10.848699,0\n" +
                        "ns=2;s=47.ProcessVariables.Mold_Open_Time,1528285608582,1528285608582,0.20775,0\n" +
                        "ns=2;s=47.ProcessVariables.Maximum_Fill_Pressure,1528285608582,1528285608582,33.245487,0\n" +
                        "ns=2;s=47.CycleCounter,1528285608582,1528285608582,2419756,0\n" +
                        "ns=2;s=47.ProcessVariables.Tonnage,1528285608582,1528285608582,150.13992,0\n" +
                        "ns=2;s=47.ProcessVariables.Transition_Pressure,1528285608582,1528285608582,32.43998,0\n" +
                        "ns=2;s=47.ProcessVariables.Back_Pressure,1528285608582,1528285608582,4.540133,0\n" +
                        "ns=2;s=47.ProcessVariables.Screw_Run_Time,1528285608582,1528285608582,0.948,0\n" +
                        "ns=2;s=47.ProcessVariables.Oil_Temperature,1528285608582,1528285608582,50.000004,0\n" +
                        "ns=2;s=47.ProcessVariables.Ejector_Maximum_Forward_Position,1528285608582,1528285608582,11.0207815,0\n" +
                        "ns=2;s=47.ProcessVariables.Mold_Growth,1528285608582,1528285608582,-0.07940674,0\n" +
                        "ns=2;s=47.ProcessVariables.Screw_RPM,1528285608582,1528285608582,350.05255,0";


        Mockito.doAnswer(
                (Answer<String>) invocation -> {
                    Object[] args = invocation.getArguments();
                    populateQueue((BlockingQueue<String>) args[1], queueString);
                    return "12345678"; // random subscriber uid, doesn't matter in test
                }
        ).when(service).subscribe(any(), any(), anyBoolean());


        testRunner.setProperty(SubscribeOPCNodes.OPCUA_SERVICE, "controller");
        testRunner.setProperty(SubscribeOPCNodes.TAG_FILE_LOCATION, tagFilePath);
        testRunner.setProperty(SubscribeOPCNodes.AGGREGATE_RECORD, "true");

        testRunner.run(1, false, true);
        Thread.sleep(1500);
        testRunner.run(1, true, false);


        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(GetOPCData.SUCCESS);
        assertEquals(1, results.size());
        String expectedPayload = "1528285608582,,,,,2419756,,,150.13992,42.543697,,38.71,,10.848699,32.43998,33.245487,,,,,,4.540133,0.948,,,,50.000004,,,,,,11.0207815,,,-0.07940674,0.20775,,,350.05255,,,,,,,,,,,," + System.lineSeparator();
        results.get(0).assertContentEquals(expectedPayload);

    }

    @After
    public void shutdown() {
        testRunner.disableControllerService(service);
    }


    private void populateQueue(BlockingQueue<String> queue, String str) {
        String[] msgs = str.split("\n");
        for (int i = 0; i < msgs.length; i++) {
            queue.offer(msgs[i] + '\n');
        }
    }

}
