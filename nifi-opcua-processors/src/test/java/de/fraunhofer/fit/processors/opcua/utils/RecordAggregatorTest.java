package de.fraunhofer.fit.processors.opcua.utils;

import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class RecordAggregatorTest {

    private BlockingQueue<String> queue;
    private List<String> tags;
    private RecordAggregator ra;


    @Before
    public void init() throws IOException {
        queue = new LinkedBlockingQueue<>();
        String tagFilePath = (new File("src\\test\\resources\\husky_tags.txt")).getAbsolutePath();
        tags = parseFile(Paths.get(tagFilePath));
        ra = new RecordAggregator(queue, tags);
    }

    @Test
    public void testUniformTimeStampNoWait() {

        String queueString = "ns=2;s=47.ProcessVariables.Shot_Length,1528285608582,1528285608582,38.71,0\n" +
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
        populateQueue(queue, queueString);
        ra.aggregate();

        assertEquals(0, ra.getReadyRecords().size());
    }

    @Test
    public void testUniformTimeStampWithWait() throws InterruptedException {

        String queueString = "ns=2;s=47.ProcessVariables.Shot_Length,1528285608582,1528285608582,38.71,0\n" +
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
        populateQueue(queue, queueString);
        ra.aggregate();

        Thread.sleep(1500);
        ra.aggregate();

        for(String s : ra.getReadyRecords()) {
            System.out.println(s);
        }

        assertEquals(1, ra.getReadyRecords().size());
    }

    @Test
    public void testUniformTimeStampTwoTimesAddWithWait() throws InterruptedException {

        String queueString = "ns=2;s=47.ProcessVariables.Shot_Length,1528285608582,1528285608582,38.71,0\n" +
                "ns=2;s=47.ProcessVariables.Shot_Size,1528285608582,1528285608582,42.543697,0\n" +
                "ns=2;s=47.ProcessVariables.Transition_Position,1528285608582,1528285608582,10.848699,0\n" +
                "ns=2;s=47.ProcessVariables.Mold_Open_Time,1528285608582,1528285608582,0.20775,0\n" +
                "ns=2;s=47.ProcessVariables.Maximum_Fill_Pressure,1528285608582,1528285608582,33.245487,0\n" +
                "ns=2;s=47.CycleCounter,1528285608582,1528285608582,2419756,0\n";

        populateQueue(queue, queueString);
        ra.aggregate();

        queueString = "ns=2;s=47.ProcessVariables.Tonnage,1528285608582,1528285608582,150.13992,0\n" +
                "ns=2;s=47.ProcessVariables.Transition_Pressure,1528285608582,1528285608582,32.43998,0\n" +
                "ns=2;s=47.ProcessVariables.Back_Pressure,1528285608582,1528285608582,4.540133,0\n" +
                "ns=2;s=47.ProcessVariables.Screw_Run_Time,1528285608582,1528285608582,0.948,0\n" +
                "ns=2;s=47.ProcessVariables.Oil_Temperature,1528285608582,1528285608582,50.000004,0\n" +
                "ns=2;s=47.ProcessVariables.Ejector_Maximum_Forward_Position,1528285608582,1528285608582,11.0207815,0\n" +
                "ns=2;s=47.ProcessVariables.Mold_Growth,1528285608582,1528285608582,-0.07940674,0\n" +
                "ns=2;s=47.ProcessVariables.Screw_RPM,1528285608582,1528285608582,350.05255,0";

        populateQueue(queue, queueString);
        ra.aggregate();

        Thread.sleep(1500);
        ra.aggregate();

        assertEquals(1, ra.getReadyRecords().size());
    }

    @Test
    public void testMutipleTimeStampsNoWait() {

        String queueString = "ns=2;s=47.SerialNumber,1528285396720,1526914447403,7691403,0\n" +
                "ns=2;s=47.Description,1528285396720,1526914447403,H300-RS65/60,0\n" +
                "ns=2;s=47.MachineState,1528285396720,1528257014806,Auto Cycling,0\n" +
                "ns=2;s=47.CycleInterruption,1528285396720,1528257014806,,0\n" +
                "ns=2;s=47.CycleCounter,1528285396720,1528285390205,2419724,0\n" +
                "ns=2;s=47.CycleCounter,1528285397032,1528285397032,2419725,0\n" +
                "ns=2;s=47.ProcessVariables.Cycle_Time,1528285396720,1528285390205,6.82,0\n" +
                "ns=2;s=47.ProcessVariables.Mold_Closing_Time,1528285396720,1528285390205,0.944,0\n" +
                "ns=2;s=47.ProcessVariables.Tonnage,1528285396720,1528285390205,150.00896,0\n" +
                "ns=2;s=47.ProcessVariables.Tonnage,1528285397032,1528285397032,149.96933,0\n" +
                "ns=2;s=47.ProcessVariables.Shot_Size,1528285396720,1528285390205,42.598698,0\n" +
                "ns=2;s=47.ProcessVariables.Shot_Size,1528285397032,1528285397032,42.6787,0\n" +
                "ns=2;s=47.ProcessVariables.Cushion,1528285396720,1528285390205,3.8786993,0\n" +
                "ns=2;s=47.ProcessVariables.Cushion,1528285397032,1528285397032,3.9336991,0\n" +
                "ns=2;s=47.ProcessVariables.Shot_Length,1528285396720,1528285390205,38.719997,0\n" +
                "ns=2;s=47.ProcessVariables.Shot_Length,1528285397032,1528285397032,38.745,0\n" +
                "ns=2;s=47.ProcessVariables.Transition_Position,1528285396720,1528285390205,10.883699,0\n" +
                "ns=2;s=47.ProcessVariables.Transition_Position,1528285397032,1528285397032,10.963699,0\n" +
                "ns=2;s=47.ProcessVariables.Transition_Pressure,1528285396720,1528285390205,32.73289,0\n" +
                "ns=2;s=47.ProcessVariables.Transition_Pressure,1528285397032,1528285397032,32.513206,0\n" +
                "ns=2;s=47.ProcessVariables.Maximum_Fill_Pressure,1528285396720,1528285390205,33.172256,0\n" +
                "ns=2;s=47.ProcessVariables.Hold_Pressure_Zone_-_1,1528285396720,1528285390205,21.529015,0\n" +
                "ns=2;s=47.ProcessVariables.Hold_Pressure_Zone_-_2,1528285396720,1528285390205,18.52667,0\n" +
                "ns=2;s=47.ProcessVariables.Hold_Pressure_Zone_-_2,1528285397032,1528285397032,18.746353,0\n" +
                "ns=2;s=47.ProcessVariables.Hold_Pressure_Zone_-_3,1528285396720,1528285390205,0.0,0\n" +
                "ns=2;s=47.ProcessVariables.Hold_Pressure_Zone_-_4,1528285396720,1528285390205,0.0,0\n" +
                "ns=2;s=47.ProcessVariables.Hold_Pressure_Zone_-_10,1528285396720,1528285390205,0.0,0\n" +
                "ns=2;s=47.ProcessVariables.Back_Pressure,1528285396720,1528285390205,4.759816,0\n" +
                "ns=2;s=47.ProcessVariables.Back_Pressure,1528285397032,1528285397032,4.833044,0\n" +
                "ns=2;s=47.ProcessVariables.Screw_Run_Time,1528285396720,1528285390205,0.952,0\n" +
                "ns=2;s=47.ProcessVariables.Screw_Run_Time,1528285397032,1528285397032,0.932,0\n" +
                "ns=2;s=47.ProcessVariables.Mold_Opening_Time,1528285396720,1528285390205,1.12,0\n" +
                "ns=2;s=47.ProcessVariables.Ejector_Forward_Time,1528285396720,1528285390205,0.132,0\n" +
                "ns=2;s=47.ProcessVariables.Oil_Temperature,1528285396720,1528285390205,50.09346,0\n" +
                "ns=2;s=47.ProcessVariables.Oil_Temperature,1528285397032,1528285397032,50.062447,0\n" +
                "ns=2;s=47.ProcessVariables.Extruder_Temperature_-_1,1528285396720,1528285390205,195.0,0\n" +
                "ns=2;s=47.ProcessVariables.Extruder_Temperature_-_2,1528285396720,1528285390205,195.0,0\n" +
                "ns=2;s=47.ProcessVariables.Extruder_Temperature_-_3,1528285396720,1528285390205,200.0,0\n" +
                "ns=2;s=47.ProcessVariables.Barrel_Head_Temperature,1528285396720,1528285390205,210.0,0\n" +
                "ns=2;s=47.ProcessVariables.Ejector_Back_Time,1528285396720,1528285390205,0.296,0\n" +
                "ns=2;s=47.ProcessVariables.Ejector_Maximum_Forward_Position,1528285396720,1528285390205,11.003781,0\n" +
                "ns=2;s=47.ProcessVariables.Ejector_Maximum_Forward_Position,1528285397032,1528285397032,11.036781,0\n" +
                "ns=2;s=47.ProcessVariables.Maximum_Cavity_Pressure,1528285396720,1528285390205,0.0,0\n" +
                "ns=2;s=47.ProcessVariables.Cavity_Pressure_At_Transition,1528285396720,1528285390205,0.0,0\n" +
                "ns=2;s=47.ProcessVariables.Mold_Growth,1528285396720,1528285390205,-0.0803833,0\n" +
                "ns=2;s=47.ProcessVariables.Mold_Growth,1528285397032,1528285397032,-0.07940674,0\n" +
                "ns=2;s=47.ProcessVariables.Mold_Open_Time,1528285396720,1528285390205,0.208,0\n" +
                "ns=2;s=47.ProcessVariables.Mold_Open_Time,1528285397032,1528285397032,0.207,0\n" +
                "ns=2;s=47.ProcessVariables.Nozzle_Adapter_Temperature,1528285396720,1528285390205,220.1,0\n" +
                "ns=2;s=47.ProcessVariables.Nozzle_Adapter_Temperature,1528285397032,1528285397032,220.0,0\n" +
                "ns=2;s=47.ProcessVariables.Nozzle_Shutoff_Temperature,1528285396720,1528285390205,215.0,0\n" +
                "ns=2;s=47.ProcessVariables.Screw_RPM,1528285396720,1528285390205,350.12796,0\n" +
                "ns=2;s=47.ProcessVariables.Screw_RPM,1528285397032,1528285397032,349.8606,0\n" +
                "ns=2;s=47.ProcessVariables.Hold_Pressure_Zone_-_5,1528285396720,1528285390205,0.0,0\n" +
                "ns=2;s=47.ProcessVariables.Hold_Pressure_Zone_-_6,1528285396720,1528285390205,0.0,0\n" +
                "ns=2;s=47.ProcessVariables.Hold_Pressure_Zone_-_7,1528285396720,1528285390205,0.0,0\n" +
                "ns=2;s=47.ProcessVariables.Hold_Pressure_Zone_-_8,1528285396720,1528285390205,0.0,0\n" +
                "ns=2;s=47.ProcessVariables.Hold_Pressure_Zone_-_9,1528285396720,1528285390205,0.0,0\n" +
                "ns=2;s=47.ProcessVariables.Extruder_Temperature_-_4,1528285396720,1528285390205,205.0,0\n" +
                "ns=2;s=47.ProcessVariables.Cooling_Time,1528285396720,1528285390205,2.308,0\n" +
                "ns=2;s=47.ProcessVariables.Injection_Hold_Time,1528285396720,1528285390205,1.702,0";
        populateQueue(queue, queueString);
        ra.aggregate();

        assertEquals(0, ra.getReadyRecords().size());
    }


    @Test
    public void testMutipleTimeStampsWithWait() throws InterruptedException {

        String queueString =
                "ns=2;s=47.CycleCounter,1528285396720,1528285390205,2419724,0\n" +
                "ns=2;s=47.CycleCounter,1528285397032,1528285397032,2419725,0\n" +
                "ns=2;s=47.ProcessVariables.Cycle_Time,1528285396720,1528285390205,6.82,0\n" +
                "ns=2;s=47.ProcessVariables.Mold_Closing_Time,1528285396720,1528285390205,0.944,0\n" +
                "ns=2;s=47.ProcessVariables.Tonnage,1528285396720,1528285390205,150.00896,0\n" +
                "ns=2;s=47.ProcessVariables.Tonnage,1528285397032,1528285397032,149.96933,0\n" +
                "ns=2;s=47.ProcessVariables.Shot_Size,1528285396720,1528285390205,42.598698,0\n" +
                "ns=2;s=47.ProcessVariables.Shot_Size,1528285397032,1528285397032,42.6787,0\n" +
                "ns=2;s=47.ProcessVariables.Cushion,1528285396720,1528285390205,3.8786993,0\n" +
                "ns=2;s=47.ProcessVariables.Cushion,1528285397032,1528285397032,3.9336991,0\n" +
                "ns=2;s=47.ProcessVariables.Shot_Length,1528285396720,1528285390205,38.719997,0\n" +
                "ns=2;s=47.ProcessVariables.Shot_Length,1528285397032,1528285397032,38.745,0\n" +
                "ns=2;s=47.ProcessVariables.Transition_Position,1528285396720,1528285390205,10.883699,0\n" +
                "ns=2;s=47.ProcessVariables.Transition_Position,1528285397032,1528285397032,10.963699,0\n" +
                "ns=2;s=47.ProcessVariables.Transition_Pressure,1528285396720,1528285390205,32.73289,0\n" +
                "ns=2;s=47.ProcessVariables.Transition_Pressure,1528285397032,1528285397032,32.513206,0\n" +
                "ns=2;s=47.ProcessVariables.Maximum_Fill_Pressure,1528285396720,1528285390205,33.172256,0\n" +
                "ns=2;s=47.ProcessVariables.Hold_Pressure_Zone_-_1,1528285396720,1528285390205,21.529015,0\n" +
                "ns=2;s=47.ProcessVariables.Hold_Pressure_Zone_-_2,1528285396720,1528285390205,18.52667,0\n" +
                "ns=2;s=47.ProcessVariables.Hold_Pressure_Zone_-_2,1528285397032,1528285397032,18.746353,0\n" +
                "ns=2;s=47.ProcessVariables.Hold_Pressure_Zone_-_3,1528285396720,1528285390205,0.0,0\n" +
                "ns=2;s=47.ProcessVariables.Hold_Pressure_Zone_-_4,1528285396720,1528285390205,0.0,0\n" +
                "ns=2;s=47.ProcessVariables.Hold_Pressure_Zone_-_10,1528285396720,1528285390205,0.0,0\n" +
                "ns=2;s=47.ProcessVariables.Back_Pressure,1528285396720,1528285390205,4.759816,0\n" +
                "ns=2;s=47.ProcessVariables.Back_Pressure,1528285397032,1528285397032,4.833044,0\n" +
                "ns=2;s=47.ProcessVariables.Screw_Run_Time,1528285396720,1528285390205,0.952,0\n" +
                "ns=2;s=47.ProcessVariables.Screw_Run_Time,1528285397032,1528285397032,0.932,0\n" +
                "ns=2;s=47.ProcessVariables.Mold_Opening_Time,1528285396720,1528285390205,1.12,0\n" +
                "ns=2;s=47.ProcessVariables.Ejector_Forward_Time,1528285396720,1528285390205,0.132,0\n" +
                "ns=2;s=47.ProcessVariables.Oil_Temperature,1528285396720,1528285390205,50.09346,0\n" +
                "ns=2;s=47.ProcessVariables.Oil_Temperature,1528285397032,1528285397032,50.062447,0\n" +
                "ns=2;s=47.ProcessVariables.Extruder_Temperature_-_1,1528285396720,1528285390205,195.0,0\n" +
                "ns=2;s=47.ProcessVariables.Extruder_Temperature_-_2,1528285396720,1528285390205,195.0,0\n" +
                "ns=2;s=47.ProcessVariables.Extruder_Temperature_-_3,1528285396720,1528285390205,200.0,0\n" +
                "ns=2;s=47.ProcessVariables.Barrel_Head_Temperature,1528285396720,1528285390205,210.0,0\n" +
                "ns=2;s=47.ProcessVariables.Ejector_Back_Time,1528285396720,1528285390205,0.296,0\n" +
                "ns=2;s=47.ProcessVariables.Ejector_Maximum_Forward_Position,1528285396720,1528285390205,11.003781,0\n" +
                "ns=2;s=47.ProcessVariables.Ejector_Maximum_Forward_Position,1528285397032,1528285397032,11.036781,0\n" +
                "ns=2;s=47.ProcessVariables.Maximum_Cavity_Pressure,1528285396720,1528285390205,0.0,0\n" +
                "ns=2;s=47.ProcessVariables.Cavity_Pressure_At_Transition,1528285396720,1528285390205,0.0,0\n" +
                "ns=2;s=47.ProcessVariables.Mold_Growth,1528285396720,1528285390205,-0.0803833,0\n" +
                "ns=2;s=47.ProcessVariables.Mold_Growth,1528285397032,1528285397032,-0.07940674,0\n" +
                "ns=2;s=47.ProcessVariables.Mold_Open_Time,1528285396720,1528285390205,0.208,0\n" +
                "ns=2;s=47.ProcessVariables.Mold_Open_Time,1528285397032,1528285397032,0.207,0\n" +
                "ns=2;s=47.ProcessVariables.Nozzle_Adapter_Temperature,1528285396720,1528285390205,220.1,0\n" +
                "ns=2;s=47.ProcessVariables.Nozzle_Adapter_Temperature,1528285397032,1528285397032,220.0,0\n" +
                "ns=2;s=47.ProcessVariables.Nozzle_Shutoff_Temperature,1528285396720,1528285390205,215.0,0\n" +
                "ns=2;s=47.ProcessVariables.Screw_RPM,1528285396720,1528285390205,350.12796,0\n" +
                "ns=2;s=47.ProcessVariables.Screw_RPM,1528285397032,1528285397032,349.8606,0\n" +
                "ns=2;s=47.ProcessVariables.Hold_Pressure_Zone_-_5,1528285396720,1528285390205,0.0,0\n" +
                "ns=2;s=47.ProcessVariables.Hold_Pressure_Zone_-_6,1528285396720,1528285390205,0.0,0\n" +
                "ns=2;s=47.ProcessVariables.Hold_Pressure_Zone_-_7,1528285396720,1528285390205,0.0,0\n" +
                "ns=2;s=47.ProcessVariables.Hold_Pressure_Zone_-_8,1528285396720,1528285390205,0.0,0\n" +
                "ns=2;s=47.ProcessVariables.Hold_Pressure_Zone_-_9,1528285396720,1528285390205,0.0,0\n" +
                "ns=2;s=47.ProcessVariables.Extruder_Temperature_-_4,1528285396720,1528285390205,205.0,0\n" +
                "ns=2;s=47.ProcessVariables.Cooling_Time,1528285396720,1528285390205,2.308,0\n" +
                "ns=2;s=47.ProcessVariables.Injection_Hold_Time,1528285396720,1528285390205,1.702,0";
        populateQueue(queue, queueString);
        ra.aggregate();

        Thread.sleep(1500);
        ra.aggregate();

        assertEquals(2, ra.getReadyRecords().size());
    }

    @Test
    public void testMutipleTimeStampsTwoTimeAddWithWait() throws InterruptedException {

        String queueString =
                "ns=2;s=47.CycleCounter,1528285396720,1528285390205,2419724,0\n" +
                "ns=2;s=47.CycleCounter,1528285397032,1528285397032,2419725,0\n" +
                "ns=2;s=47.ProcessVariables.Cycle_Time,1528285396720,1528285390205,6.82,0\n" +
                "ns=2;s=47.ProcessVariables.Mold_Closing_Time,1528285396720,1528285390205,0.944,0\n" +
                "ns=2;s=47.ProcessVariables.Tonnage,1528285396720,1528285390205,150.00896,0\n" +
                "ns=2;s=47.ProcessVariables.Tonnage,1528285397032,1528285397032,149.96933,0\n" +
                "ns=2;s=47.ProcessVariables.Shot_Size,1528285396720,1528285390205,42.598698,0\n" +
                "ns=2;s=47.ProcessVariables.Shot_Size,1528285397032,1528285397032,42.6787,0\n" +
                "ns=2;s=47.ProcessVariables.Cushion,1528285396720,1528285390205,3.8786993,0\n" +
                "ns=2;s=47.ProcessVariables.Cushion,1528285397032,1528285397032,3.9336991,0\n" +
                "ns=2;s=47.ProcessVariables.Shot_Length,1528285396720,1528285390205,38.719997,0\n" +
                "ns=2;s=47.ProcessVariables.Shot_Length,1528285397032,1528285397032,38.745,0\n" +
                "ns=2;s=47.ProcessVariables.Transition_Position,1528285396720,1528285390205,10.883699,0\n" +
                "ns=2;s=47.ProcessVariables.Transition_Position,1528285397032,1528285397032,10.963699,0\n" +
                "ns=2;s=47.ProcessVariables.Transition_Pressure,1528285396720,1528285390205,32.73289,0\n" +
                "ns=2;s=47.ProcessVariables.Transition_Pressure,1528285397032,1528285397032,32.513206,0\n" +
                "ns=2;s=47.ProcessVariables.Maximum_Fill_Pressure,1528285396720,1528285390205,33.172256,0\n" +
                "ns=2;s=47.ProcessVariables.Hold_Pressure_Zone_-_1,1528285396720,1528285390205,21.529015,0\n" +
                "ns=2;s=47.ProcessVariables.Hold_Pressure_Zone_-_2,1528285396720,1528285390205,18.52667,0\n" +
                "ns=2;s=47.ProcessVariables.Hold_Pressure_Zone_-_2,1528285397032,1528285397032,18.746353,0\n" +
                "ns=2;s=47.ProcessVariables.Hold_Pressure_Zone_-_3,1528285396720,1528285390205,0.0,0\n" +
                "ns=2;s=47.ProcessVariables.Hold_Pressure_Zone_-_4,1528285396720,1528285390205,0.0,0\n" +
                "ns=2;s=47.ProcessVariables.Hold_Pressure_Zone_-_10,1528285396720,1528285390205,0.0,0\n";

        populateQueue(queue, queueString);
        ra.aggregate();

        queueString = "ns=2;s=47.ProcessVariables.Back_Pressure,1528285396720,1528285390205,4.759816,0\n" +
                "ns=2;s=47.ProcessVariables.Back_Pressure,1528285397032,1528285397032,4.833044,0\n" +
                "ns=2;s=47.ProcessVariables.Screw_Run_Time,1528285396720,1528285390205,0.952,0\n" +
                "ns=2;s=47.ProcessVariables.Screw_Run_Time,1528285397032,1528285397032,0.932,0\n" +
                "ns=2;s=47.ProcessVariables.Mold_Opening_Time,1528285396720,1528285390205,1.12,0\n" +
                "ns=2;s=47.ProcessVariables.Ejector_Forward_Time,1528285396720,1528285390205,0.132,0\n" +
                "ns=2;s=47.ProcessVariables.Oil_Temperature,1528285396720,1528285390205,50.09346,0\n" +
                "ns=2;s=47.ProcessVariables.Oil_Temperature,1528285397032,1528285397032,50.062447,0\n" +
                "ns=2;s=47.ProcessVariables.Extruder_Temperature_-_1,1528285396720,1528285390205,195.0,0\n" +
                "ns=2;s=47.ProcessVariables.Extruder_Temperature_-_2,1528285396720,1528285390205,195.0,0\n" +
                "ns=2;s=47.ProcessVariables.Extruder_Temperature_-_3,1528285396720,1528285390205,200.0,0\n" +
                "ns=2;s=47.ProcessVariables.Barrel_Head_Temperature,1528285396720,1528285390205,210.0,0\n" +
                "ns=2;s=47.ProcessVariables.Ejector_Back_Time,1528285396720,1528285390205,0.296,0\n" +
                "ns=2;s=47.ProcessVariables.Ejector_Maximum_Forward_Position,1528285396720,1528285390205,11.003781,0\n" +
                "ns=2;s=47.ProcessVariables.Ejector_Maximum_Forward_Position,1528285397032,1528285397032,11.036781,0\n" +
                "ns=2;s=47.ProcessVariables.Maximum_Cavity_Pressure,1528285396720,1528285390205,0.0,0\n" +
                "ns=2;s=47.ProcessVariables.Cavity_Pressure_At_Transition,1528285396720,1528285390205,0.0,0\n" +
                "ns=2;s=47.ProcessVariables.Mold_Growth,1528285396720,1528285390205,-0.0803833,0\n" +
                "ns=2;s=47.ProcessVariables.Mold_Growth,1528285397032,1528285397032,-0.07940674,0\n" +
                "ns=2;s=47.ProcessVariables.Mold_Open_Time,1528285396720,1528285390205,0.208,0\n" +
                "ns=2;s=47.ProcessVariables.Mold_Open_Time,1528285397032,1528285397032,0.207,0\n" +
                "ns=2;s=47.ProcessVariables.Nozzle_Adapter_Temperature,1528285396720,1528285390205,220.1,0\n" +
                "ns=2;s=47.ProcessVariables.Nozzle_Adapter_Temperature,1528285397032,1528285397032,220.0,0\n" +
                "ns=2;s=47.ProcessVariables.Nozzle_Shutoff_Temperature,1528285396720,1528285390205,215.0,0\n" +
                "ns=2;s=47.ProcessVariables.Screw_RPM,1528285396720,1528285390205,350.12796,0\n" +
                "ns=2;s=47.ProcessVariables.Screw_RPM,1528285397032,1528285397032,349.8606,0\n" +
                "ns=2;s=47.ProcessVariables.Hold_Pressure_Zone_-_5,1528285396720,1528285390205,0.0,0\n" +
                "ns=2;s=47.ProcessVariables.Hold_Pressure_Zone_-_6,1528285396720,1528285390205,0.0,0\n" +
                "ns=2;s=47.ProcessVariables.Hold_Pressure_Zone_-_7,1528285396720,1528285390205,0.0,0\n" +
                "ns=2;s=47.ProcessVariables.Hold_Pressure_Zone_-_8,1528285396720,1528285390205,0.0,0\n" +
                "ns=2;s=47.ProcessVariables.Hold_Pressure_Zone_-_9,1528285396720,1528285390205,0.0,0\n" +
                "ns=2;s=47.ProcessVariables.Extruder_Temperature_-_4,1528285396720,1528285390205,205.0,0\n" +
                "ns=2;s=47.ProcessVariables.Cooling_Time,1528285396720,1528285390205,2.308,0\n" +
                "ns=2;s=47.ProcessVariables.Injection_Hold_Time,1528285396720,1528285390205,1.702,0";
        populateQueue(queue, queueString);
        ra.aggregate();

        Thread.sleep(1500);
        ra.aggregate();

        assertEquals(2, ra.getReadyRecords().size());

        ra.clearReadyRecord();
        assertEquals(0, ra.getReadyRecords().size());


    }


    private List<String> parseFile(Path filePath) throws IOException {
        byte[] encoded;
        encoded = Files.readAllBytes(filePath);
        String fileContent = new String(encoded, Charset.defaultCharset());
        return new BufferedReader(new StringReader(fileContent)).lines().collect(Collectors.toList());
    }

    private void populateQueue(BlockingQueue<String> queue, String str) {
        String[] msgs = str.split("\n");
        for(int i=0; i<msgs.length; i++) {
            queue.offer(msgs[i] + '\n');
        }
    }

}
