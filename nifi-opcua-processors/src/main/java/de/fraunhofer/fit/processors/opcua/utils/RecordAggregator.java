package de.fraunhofer.fit.processors.opcua.utils;

import java.util.*;

public class RecordAggregator {

    // This variable indicates how long a record waits for notification messages from OPC server
    private final long PUBLISH_THRESHOLD_TIME = 1000;

    // Index of elements in the queue message
    private final int VARIABLE_ID_INDEX = 0;
    private final int SOURCE_TS_INDEX = 2;
    private final int VALUE_INDEX = 3;
    private final int STATUS_CODE_INDEX = 4;

    private List<String> tags;
    private Map<String, Integer> tagOrderMap;
    private Map<String, Record> recordMap;
    private SortedMap<String, Record> readyRecordMap;

    public RecordAggregator(List<String> tags) {

        this.tags = tags;
        this.tagOrderMap = new HashMap<>();
        this.recordMap = new HashMap<>();
        this.readyRecordMap = new TreeMap<>((s1, s2) -> (int)(Long.parseLong(s1) - Long.parseLong(s2)));

        // Create a map which with tag name as key and its order in list as value
        for (int i = 0; i < tags.size(); i++) {
            this.tagOrderMap.put(tags.get(i), i);
        }

    }

    public void aggregate(String rawMsg) {


            // msg has the following elements in order:
            // 1. variable ID; 2. server time stamp; 3. source time stamp; 4. value; 5. status code
            String[] msg = rawMsg.trim().split(",");

            // Ditch all messages with bad status code
            if (!msg[STATUS_CODE_INDEX].equals("0")) {
                return;
            }

            String timeStamp = msg[SOURCE_TS_INDEX];
            String variableId = msg[VARIABLE_ID_INDEX];
            String value = msg[VALUE_INDEX];

            // Check if a record is already exist for the given time stamp
            Record rec;
            if (recordMap.containsKey(timeStamp)) {
                // Get the record with the same time stamp as the message
                rec = recordMap.get(timeStamp);
            } else {
                // Create a record
                rec = new Record(timeStamp, tags.size());
                // Insert it into the map
                recordMap.put(timeStamp, rec);
            }
            // Get the index of the variable given in the message
            if (!tagOrderMap.containsKey(variableId)) {
                return;
            }
            int index = tagOrderMap.get(variableId);
            // Update the value in the record array
            rec.getRecordArray()[index] = value;
        }


    public List<String> getReadyRecords() {
        // Move old record to another list, ready for publish
        long currentTime = System.currentTimeMillis();
        Iterator<Map.Entry<String, Record>> iterator = recordMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Record> entry = iterator.next();
            if (currentTime - entry.getValue().getCreatedTime() > PUBLISH_THRESHOLD_TIME) {
                readyRecordMap.put(entry.getKey(), entry.getValue());
                iterator.remove();
            }
        }

        List<String> list = new ArrayList<>();
        for (Map.Entry<String, Record> entry : readyRecordMap.entrySet()) {
            list.add(entry.getKey() + "," +
                    String.join(",", entry.getValue().getRecordArray()) +
                    System.getProperty("line.separator"));
        }

        return list;
    }

    public void clearReadyRecord() {
        readyRecordMap.clear();
    }

    // Method for debugging
    public void printRecord() {
        System.out.println("Record waiting:");
        for (Map.Entry<String, Record> entry : recordMap.entrySet()) {
            System.out.println(entry.getKey() + "-" + Arrays.toString(entry.getValue().getRecordArray()));
        }

        System.out.println("Record ready:");
        for (Map.Entry<String, Record> entry : readyRecordMap.entrySet()) {
            System.out.println(entry.getKey() + "-" + Arrays.toString(entry.getValue().getRecordArray()));
        }
    }

    class Record {

        private String timeStamp;
        private long createdTime; // The createdTime is used to see whether a record is ready to be published
        private String[] recordValues;

        Record(String timeStamp, int recordSize) {
            this.timeStamp = timeStamp;
            createdTime = System.currentTimeMillis();
            recordValues = new String[recordSize];
            Arrays.fill(recordValues, "");
        }

        String[] getRecordArray() {
            return recordValues;
        }

        long getCreatedTime() {
            return createdTime;
        }

        String getTimeStamp() {
            return timeStamp;
        }

    }

}
