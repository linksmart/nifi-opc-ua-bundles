package de.fraunhofer.fit.processors.opcua.utils;

import java.util.*;

public class RecordAggregator {

    // This variable indicates how long a record waits for notification messages from OPC server
    private final int PUBLISH_INTERVAL_MULTIPLIER = 4;

    // Index of elements in the queue message
    private final int VARIABLE_ID_INDEX = 0;
    private final int SOURCE_TS_INDEX = 2;
    private final int VALUE_INDEX = 3;
    private final int STATUS_CODE_INDEX = 4;

    private long PUBLISH_THRESHOLD_TIME;
    private List<String> tags;
    private Map<String, Integer> tagOrderMap;
    private Map<String, Record> recordMap;

    // minPublishInterval is the minimum subscription notification publish interval from OPC UA server
    public RecordAggregator(List<String> tags, long minPublishInterval) {

        this.tags = tags;
        this.tagOrderMap = new HashMap<>();
        this.recordMap = new HashMap<>();

        PUBLISH_THRESHOLD_TIME = PUBLISH_INTERVAL_MULTIPLIER * minPublishInterval;

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

        List<String> list = new ArrayList<>();
        List<String> recordKeyList = new ArrayList<>(recordMap.keySet());
        Collections.sort(recordKeyList);

        for(String key : recordKeyList) {
            Record rec = recordMap.get(key);
            if(rec.isReady(PUBLISH_THRESHOLD_TIME)) {
                list.add(key + "," +
                        String.join(",", rec.getRecordArray()) +
                        System.getProperty("line.separator"));
                recordMap.remove(key);
            }
        }

        return list;
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

        String getTimeStamp() {
            return timeStamp;
        }

        boolean isReady(long timeThrashold) {
            long currentTime = System.currentTimeMillis();

            return (currentTime - createdTime) > timeThrashold;
        }

    }

}
