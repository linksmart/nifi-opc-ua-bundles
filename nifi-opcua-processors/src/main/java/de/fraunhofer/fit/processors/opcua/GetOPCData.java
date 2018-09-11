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

import de.fraunhofer.fit.opcua.OPCUAService;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Tags({"opc"})
@CapabilityDescription("Get the data of specified nodes from a OPC UA server.")
public class GetOPCData extends AbstractProcessor {

    private final AtomicReference<String> timestamp = new AtomicReference<>();
    private final AtomicBoolean excludeNullValue = new AtomicBoolean();
    private String nullValueString = "";

    private List<String> tagList;

    public static final PropertyDescriptor OPCUA_SERVICE = new PropertyDescriptor.Builder()
            .name("OPC UA Service")
            .description("Specifies the OPC UA Service that can be used to access data")
            .required(true)
            .identifiesControllerService(OPCUAService.class)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor RETURN_TIMESTAMP = new PropertyDescriptor
            .Builder().name("Return Timestamp")
            .description("Allows to select the source, server, or both timestamps")
            .required(true)
            .sensitive(false)
            .allowableValues("SourceTimestamp", "ServerTimestamp", "Both")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TAG_LIST_SOURCE = new PropertyDescriptor
            .Builder().name("Tag List Source")
            .description("Either get the tag list from the flow file, or from a dynamic property")
            .required(true)
            .allowableValues("Flowfile", "Local File")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor TAG_LIST_FILE = new PropertyDescriptor
            .Builder().name("Tag List Location")
            .description("The location of the tag list file")
            .required(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .sensitive(false)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor EXCLUDE_NULL_VALUE = new PropertyDescriptor
            .Builder().name("Exclude Null Value")
            .description("Return data only for non null values")
            .required(true)
            .sensitive(false)
            .allowableValues("No", "Yes")
            .defaultValue("No")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor NULL_VALUE_STRING = new PropertyDescriptor
            .Builder().name("Null Value String")
            .description("If removing null values, what string is used for null")
            .required(false)
            .sensitive(false)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor AGGREGATE_RECORD = new PropertyDescriptor
            .Builder().name("Aggregate Records")
            .description("Whether to aggregate records. If this is set to true, then variable with the same time stamp will be merged into a single line. This is useful for batch-based data.")
            .required(true)
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .sensitive(false)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("Successful OPC read")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("Failed OPC read")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(OPCUA_SERVICE);
        descriptors.add(RETURN_TIMESTAMP);
        descriptors.add(EXCLUDE_NULL_VALUE);
        descriptors.add(NULL_VALUE_STRING);
        descriptors.add(TAG_LIST_SOURCE);
        descriptors.add(TAG_LIST_FILE);
        descriptors.add(AGGREGATE_RECORD);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

        timestamp.set(context.getProperty(RETURN_TIMESTAMP).getValue());
        excludeNullValue.set(context.getProperty(EXCLUDE_NULL_VALUE).getValue().equals("Yes"));
        if (context.getProperty(NULL_VALUE_STRING).isSet()) {
            nullValueString = context.getProperty(NULL_VALUE_STRING).getValue();
        }

        // Now every time onSchedule is triggered, data will be read from file anew
        if (context.getProperty(TAG_LIST_SOURCE).toString().equals("Local File")) {
            try {
                tagList = parseFile(Paths.get(context.getProperty(TAG_LIST_FILE).evaluateAttributeExpressions().toString()));
            } catch (IOException e) {
                getLogger().error("Error reading tag list from local file.");
            }
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        // Initialize  response variable
        final AtomicReference<List<String>> requestedTagnames = new AtomicReference<>();
        // Submit to getValue
        OPCUAService opcUAService;

        try {
            opcUAService = context.getProperty(OPCUA_SERVICE)
                    .asControllerService(OPCUAService.class);
        } catch (Exception ex) {
            getLogger().error(ex.getMessage());
            return;
        }

        if (context.getProperty(TAG_LIST_SOURCE).toString().equals("Flowfile")) {

            // get FlowFile
            FlowFile flowFile = session.get();
            if (flowFile == null)
                return;

            // Read tag name from flow file content
            session.read(flowFile, in -> {
                try {
                    // TODO: combine this with parseFile
                    List<String> tagname = new BufferedReader(new InputStreamReader(in))
                            .lines().collect(Collectors.toList());

                    requestedTagnames.set(tagname);

                } catch (Exception e) {
                    getLogger().error("Failed to read flowfile " + e.getMessage());
                }
            });
        } else {

            if(tagList == null)
                return;

            try {
                requestedTagnames.set(tagList);
            } catch (Exception ex) {
                getLogger().error(ex.getMessage());
                return;
            }
        }

        FlowFile flowFile;
        flowFile = session.get();
        if (flowFile == null)
            flowFile = session.create();

        byte[] values = opcUAService.getValue(requestedTagnames.get(), timestamp.get(),
                excludeNullValue.get(), nullValueString);

        if(context.getProperty(AGGREGATE_RECORD).asBoolean()) {
            values = mergeRecord(values).getBytes();
            // add csvHeader attribute to flowfile
            Map<String, String> attrMap = new HashMap<>();
            attrMap.put("csvHeader", "timestamp," + String.join(",", requestedTagnames.get()));
            flowFile = session.putAllAttributes(flowFile, attrMap);
        }

        byte[] payload = values;

        // Write the results back out to flow file
        try {
            flowFile = session.write(flowFile, out -> out.write(payload));
            session.transfer(flowFile, SUCCESS);
        } catch (ProcessException ex) {
            getLogger().error("Unable to process", ex);
            session.transfer(flowFile, FAILURE);
        }

    }


    private List<String> parseFile(Path filePath) throws IOException {
        byte[] encoded;
        encoded = Files.readAllBytes(filePath);
        String fileContent = new String(encoded, Charset.defaultCharset());
        return new BufferedReader(new StringReader(fileContent)).lines().collect(Collectors.toList());
    }

    private String mergeRecord(byte[] values) {

        int SOURCE_TS_INDEX;
        int VALUE_INDEX;
        int STATUS_CODE_INDEX;

        if (!timestamp.get().equals("Both")) {
            SOURCE_TS_INDEX = 1;
            VALUE_INDEX = 2;
            STATUS_CODE_INDEX = 3;
        } else {
            SOURCE_TS_INDEX = 2;
            VALUE_INDEX = 3;
            STATUS_CODE_INDEX = 4;
        }

        String[] rawMsgs = new String(values).split(System.lineSeparator());
        if(rawMsgs.length == 0) return "";

        StringBuilder sb = new StringBuilder();
        // Use the source timestamp of the first element as the timestamp

        boolean tsAppended = false;
        for(int i=0; i<rawMsgs.length; i++) {
            String[] fields = rawMsgs[i].trim().split(",");
            if (!fields[STATUS_CODE_INDEX].equals("0")) continue;
            if (!tsAppended) {
                sb.append(fields[SOURCE_TS_INDEX]).append(",");
                tsAppended = true;
            }
            sb.append(fields[VALUE_INDEX]);
            if( i < (rawMsgs.length - 1)) sb.append(",");
        }

        return sb.toString();
    }

}
