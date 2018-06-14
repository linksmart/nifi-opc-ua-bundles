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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.config.OpcUaClientConfigBuilder;
import org.eclipse.milo.opcua.sdk.client.api.nodes.Node;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaMonitoredItem;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaSubscription;
import org.eclipse.milo.opcua.stack.client.UaTcpStackClient;
import org.eclipse.milo.opcua.stack.core.AttributeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.QualifiedName;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MonitoringMode;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoredItemCreateRequest;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoringParameters;
import org.eclipse.milo.opcua.stack.core.types.structured.ReadValueId;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;

@Tags({"opc"})
@CapabilityDescription("ControllerService implementation of OPCUAService.")
public class StandardOPCUAService extends AbstractControllerService implements OPCUAService {

    public static final PropertyDescriptor ENDPOINT = new PropertyDescriptor
            .Builder().name("Endpoint URL")
            .description("the opc.tcp address of the opc ua server")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SECURITY_POLICY = new PropertyDescriptor
            .Builder().name("Security Policy")
            .description("How should Nifi create the connection with the UA server")
            .required(true)
            .allowableValues("None", "Basic128Rsa15", "Basic256", "Basic256Rsa256")
            .defaultValue("None")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> properties;

    private final String SECURITY_POLICY_PREFIX = "http://opcfoundation.org/UA/SecurityPolicy#";

    private String endpoint;
    private String securityPolicy;
    private OpcUaClient opcClient;
    private UaSubscription uaSubscription;
    private Map<String, List<UaMonitoredItem>> subscriberMap;

    private final AtomicLong clientHandles = new AtomicLong(1L);

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(ENDPOINT);
        props.add(SECURITY_POLICY);
        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    /**
     * @param context the configuration context
     * @throws InitializationException if unable to create a database connection
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {

        endpoint = context.getProperty(ENDPOINT).getValue();
        securityPolicy = SECURITY_POLICY_PREFIX + context.getProperty(SECURITY_POLICY).getValue();

        try {
            EndpointDescription[] endpoints =
                    UaTcpStackClient.getEndpoints(endpoint).get();

            EndpointDescription endpointDescription = null;
            for (EndpointDescription ed : endpoints) {
                getLogger().info("Endpoint: " + ed.getEndpointUrl() + " security: " + ed.getSecurityPolicyUri());
                if (ed.getSecurityPolicyUri().equals(securityPolicy)) {
                    endpointDescription = ed;
                    getLogger().info("Connecting to endpoint " + ed.getEndpointUrl()
                            + " with security policy " + ed.getSecurityPolicyUri());
                }
            }

            if (endpointDescription == null) {
                getLogger().error("No endpoint with the specified security policy was found.");
                throw new RuntimeException("No endpoint with the specified security policy was found.");
            }

            OpcUaClientConfigBuilder cfg = new OpcUaClientConfigBuilder();
            cfg.setEndpoint(endpointDescription);
            opcClient = new OpcUaClient(cfg.build());
            opcClient.connect().get();


        } catch (Exception e) {
            throw new InitializationException(e);
        }


    }

    @OnDisabled
    public void shutdown() {
        try {
            if (opcClient != null) {
                getLogger().debug("Disconnecting from OPC server...");
                opcClient.disconnect().get();
            }
        } catch (InterruptedException | ExecutionException e) {
            getLogger().warn(e.getMessage());
        }
    }


    @Override
    public byte[] getValue(List<String> tagNames, String returnTimestamp, boolean excludeNullValue,
                           String nullValueString) throws ProcessException {
        try {
            if (opcClient == null) {
                throw new Exception("OPC Client is null. OPC UA service was not enabled properly.");
            }

            // TODO: Throw more descriptive exception when parsing fails
            ArrayList<NodeId> nodeIdList = new ArrayList<>();
            tagNames.forEach((tagName) -> nodeIdList.add(NodeId.parse(tagName)));


            List<DataValue> rvList = opcClient.readValues(0, TimestampsToReturn.Both, nodeIdList).get();

            StringBuilder serverResponse = new StringBuilder();

            for (int i = 0; i < tagNames.size(); i++) {
                String valueLine = "";
                try {
                    if (excludeNullValue && rvList.get(i).getValue().getValue().toString().equals(nullValueString)) {
                        getLogger().debug("Null value returned for " + rvList.get(i).getValue().getValue().toString()
                                + " -- Skipping because property is set");
                        continue;
                    }

                    valueLine = writeCsv(tagNames.get(i), returnTimestamp, rvList.get(i));

                } catch (Exception ex) {
                    getLogger().error("Error parsing result for " + tagNames.get(i));
                    valueLine = "";
                }
                if (valueLine.isEmpty())
                    continue;

                serverResponse.append(valueLine);

            }

            return serverResponse.toString().trim().getBytes();

        } catch (Exception e) {
            throw new ProcessException(e);
        }

    }


    private String writeCsv(String tagName, String returnTimestamp, DataValue value) {

        if(value.getServerTime() == null || value.getServerTime() == null) {
            return null;
        }

        // TODO: maybe use StringBuilder for better performance
        String valueLine = "";

        valueLine += tagName + ',';

        if (("ServerTimestamp").equals(returnTimestamp) || ("Both").equals(returnTimestamp)) {
            valueLine += value.getServerTime().getJavaTime() + ",";
        }
        if (("SourceTimestamp").equals(returnTimestamp) || ("Both").equals(returnTimestamp)) {
            valueLine += value.getServerTime().getJavaTime() + ",";
        }

        valueLine += value.getValue().getValue().toString() + ","
                + value.getStatusCode().getValue()
                + System.getProperty("line.separator");

        return valueLine;
    }




    @Override
    public byte[] getNodes(String indentString, int maxRecursiveDepth, int maxReferencePerNode,
                           boolean printNonLeafNode, String rootNodeId)
            throws ProcessException {

        try {
            if (opcClient == null) {
                throw new Exception("OPC Client is null. OPC UA service was not enabled properly.");
            }

            StringBuilder builder = new StringBuilder();
            browseNodeIteratively("", indentString, maxRecursiveDepth, maxReferencePerNode, printNonLeafNode,
                    opcClient, NodeId.parse(rootNodeId), builder);

            return builder.toString().getBytes();

        } catch (Exception e) {
            throw new ProcessException(e.getMessage());
        }

    }

    @Override
    public String subscribe(List<String> tagNames, BlockingQueue<String> queue) throws ProcessException {

        if (subscriberMap == null) {
            subscriberMap = new HashMap<>();
        }

        try {
            if (opcClient == null) {
                throw new Exception("OPC Client is null. OPC UA service was not enabled properly.");
            }

            uaSubscription = opcClient.getSubscriptionManager().createSubscription(1000.0).get();

            // Create a list of MonitoredItemCreateRequest
            ArrayList<MonitoredItemCreateRequest> micrList = new ArrayList<>();
            tagNames.forEach((tagName) -> {

                ReadValueId readValueId = new ReadValueId(
                        NodeId.parse(tagName),
                        AttributeId.Value.uid(), null, QualifiedName.NULL_VALUE);

                Long clientHandleLong = clientHandles.getAndIncrement();
                UInteger clientHandle = uint(clientHandleLong);

                MonitoringParameters parameters = new MonitoringParameters(
                        clientHandle,
                        1000.0,     // sampling interval
                        null,       // filter, null means use default
                        uint(10),   // queue size
                        true        // discard oldest
                );

                micrList.add(new MonitoredItemCreateRequest(
                        readValueId, MonitoringMode.Reporting, parameters));

            });

            // This is the callback when the MonitoredItem is created. In this callback, we set the consumer for incoming values
            BiConsumer<UaMonitoredItem, Integer> onItemCreated =
                    (item, id) -> item.setValueConsumer((it, value) -> {
                        getLogger().debug("subscription value received: item=" + it.getReadValueId().getNodeId()
                                + " value=" + value.getValue());
                        String valueLine = writeCsv(getFullName(it.getReadValueId().getNodeId()),
                                "Both", value);

                        if(valueLine != null) {
                            queue.offer(valueLine);
                        }

                    });

            List<UaMonitoredItem> items = uaSubscription.createMonitoredItems(
                    TimestampsToReturn.Both,
                    micrList,
                    onItemCreated
            ).get();

            for (UaMonitoredItem item : items) {
                if (item.getStatusCode().isGood()) {
                    getLogger().debug("item created for nodeId=" + item.getReadValueId().getNodeId());
                } else {
                    getLogger().error("failed to create item for nodeId=" + item.getReadValueId().getNodeId()
                            + " (status=" + item.getStatusCode() + ")");
                }
            }

            String subscriberUid;
            do {
                subscriberUid = generateRandomChars(10);
            } while (subscriberMap.containsKey(subscriberUid));

            subscriberMap.put(subscriberUid, items);

            return subscriberUid;

        } catch (Exception e) {
            throw new ProcessException(e.getMessage());
        }
    }

    @Override
    public void unsubscribe(String subscriberUid) throws ProcessException {

        if (opcClient == null) {
            throw new ProcessException("OPC Client is null. OPC UA service was not enabled properly.");
        }

        List<UaMonitoredItem> listToDelete;
        if ((listToDelete = subscriberMap.get(subscriberUid)) != null) {
            try {
                uaSubscription.deleteMonitoredItems(listToDelete).get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        // TODO: maybe return boolean as result?
    }

    // remainDepth = 0 means only print out the current node
    // StringBuilder is passed into the recursive method to reduce generating strings and improve performance
    private void browseNodeIteratively(String currentIndent, String indentString, int remainDepth, int maxRefPerNode,
                                       boolean printNonLeafNode, OpcUaClient client, NodeId browseRoot, StringBuilder builder) {

        //getLogger().info(indent + " Node=" + node.getNodeId().get().getIdentifier().toString());

        try {
            List<Node> nodes = client.getAddressSpace().browse(browseRoot).get();

            if (printNonLeafNode || nodes.size() == 0) {
                builder.append(currentIndent)
                        .append(getFullName(browseRoot))
                        .append("\n");
            }

            if (remainDepth > 0) {

                String newIndent = currentIndent + indentString;
                remainDepth--;

                int currNodeCount = 0;

                for (Node node : nodes) {
                    if (currNodeCount == maxRefPerNode)
                        break;

                    // recursively browse to children
                    browseNodeIteratively(newIndent, indentString, remainDepth, maxRefPerNode, printNonLeafNode,
                            client, node.getNodeId().get(), builder);

                    currNodeCount++;
                }
            }

        } catch (InterruptedException | ExecutionException e) {
            //getLogger().error("Browsing nodeId=" + browseRoot + " failed: " + e.getMessage());
        }

    }

    private String getFullName(NodeId nodeId) {

        String identifierType;

        switch (nodeId.getType()) {
            case Numeric:
                identifierType = "i";
                break;
            case Opaque:
                identifierType = "b";
                break;
            case Guid:
                identifierType = "g";
                break;
            default:
                identifierType = "s";
        }

        return String.format("ns=%s;%s=%s", nodeId.getNamespaceIndex().toString(),
                identifierType, nodeId.getIdentifier().toString());
    }

    private String generateRandomChars(int length) {
        String candidateChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder sb = new StringBuilder();
        Random random = new Random();
        for (int i = 0; i < length; i++) {
            sb.append(candidateChars.charAt(random.nextInt(candidateChars
                    .length())));
        }
        return sb.toString();
    }

}
