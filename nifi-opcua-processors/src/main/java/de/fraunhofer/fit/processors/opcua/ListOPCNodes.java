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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class ListOPCNodes extends AbstractProcessor {

    private static String starting_node = null;
    private static String print_indentation = "No";
    private static Integer max_recursiveDepth;
    private static Integer max_reference_per_node;
    private static boolean print_non_leaf_nodes;

    public static final PropertyDescriptor OPCUA_SERVICE = new PropertyDescriptor.Builder()
            .name("OPC UA Service")
            .description("Specifies the OPC UA Service that can be used to access data")
            .required(true)
            .identifiesControllerService(OPCUAService.class)
            .build();

    public static final PropertyDescriptor STARTING_NODE = new PropertyDescriptor
            .Builder().name("Starting Nodes")
            .description("From what node should Nifi begin browsing the node tree. Default is the root node. Seperate multiple nodes with a comma (,)")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor RECURSIVE_DEPTH = new PropertyDescriptor
            .Builder().name("Recursive Depth")
            .description("Maximum depth from the starting node to read, Default is 0")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor PRINT_INDENTATION = new PropertyDescriptor
            .Builder().name("Print Indentation")
            .description("Should Nifi add indentation to the output text")
            .required(true)
            .defaultValue("")
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor MAX_REFERENCE_PER_NODE = new PropertyDescriptor
            .Builder().name("Max References Per Node")
            .description("The number of Reference Descriptions to pull per node query.")
            .required(true)
            .defaultValue("1000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor PRINT_NON_LEAF_NODES = new PropertyDescriptor
            .Builder().name("Print Non Leaf Nodes")
            .description("Whether or not to print the nodes which are not leaves.")
            .required(true)
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("Successful OPC read")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("Failed OPC read")
            .build();

    public List<PropertyDescriptor> descriptors;

    public Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(OPCUA_SERVICE);
        descriptors.add(RECURSIVE_DEPTH);
        descriptors.add(STARTING_NODE);
        descriptors.add(PRINT_INDENTATION);
        descriptors.add(MAX_REFERENCE_PER_NODE);
        descriptors.add(PRINT_NON_LEAF_NODES);

        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
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
        print_indentation = context.getProperty(PRINT_INDENTATION).getValue();
        max_recursiveDepth = Integer.valueOf(context.getProperty(RECURSIVE_DEPTH).getValue());
        starting_node = context.getProperty(STARTING_NODE).getValue();
        max_reference_per_node = Integer.valueOf(context.getProperty(MAX_REFERENCE_PER_NODE).getValue());
        print_non_leaf_nodes = Boolean.valueOf(context.getProperty(PRINT_NON_LEAF_NODES).getValue());
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        final OPCUAService opcUAService = context.getProperty(OPCUA_SERVICE)
                .asControllerService(OPCUAService.class);

        byte[] nodes = opcUAService.getNodes(print_indentation, max_recursiveDepth,
                max_reference_per_node, print_non_leaf_nodes, starting_node);

        // Write the results back out to a flow file
        FlowFile flowFile = session.create();

        if (flowFile != null) {
            try {
                flowFile = session.write(flowFile, (OutputStream out) -> {
                    out.write(nodes);
                });

                // Transfer data to flow file
                session.transfer(flowFile, SUCCESS);
            } catch (ProcessException ex) {
                getLogger().error("Unable to process", ex);
                session.transfer(flowFile, FAILURE);
            }
        }

    }
}
