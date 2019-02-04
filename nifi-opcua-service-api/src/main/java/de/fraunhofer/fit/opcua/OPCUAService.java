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
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.List;
import java.util.concurrent.BlockingQueue;

@Tags({"example"})
@CapabilityDescription("Example Service API.")
public interface OPCUAService extends ControllerService {

    byte[] getValue(List<String> reqTagNames, String returnTimestamp, boolean excludeNullValue,
                    String nullValueString) throws ProcessException;

    byte[] getNodes(String printIndent, int maxRecursiveDepth, int maxReferencePerNode,
                    boolean printNonLeafNode, String rootNodeId)
            throws ProcessException;

    String subscribe(List<String> reqTagNames, BlockingQueue<String> queue,
                     boolean tsChangedNotify, long minPublishInterval) throws ProcessException;

    void unsubscribe(String subscriberUid);
}
