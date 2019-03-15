# ListOPCNodes

### Getting started

This processor list the existing nodes in an OPC-UA server and output the result as the content of a flowfile.

Before using this processor, you must set up the StandardOPCUAService first. Documentation can be found [here](standard-opc-ua-service.md).

### Configuration

Property Name | Description 
------|-----
OPC UA Service|Specifies the OPC UA Service that can be used to access data
Starting Nodes|From what node should Nifi begin browsing the node tree. Default is the root node. Seperate multiple nodes with a comma (,)
Recursive Depth|Maximum depth from the starting node to read, Default is 0
Print Indentation|Should Nifi add indentation to the output text
Max References Per Node|The number of Reference Descriptions to pull per node query
Print Non Leaf Nodes|Whether or not to print the nodes which are not leaves