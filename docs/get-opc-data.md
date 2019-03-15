# GetOPCData

### Getting started

This processor get the values of nodes every time it is triggered.

Before using this processor, you must set up the StandardOPCUAService first. Documentation can be found [here](standard-opc-ua-service.md).

### Configuration

Property Name | Description 
------|-----
OPC UA Service|Specifies the OPC UA Service that can be used to access data
Return Timestamp|Allows to select the source, server, or both timestamps
Tag List Source|Either get the tag list from the flow file, or from a dynamic property
Tag List Location|The location of the tag list file
Exclude Null Value|Return data only for non null values
Null Value String|If removing null values, what string is used for null
Aggregate Records|Whether to aggregate records. If this is set to true, then variable with the same time stamp will be merged into a single line.

### Notes
1. You can control the interval of data collection by setting the `Scheduling/Run Schedule` property.