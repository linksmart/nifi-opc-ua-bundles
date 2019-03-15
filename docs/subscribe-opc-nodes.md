# SubscribeOPCUANodes

### Getting started

This processor subscribe to the nodes given by you in a tag list file and generate new flowfiles, whenever changes are detected in those nodes.

Before using this processor, you must set up the StandardOPCUAService first. Documentation can be found [here](standard-opc-ua-service.md).

### Configuration

Property Name | Description 
------|-----
OPC UA Service|Specifies the OPC UA Service that can be used to access data
Tag List File Location|The location of the tag list file
Aggregate Records|Whether to aggregate records. If this is set to true, then variable with the same time stamp will be merged into a single line. This is useful for batch-based data.
Notified when Timestamp changed|Whether the data should be collected, when only the timestamp of a variable has changed, but not its value.
Minimum publish interval of subscription notification messages|The minimum publish interval of subscription notification messages. Set this property to a lower value so that rapid change of data can be detected.

### Notes

1. This program is structure this way, that each time it is trigger, it will examine the incoming message queue. If new messages are present, then they'll be output as flowfiles. You may want to change the `Scheduling/Run Schedule` property, so that this processor does not waste CPU resource checking on the incoming message queue.

2. The tag list file should have the following format, using `\n` as the line separator:
    ```
    ns=4;i=12345
    ns=4;i=23456
    ns=4;i=34567
    ```

3. If the `Aggregate Record` option is set, the output of the processor may look like this:
    ```
    1552646838,1.0,2.0,3.0
    ```
    In the output flowfile, there is a property field called `csvHeader`, which may look like this:
    ```
    timestamp,ns=;i=12345,ns=4;i=23456,ns=4;i=34567
    ```
    It is now up to you to merge the record and add a header to the merged flowfile.
    