
# Nifi OPC-UA Bundle

This is a bundle of OPC UA controller service and processors for Nifi. The bundle is an improvement built on top of the OPC UA bundle made by `HashmapInc.`(source code here: [https://github.com/hashmapinc/nifi-opcua-bundle](https://github.com/hashmapinc/nifi-opcua-bundle) )  
A couple of differences between the new bundle and the `HashmapInc.` one:

 1. The `HashmapInc.` is based on [OPC UA-Java Stack](https://github.com/OPCFoundation/UA-Java), which provides more bottom-level APIs. The new bundle is based on [Apache Milo](https://github.com/eclipse/milo), which is built on top of  [OPC UA-Java Stack](https://github.com/OPCFoundation/UA-Java) but provides more high-level APIs and more advanced functionalities such as *subscription*.
 2. The new bundle adds a `SubscribeOPCNodes` processor, which allows the user to specify a list of OPC tags to subscribe to. The processor will produce flowfiles, when value changes on subscribed tags are detected.
 3. Adds an option to the `GetOPCData` processor so that the user can specify the source of tag list as a local file. The processor will get values of all tags listed in the file from the OPC.
 4. Adds an option to the `ListOPCNodes` processor, so that user may choose to not get nodes which are not leaves of the tree. This could come in handy, since in most cases, only leaf nodes contain value.
 5. Minor tweaks to improve performance as well as to adapt to our use case.

## Build Instructions
Build it with Maven:
```
mvn clean install
```
Find the built nar file here:
```
<repo directory>/nifi-custom-listen-tcp-nar/target/nifi-opcua-nar-<version number>.nar
```
and copy it to the following directory of the running Nifi instance:
```
/opt/nifi/nifi-1.4.0/lib
```
Restart Nifi, then you can find the new  processors available.