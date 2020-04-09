# Nifi OPC-UA Bundle

This is a bundle of OPC UA controller service and processors for Nifi. The bundle is an improvement built on top of the OPC UA bundle made by [HashmapInc](https://github.com/hashmapinc/nifi-opcua-bundle).  
A couple of differences between the new bundle and the `HashmapInc` one:

 1. The `HashmapInc` is based on [OPC UA-Java Stack](https://github.com/OPCFoundation/UA-Java), which provides more bottom-level APIs. The new bundle is based on [Eclipse Milo](https://github.com/eclipse/milo), which is built on top of  [OPC UA-Java Stack](https://github.com/OPCFoundation/UA-Java) but provides more high-level APIs and more advanced functionalities such as *subscription*.
 2. The new bundle adds a `SubscribeOPCNodes` processor, which allows the user to specify a list of OPC tags to subscribe to. The processor will produce flowfiles, when value changes on subscribed tags are detected.
 3. Adds an option to the `GetOPCData` processor so that the user can specify the source of tag list as a local file. The processor will get values of all tags listed in the file from the OPC.
 4. Adds an option to the `ListOPCNodes` processor, so that user may choose to not get nodes which are not leaves of the tree. This could come in handy, since in most cases, only leaf nodes contain value.
 5. More full-fledged security features, including support for signed or signed & encrypt messages, server certificate verification, etc.
 5. Minor tweaks to improve performance as well as to adapt to our use case.

## Build Instructions
### Build and install NAR file manually
Build it with Maven:
```
mvn clean install -DskipTests
```
Find the built nar file here:
```
<repo directory>/nifi-opcua-nar/target/nifi-opcua.nar
```
and copy it to the following directory of the running Nifi instance:
```
/opt/nifi/nifi-<version>/lib
```
Restart Nifi, then you can find the new processors available.

### Build with Docker
Another option is to build a Nifi image containing the NAR file directly:
```
docker build -t nifi-opc .
```

## How to use

To use the processors in this bundle, you have to set up the `StandardOPCUAService` as Nifi controller service first. Detailed guide can be found [here](docs/standard-opc-ua-service.md).

For the detailed description of each processor, you can find it here:
- [GetOPCData](docs/get-opc-data.md)
- [ListOPCNodes](docs/list-opc-nodes.md)
- [SubscribeOPCNodes](docs/subscribe-opc-nodes.md)

## Contributing

This project is not actively maintained because of time crunch. But contributions are welcome in terms of documentation, implementations, examples and discussions. 

Please fork, make your changes, and submit a pull request. For major changes, please open an issue first and discuss it with the other authors.
