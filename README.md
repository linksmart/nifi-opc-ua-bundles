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

## Security Connection Setup
The OPC UA controller service provides the possibility for security connection with the OPC server. 
In the option `Security Policy`, different security policies could be selected. 
If an option other than `None` is chosen, the user must also provide information regarding other security properties.  

Property Name | Description 
------|-----
Security Policy | Different algorithms for signing and encrypting messages. If this option is set to `None`, the following options will not be in effect.
Security Mode | What measure is taken to secure data. `Signed`: data are signed to protect integrity; `SignedAndEncrypt`: Signed and encrypt data to protect privacy.
Client Keystore Location | The location of the keystore file (`JKS` type). Notice that the keystore should have one `PrivateKeyEntry` (private key + certificate). If multiple exist, then the first one will be used. Also notice that the the key password should be the same as the keystore password.
Client Keystore Password | The password of the keystore (the key password should also be the same)
Require Server Authentication | Whether to verify server certificate against the trust store. It is recommended to disable this option for easier testing, but enable it for production usage.
Trust Store Location | The location of the keystore file (`JKS` type). Multiple certificates inside the trust store is possible.
Trust Store Password | The password of the keystore.
Auth Policy | Choose between "Anonymous" or using username-password for authentication.
Username | Only valid when `Auth Policy` is set to `Username`. The username for authentication.
Password | Only valid when `Auth Policy` is set to `Username`. The password for authentication.

#### Instructions on testing security connection
- Generate a client keystore containing a self-signed certificate (notice that you should use the same password for both storepass and keypass):

```bash
keytool -genkey -keyalg RSA -alias nifi-client -keystore client.jks -storepass SuperSecret -keypass SuperSecret -validity 360 -keysize 2048
```

- Download the server certificate from the OPC UA server (let's name it server.der);  

- Import the certificate into a JKS trust store:
```bash
keytool -importcert -file server.der -alias opc-ua-server -keystore trust.jks -storepass SuperSecret
```

- Reference these two keystores from the OPCUAService property fields.

#### Side Notes
- The security features have been tested against the in-house IBH Link UA server (However, currently I haven't found the way to limit anonymous user accessing data on this server, so the username-based access cannot be fully tested).  
  
- Notice: when the client tries to connect to the OPC server for the first time and the client certificate is not yet trusted by the server yet, 
the connection would not succeed. At this time, you should go to the web interface of the OPC server and manually trust the certificate. Restart the service and the connection should then succeed.  
  
  
