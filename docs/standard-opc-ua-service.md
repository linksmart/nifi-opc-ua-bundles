# StandardOPCUAService

### Getting started

To use other OPC-UA processor, you have to first configure the OPC-UA controller service in Nifi. 
The OPC-UA service is responsible for building the connection to the OPC-UA server.
The processors will use the connection established by the service to communicate with the server.

Detailed guide for setting up controller service in Nifi can be found [here](https://nifi.apache.org/docs/nifi-docs/html/user-guide.html#Controller_Services_for_Dataflows).

### Common Configuration

Property Name | Description 
------|-----
Endpoint URL|The endpoint of the OPC-UA server, e.g. `opc.tcp://192.168.0.2:48010`
Use Proxy|If true, the `Endpoint URL` specified above will be used to establish connection to the server instead of the discovered URL. Useful when connecting to OPC UA server behind NAT or through SSH tunnel, in which the discovered URL is not reachable by the client.


## Security Configuration
The OPC UA controller service provides the possibility for security connection with the OPC server. 
In the option `Security Policy`, different security policies could be selected. 
If an option other than `None` is chosen, the user must also provide information regarding other security properties.  

Property Name | Description 
------|-----
Security Policy | Different algorithms for signing and encrypting messages. If this option is set to `None`, the following options will not be in effect.
Security Mode | What measure is taken to secure data. `Signed`: data are signed to protect integrity; `SignedAndEncrypt`: Signed and encrypt data to protect privacy.
Application URI | The application URI of your OPC-UA CLIENT. It must match the \"URI\" field in \"Subject Alternative Name\" of your client certificate. Typically it has the form of \"urn:aaa:bbb\". However, whether this field is checked depends on the implementation of the server. That means, for some servers, it is not necessary to specify this field.
Client Keystore Location | The location of the keystore file (`JKS` type). Notice that the keystore should have one `PrivateKeyEntry` (private key + certificate). If multiple exist, then the first one will be used. Also notice that the the key password should be the same as the keystore password.
Client Keystore Password | The password of the keystore (the key password should also be the same)
Require Server Authentication | Whether to verify server certificate against the trust store. It is recommended to disable this option for easier testing, but enable it for production usage.
Trust Store Location | The location of the truststore file (`JKS` type). Multiple certificates inside the trust store is possible.
Trust Store Password | The password of the truststore.
Auth Policy | Choose between "Anonymous" or using username-password for authentication.
Username | Only valid when `Auth Policy` is set to `Username`. The username for authentication.
Password | Only valid when `Auth Policy` is set to `Username`. The password for authentication.

#### How to test security connection
- Generate a client keystore containing a self-signed certificate (notice that you should use the same password for both storepass and keypass):

```bash
keytool -genkey -keyalg RSA -alias nifi-client -keystore client.jks -storepass SuperSecret -keypass SuperSecret -validity 360 -keysize 2048 -ext SAN=uri:urn:nifi:opcua
```

- Download the server certificate from the OPC UA server (let's name it `server.der`);  

- Import the certificate into a JKS trust store:
```bash
keytool -importcert -file server.der -alias opc-ua-server -keystore trust.jks -storepass SuperSecret
```

- Reference these two stores from the `StandardOPCUAService` property fields. The `Application URI` field should match the `uri` field in `SAN` when generating the client certificate. 

#### Side Notes
- The security features have been tested against the in-house `IBH Link UA` server 

- When the client tries to connect to the OPC server for the first time using a self-signed certificate, 
the connection will fail with the error `Bad_SecurityChecksFailed`. This is because your client certificate is not trusted by the OPC-UA server yet. At this point, you should go to the web interface of the OPC server and manually trust the certificate. Restart the service and the connection should then succeed.  

- When you get the error `Bad_CertificateUriInvalid`, it is because the OPC-UA server checks the client's `Application URI` in its `Application Description` and it doesn't match the URI in the client certificate. Make sure you fill in the property field `Application URI` correctly.

- When testing secure connection, there is possibility that you run into the exception `Illegal Key Size`. For solution, please refer to the post [here](https://deveshsharmablogs.wordpress.com/2012/10/09/fixing-java-security-invalidkeyexception-illegal-key-size-exception/).