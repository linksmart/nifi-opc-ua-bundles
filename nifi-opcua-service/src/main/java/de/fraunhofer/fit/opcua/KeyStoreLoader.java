package de.fraunhofer.fit.opcua;

import org.eclipse.milo.opcua.stack.core.util.SelfSignedCertificateBuilder;
import org.eclipse.milo.opcua.stack.core.util.SelfSignedCertificateGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.security.*;
import java.security.cert.X509Certificate;
import java.util.regex.Pattern;

class KeyStoreLoader {

    private static final Pattern IP_ADDR_PATTERN = Pattern.compile(
            "^(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])$");

    private static final char[] PASSWORD = "password".toCharArray();

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private X509Certificate clientCertificate;
    private KeyPair clientKeyPair;

    KeyStoreLoader load(String applicationName) throws Exception {
        File securityTempDir = new File(System.getProperty("java.io.tmpdir"), "security");
        if (!securityTempDir.exists() && !securityTempDir.mkdirs()) {
            throw new Exception("unable to create security dir: " + securityTempDir);
        }

        KeyStore keyStore = KeyStore.getInstance("PKCS12");

        File serverKeyStore = securityTempDir.toPath().resolve("example-client.pfx").toFile();

        logger.info("Loading KeyStore at {}", serverKeyStore);

        if (!serverKeyStore.exists()) {
            keyStore.load(null, PASSWORD);

            KeyPair keyPair = SelfSignedCertificateGenerator.generateRsaKeyPair(2048);

            SelfSignedCertificateBuilder builder = new SelfSignedCertificateBuilder(keyPair)
                    .setCommonName("Nifi Milo OPC UA Client")
                    .setOrganization("Nifi")
                    .setOrganizationalUnit("dev")
                    .setLocalityName("Sankt Augustin")
                    .setStateName("NRW")
                    .setCountryCode("DE")
                    .setApplicationUri(applicationName)
                    .addDnsName("localhost")
                    .addIpAddress("127.0.0.1");

            // Get as many hostnames and IP addresses as we can listed in the certificate.
            for (String hostname : Utils.getHostnames("0.0.0.0", true)) {
                System.out.println("hostname: " + hostname);
                if (IP_ADDR_PATTERN.matcher(hostname).matches()) {
                    builder.addIpAddress(hostname);
                } else {
                    builder.addDnsName(hostname);
                }
            }

            X509Certificate certificate = builder.build();

            keyStore.setKeyEntry(applicationName, keyPair.getPrivate(), PASSWORD, new X509Certificate[]{certificate});
            keyStore.store(new FileOutputStream(serverKeyStore), PASSWORD);
        } else {
            keyStore.load(new FileInputStream(serverKeyStore), PASSWORD);
        }


        Key serverPrivateKey = keyStore.getKey(applicationName, PASSWORD);
        if (serverPrivateKey != null) {
            if (serverPrivateKey instanceof PrivateKey) {
                clientCertificate = (X509Certificate) keyStore.getCertificate(applicationName);
                PublicKey serverPublicKey = clientCertificate.getPublicKey();
                clientKeyPair = new KeyPair(serverPublicKey, (PrivateKey) serverPrivateKey);
            }
        }

        return this;
    }

    X509Certificate getClientCertificate() {
        return clientCertificate;
    }

    KeyPair getClientKeyPair() {
        return clientKeyPair;
    }

}
