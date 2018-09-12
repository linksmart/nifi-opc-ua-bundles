package de.fraunhofer.fit.opcua;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;

class TrustStoreLoader {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private KeyStore keyStore;

    TrustStoreLoader load(String tsLocation, char[] password) throws Exception {

        keyStore = KeyStore.getInstance("JKS");
        keyStore.load(new FileInputStream(tsLocation), password);

        return this;
    }

    // Only verify the first certificate, and CA certificate at the end of the chain. Intermediate certificates are not verified
    public void verify(List<X509Certificate> serverCerts) throws Exception {

        // TODO: maybe also need to verify certificate according to the application name, application uri

        if (serverCerts.size() == 0) throw new Exception("No server certificate.");

        // Get a list of certificates from trust store
        List<X509Certificate> trustedCerts = new ArrayList<>();
        Enumeration<String> aliases = keyStore.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            X509Certificate cert = (X509Certificate) keyStore.getCertificate(alias);
            if (cert != null) {
                trustedCerts.add(cert);
            }
        }

        // Check the first certificate to see if it is trusted
        X509Certificate cert = serverCerts.get(0);
        boolean certTrusted = trustedCerts.stream()
                .anyMatch(c -> Arrays.equals(cert.getSignature(), c.getSignature()));

        if (certTrusted) return;

        // Verify certificate chain
        for (int i = 0; i < serverCerts.size(); i++) {
            if (i < serverCerts.size() - 1) {
                try {
                    serverCerts.get(i).verify(serverCerts.get(i + 1).getPublicKey());
                } catch (Exception e) {
                    throw new Exception("Server certificate chain not valid.");
                }
            }
        }

        // Check the last certificate to see if it is trusted
        X509Certificate caCert = serverCerts.get(serverCerts.size() - 1);
        certTrusted = trustedCerts.stream()
                .anyMatch(c -> Arrays.equals(caCert.getSignature(), c.getSignature()));

        if (!certTrusted) throw new Exception("The CA of server certificate chain is not trusted.");
    }
}
