package de.fraunhofer.fit.opcua;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.Enumeration;

class TrustStoreLoader {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private KeyStore keyStore;

    TrustStoreLoader load(String tsLocation, char[] password) throws Exception {

        keyStore = KeyStore.getInstance("JKS");
        keyStore.load(new FileInputStream(tsLocation), password);

        return this;
    }

    public boolean verify(Certificate serverCert) throws Exception {
        boolean verified = false;

        Enumeration<String> aliases = keyStore.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            Certificate cert = keyStore.getCertificate(alias);
            if (cert != null) {
                try {
                    // TODO: maybe except verify signature, there are other fields that we could verify
                    serverCert.verify(cert.getPublicKey());
                } catch (Exception e) {
                    continue;
                }
                verified = true;
            }

        }

        return verified;
    }
}
