package de.fraunhofer.fit.opcua;

import org.slf4j.LoggerFactory;

import java.net.*;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.collect.Sets.newHashSet;

public class Utils {

    private static final Pattern IP_ADDR_PATTERN = Pattern.compile(
            "^(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])$");

    public static Set<String> getHostnames(String address, boolean includeLoopback) {
        Set<String> hostnames = newHashSet();

        try {
            InetAddress inetAddress = InetAddress.getByName(address);

            if (inetAddress.isAnyLocalAddress()) {
                try {
                    Enumeration<NetworkInterface> nis = NetworkInterface.getNetworkInterfaces();

                    for (NetworkInterface ni : Collections.list(nis)) {
                        Collections.list(ni.getInetAddresses()).forEach(ia -> {
                            if (ia instanceof Inet4Address) {
                                boolean loopback = ia.isLoopbackAddress();

                                if (!loopback || includeLoopback) {
                                    hostnames.add(ia.getHostName());
                                    hostnames.add(ia.getHostAddress());
                                    hostnames.add(ia.getCanonicalHostName());
                                }
                            }
                        });
                    }
                } catch (SocketException e) {
                    LoggerFactory.getLogger(Utils.class)
                            .warn("Failed to NetworkInterfaces for bind address: {}", address, e);
                }
            } else {
                boolean loopback = inetAddress.isLoopbackAddress();

                if (!loopback || includeLoopback) {
                    hostnames.add(inetAddress.getHostName());
                    hostnames.add(inetAddress.getHostAddress());
                    hostnames.add(inetAddress.getCanonicalHostName());
                }
            }
        } catch (UnknownHostException e) {
            LoggerFactory.getLogger(Utils.class)
                    .warn("Failed to get InetAddress for bind address: {}", address, e);
        }

        return hostnames;
    }

}
