package com.emc.vipr.services.s3;

import com.amazonaws.auth.AWSCredentialsProvider;
import org.junit.Assert;
import org.junit.Test;

public class IpAddressTest {
    @Test
    public void testIpAddressPattern() {
        class ViPRS3ClientProxy extends ViPRS3Client {
            public ViPRS3ClientProxy() {
                super("foo", (AWSCredentialsProvider) null);
            }

            public boolean isIpAddress(String host) {
                return host.matches(IP_ADDRESS_PATTERN);
            }
        }

        ViPRS3ClientProxy proxy = new ViPRS3ClientProxy();

        // positive tests
        String host = "10.149.137.138";
        Assert.assertTrue(host, proxy.isIpAddress(host));
        host = "255.255.255.255";
        Assert.assertTrue(host, proxy.isIpAddress(host));
        host = "1.2.3.4";
        Assert.assertTrue(host, proxy.isIpAddress(host));
        host = "99.88.77.66";
        Assert.assertTrue(host, proxy.isIpAddress(host));
        host = "123.4.56.789";
        Assert.assertTrue(host, proxy.isIpAddress(host));

        // negative tests
        host = "1.2.3";
        Assert.assertFalse(host, proxy.isIpAddress(host));
        host = "1.2.3.";
        Assert.assertFalse(host, proxy.isIpAddress(host));
        host = "1.2..3";
        Assert.assertFalse(host, proxy.isIpAddress(host));
        host = "1..2.3";
        Assert.assertFalse(host, proxy.isIpAddress(host));
        host = ".1.2.3";
        Assert.assertFalse(host, proxy.isIpAddress(host));
        host = "1.2.";
        Assert.assertFalse(host, proxy.isIpAddress(host));
        host = ".2.3";
        Assert.assertFalse(host, proxy.isIpAddress(host));
        host = "1...3";
        Assert.assertFalse(host, proxy.isIpAddress(host));
        host = "1234.5.6.7";
        Assert.assertFalse(host, proxy.isIpAddress(host));
        host = "1.2345.6.7";
        Assert.assertFalse(host, proxy.isIpAddress(host));
        host = "1.2.3456.7";
        Assert.assertFalse(host, proxy.isIpAddress(host));
        host = "1.2.3.4567";
        Assert.assertFalse(host, proxy.isIpAddress(host));
        host = "12.3.45.6.7";
        Assert.assertFalse(host, proxy.isIpAddress(host));
        host = "1.2.34.5.";
        Assert.assertFalse(host, proxy.isIpAddress(host));
        host = ".1.2.34.5";
        Assert.assertFalse(host, proxy.isIpAddress(host));
        host = "123.";
        Assert.assertFalse(host, proxy.isIpAddress(host));
    }
}
