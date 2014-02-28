package com.emc.vipr.transform.encryption;

import java.lang.reflect.Method;
import java.security.Provider;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Fips140KeyStoreEncryptionTest extends KeyStoreEncryptionFactoryTest {

    private static final Logger logger = LoggerFactory.getLogger(Fips140BasicEncryptionTest.class);

    @Before
    public void setUp() throws Exception {
        // Check to make sure the provider is available.
        boolean providerLoaded = false;
        
        try {
            Class<?> bsafeProvider = Class.forName("com.rsa.jsafe.provider.JsafeJCE");
            Provider p = (Provider) bsafeProvider.newInstance();
            provider = p;
            providerLoaded = true;
        } catch(ClassNotFoundException e) {
            logger.info("RSA Crypto-J JCE Provider not found: " + e);
        } catch(NoClassDefFoundError e) {
            logger.info("RSA Crypto-J JCE Provider not found: " + e);
        }
        
        Assume.assumeTrue("Crypto-J JCE provider not loaded", providerLoaded);
        super.setUp();
    }
    
    @Test
    public void testFips140CompliantMode() throws Exception {
        // Verify FIPS-140 mode.
        // Do this through reflection so tests don't fail to run/compile if the 
        // crypto-J module is not available.
        Class<?> cryptoJClass = Class.forName("com.rsa.jsafe.crypto.CryptoJ");
        Method fipsCheck = cryptoJClass.getMethod("isFIPS140Compliant", (Class<?>[])null);
        Object result = fipsCheck.invoke(null, (Object[])null);
        Assert.assertTrue("isFips140Compliant() didn't return a boolean", 
                result instanceof Boolean);
        Boolean b = (Boolean)result;
        Assert.assertTrue("Crypto-J is not FIPS-140 compliant", b);
        
        Method fipsCheck2 = cryptoJClass.getMethod("isInFIPS140Mode", (Class<?>[])null);
        Object result2 = fipsCheck2.invoke(null, (Object[])null);
        Assert.assertTrue("isFips140Compliant() didn't return a boolean", 
                result2 instanceof Boolean);
        Boolean b2 = (Boolean)result;
        Assert.assertTrue("Crypto-J is not in FIPS-140 mode", b2);

    }

}
