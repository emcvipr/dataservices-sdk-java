package com.emc.vipr.services.s3;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * Test suite to test the object functionality (but not file or internals)
 */
@RunWith(Suite.class)
@SuiteClasses({ AppendTest.class, BasicS3Test.class,
        S3EncryptionClientTest.class, UpdateTest.class })
public class ObjectTests {

}
