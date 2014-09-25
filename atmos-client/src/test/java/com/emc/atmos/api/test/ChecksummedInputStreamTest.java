package com.emc.atmos.api.test;

import com.emc.atmos.ChecksumError;
import com.emc.atmos.api.*;
import com.emc.util.StreamUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class ChecksummedInputStreamTest {
    @Test
    public void testAlgorithms() throws Exception {
        byte[] content = "Hello Checksums!".getBytes("UTF-8");

        RunningChecksum md5 = new RunningChecksum(ChecksumAlgorithm.MD5);
        md5.update(content, 0, content.length);
        RunningChecksum sha0 = new RunningChecksum(ChecksumAlgorithm.SHA0);
        sha0.update(content, 0, content.length);
        RunningChecksum sha1 = new RunningChecksum(ChecksumAlgorithm.SHA1);
        sha1.update(content, 0, content.length);

        StreamUtil.readAsBytes(new ChecksummedInputStream(new ByteArrayInputStream(content), md5));
        StreamUtil.readAsBytes(new ChecksummedInputStream(new ByteArrayInputStream(content), sha0));
        StreamUtil.readAsBytes(new ChecksummedInputStream(new ByteArrayInputStream(content), sha1));
    }

    @Test
    public void testAllReadMethods() throws Exception {
        byte[] content = "Hello Read Methods!".getBytes("UTF-8");

        RunningChecksum md5 = new RunningChecksum(ChecksumAlgorithm.MD5);
        md5.update(content, 0, content.length);

        // positive test
        InputStream is = new ChecksummedInputStream(new ByteArrayInputStream(content), md5);
        is.read();
        is.read(new byte[5]);
        is.read(new byte[5], 0, 5);
        StreamUtil.readAsBytes(is);

        // negative tests
        ChecksumValue wrong = new ChecksumValueImpl(ChecksumAlgorithm.MD5, content.length, "abcdefg");
        is = new ChecksummedInputStream(new ByteArrayInputStream(content), wrong);
        try {
            int read = 0;
            while (read >= 0) {
                read = is.read();
            }
            Assert.fail("checksum should fail in read() method");
        } catch (ChecksumError e) {
            // expected
        }

        is = new ChecksummedInputStream(new ByteArrayInputStream(content), wrong);
        try {
            int read = 0;
            while (read >= 0) {
                read = is.read(new byte[5]);
            }
            Assert.fail("checksum should fail in read(byte[]) method");
        } catch (ChecksumError e) {
            // expected
        }

        is = new ChecksummedInputStream(new ByteArrayInputStream(content), wrong);
        try {
            int read = 0;
            while (read >= 0) {
                read = is.read(new byte[5], 0, 5);
            }
            Assert.fail("checksum should fail in read(byte[], int, int) method");
        } catch (ChecksumError e) {
            // expected
        }
    }

    @Test
    public void testSkip() throws Exception {
        byte[] content = "Hello Skip Method!".getBytes("UTF-8");

        RunningChecksum md5 = new RunningChecksum(ChecksumAlgorithm.MD5);
        md5.update(content, 0, content.length);

        // positive test
        InputStream is = new ChecksummedInputStream(new ByteArrayInputStream(content), md5);
        is.read(); // read one byte
        is.skip(5); // skip 5 bytes
        StreamUtil.readAsBytes(is); // read rest

        // negative test
        ChecksumValue wrong = new ChecksumValueImpl(ChecksumAlgorithm.MD5, content.length, "abcdefg");
        is = new ChecksummedInputStream(new ByteArrayInputStream(content), wrong);
        is.read(); // read one byte
        is.skip(5); // skip 5 bytes
        testFailure(is, "wrong checksum should fail even when skipping");
    }

    @Test
    public void testFailures() throws Exception {
        byte[] content = "Hello Checksum Failures!".getBytes("UTF-8");
        RunningChecksum md5 = new RunningChecksum(ChecksumAlgorithm.MD5);
        md5.update(content, 0, content.length);
        RunningChecksum sha0 = new RunningChecksum(ChecksumAlgorithm.SHA0);
        sha0.update(content, 0, content.length);
        RunningChecksum sha1 = new RunningChecksum(ChecksumAlgorithm.SHA1);
        sha1.update(content, 0, content.length);

        // wrong checksum value
        ChecksumValue wrong = new ChecksumValueImpl(ChecksumAlgorithm.MD5, content.length, "abcdefg");
        testFailure(new ChecksummedInputStream(new ByteArrayInputStream(content), wrong),
                "wrong MD5 checksum value should throw exception");

        wrong = new ChecksumValueImpl(ChecksumAlgorithm.SHA0, content.length, "abcdefg");
        testFailure(new ChecksummedInputStream(new ByteArrayInputStream(content), wrong),
                "wrong SHA0 checksum value should throw exception");

        wrong = new ChecksumValueImpl(ChecksumAlgorithm.SHA1, content.length, "abcdefg");
        testFailure(new ChecksummedInputStream(new ByteArrayInputStream(content), wrong),
                "wrong SHA1 checksum value should throw exception");

        // right value, wrong algorithm
        wrong = new ChecksumValueImpl(ChecksumAlgorithm.MD5, content.length, sha1.getValue());
        testFailure(new ChecksummedInputStream(new ByteArrayInputStream(content), wrong),
                "SHA1 value should fail for MD5");
    }

    private void testFailure(InputStream is, String message) throws IOException {
        try {
            StreamUtil.readAsBytes(is);
            Assert.fail(message);
        } catch (ChecksumError e) {
            // expected
        }
    }
}
