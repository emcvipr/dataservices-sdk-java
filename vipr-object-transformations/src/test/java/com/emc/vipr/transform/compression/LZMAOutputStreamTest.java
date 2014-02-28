/**
 * 
 */
package com.emc.vipr.transform.compression;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import SevenZip.Compression.LZMA.Encoder;

import com.emc.vipr.transform.TransformConstants;
import com.emc.vipr.transform.compression.CompressionTransformFactory.LzmaProfile;

/**
 * @author cwikj
 * 
 */
public class LZMAOutputStreamTest {
    byte[] data;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        // get some data to compress.
        InputStream classin = this.getClass().getClassLoader()
                .getResourceAsStream("uncompressed.txt");
        ByteArrayOutputStream classByteStream = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int c = 0;
        while ((c = classin.read(buffer)) != -1) {
            classByteStream.write(buffer, 0, c);
        }
        data = classByteStream.toByteArray();
        
        classin.close();
    }

    /**
     * Test method for
     * {@link com.emc.vipr.transform.compression.LZMAOutputStream#memoryRequired(int)}
     * .
     */
    @Test
    public void testMemoryRequiredInt() {
        assertEquals(192937984, CompressionTransformFactory.memoryRequiredForLzma(5));
        assertEquals(771751936, CompressionTransformFactory.memoryRequiredForLzma(9));
    }

    /**
     * Test method for
     * {@link com.emc.vipr.transform.compression.LZMAOutputStream#memoryRequired(com.emc.vipr.transform.compression.LZMAOutputStream.LzmaProfile)}
     * .
     */
    @Test
    public void testMemoryRequiredLzmaProfile() {
        assertEquals(0,
                CompressionTransformFactory.memoryRequiredForLzma(new LzmaProfile(0, 0, 0)));
        assertEquals(6174015488L,
                CompressionTransformFactory.memoryRequiredForLzma(new LzmaProfile(
                        512 * 1024 * 1024, 0, 0)));
    }

    @Test
    public void testCompressMode0() throws Exception {
        runCompressMode(0);
    }

    @Test
    public void testCompressMode1() throws Exception {
        runCompressMode(1);
    }

    @Test
    public void testCompressMode2() throws Exception {
        runCompressMode(2);
    }

    @Test
    public void testCompressMode3() throws Exception {
        runCompressMode(3);
    }

    @Test
    public void testCompressMode4() throws Exception {
        runCompressMode(4);
    }

    @Test
    public void testCompressMode5() throws Exception {
        runCompressMode(5);
    }

    @Test
    public void testCompressMode6() throws Exception {
        runCompressMode(6);
    }

    @Test
    public void testCompressMode7() throws Exception {
        runCompressMode(7);
    }

    @Test
    public void testCompressMode8() throws Exception {
        runCompressMode(8);
    }

    @Test
    public void testCompressMode9() throws Exception {
        runCompressMode(9);
    }

    @Test
    public void testCustomCompressMode() throws Exception {
        // Small memory footprint but work harder (max fastBits and 64-bit
        // matcher)
        runCompressMode(new LzmaProfile(8 * 1024, 273,
                Encoder.EMatchFinderTypeBT4));
    }

    @Test
    public void testCompressionMetadata() throws Exception {
        LZMAOutputStream lzma = runCompressMode(new LzmaProfile(16 * 1024, 128,
                Encoder.EMatchFinderTypeBT2));
        Map<String, String> m = lzma.getStreamMetadata();
        assertEquals("Uncompressed digest incorrect", "027e997e6b1dfc97b93eb28dc9a6804096d85873",
                m.get(TransformConstants.META_COMPRESSION_UNCOMP_SHA1));
        assertEquals("Compression ratio incorrect", "93.9%",
                m.get(TransformConstants.META_COMPRESSION_COMP_RATIO));
        assertEquals("Uncompressed size incorrect", 2516125, Long.parseLong(m
                .get(TransformConstants.META_COMPRESSION_UNCOMP_SIZE)));
        assertEquals("Compressed size incorrect", 154656, Long.parseLong(m
                .get(TransformConstants.META_COMPRESSION_COMP_SIZE)));
    }

    private void runCompressMode(int i) throws IOException {
        System.out.println("Testing compression level " + i);

        long requiredMemory = CompressionTransformFactory.memoryRequiredForLzma(i);
        System.out.println("Estimated memory usage: "
                + (requiredMemory / (1024 * 1024)) + "MB");

        // Make sure there's enough RAM otherwise skip.
        Runtime.getRuntime().gc();
        long startMemory = Runtime.getRuntime().totalMemory()
                - Runtime.getRuntime().freeMemory();
        long availableMemory = Runtime.getRuntime().maxMemory() - startMemory;

        Assume.assumeTrue(
                "Skipping test because there is not enough available heap",
                availableMemory > requiredMemory);

        long now = System.currentTimeMillis();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        LZMAOutputStream lout = new LZMAOutputStream(out, i);
        lout.write(data);
        long usedMemory = Runtime.getRuntime().totalMemory()
                - Runtime.getRuntime().freeMemory() - startMemory;
        System.out.println("Memory used: " + (usedMemory / (1024 * 1024))
                + "MB");
        lout.close();

        byte[] compressed = out.toByteArray();

        System.out.println(String.format(
                "Original size: %d Compressed size: %d", data.length,
                compressed.length));
        System.out.println("Compression Ratio: "
                + (100 - (compressed.length * 100 / data.length)) + "%");
        System.out
                .println("Time: " + (System.currentTimeMillis() - now) + "ms");
        assertTrue("compressed data not smaller than original for level " + i,
                compressed.length < data.length);
    }

    private LZMAOutputStream runCompressMode(LzmaProfile profile)
            throws IOException {
        System.out.println("Testing custom profile");

        long requiredMemory = CompressionTransformFactory.memoryRequiredForLzma(profile);
        System.out.println("Estimated memory usage: "
                + (requiredMemory / (1024 * 1024)) + "MB");

        // Make sure there's enough RAM otherwise skip.
        Runtime.getRuntime().gc();
        long startMemory = Runtime.getRuntime().totalMemory()
                - Runtime.getRuntime().freeMemory();
        long availableMemory = Runtime.getRuntime().maxMemory() - startMemory;

        Assume.assumeTrue(
                "Skipping test because there is not enough available heap",
                availableMemory > requiredMemory);

        long now = System.currentTimeMillis();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        LZMAOutputStream lout = new LZMAOutputStream(out, profile);
        lout.write(data);
        long usedMemory = Runtime.getRuntime().totalMemory()
                - Runtime.getRuntime().freeMemory() - startMemory;
        System.out.println("Memory used: " + (usedMemory / (1024 * 1024))
                + "MB");
        lout.close();

        byte[] compressed = out.toByteArray();

        System.out.println(String.format(
                "Original size: %d Compressed size: %d", data.length,
                compressed.length));
        System.out.println("Compression Ratio: "
                + (100 - (compressed.length * 100 / data.length)) + "%");
        System.out
                .println("Time: " + (System.currentTimeMillis() - now) + "ms");
        assertTrue("compressed data not smaller than original",
                compressed.length < data.length);

        return lout;
    }

}
