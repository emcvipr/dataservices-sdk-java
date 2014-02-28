package com.emc.vipr.transform.compression;

import static org.junit.Assert.*;

import java.io.InputStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LZMAInputStreamTest {

    private InputStream uncompressedData;
    private InputStream compressedData;

    @Before
    public void setUp() throws Exception {
        // Get streams for the compressed and uncompressed test data.
        uncompressedData = this.getClass().getClassLoader()
                .getResourceAsStream("uncompressed.txt");
        compressedData = this.getClass().getClassLoader()
                .getResourceAsStream("compressed.txt.lz");
    }
    
    @After
    public void tearDown() {
        try {
            uncompressedData.close();
            uncompressedData = null;
        } catch(Exception e) {
            // Ignore
        }
        
        try {
            compressedData.close();
            compressedData = null;
        } catch(Exception e) {
            // Ignore
        }
    }

    @Test
    public void testRead() throws Exception {
        InputStream decompressed = new LZMAInputStream(compressedData);
        
        int in1, in2;
        long offset = 0;
        while((in1 = uncompressedData.read()) != -1) {
            in2 = decompressed.read();
            assertEquals("Mismatch at offset " + offset, in1, in2);
            offset++;
        }
        
        // Should be -1 at EOF
        in2 = decompressed.read();
        assertEquals("Mismatch at EOF", -1, in2);        
        
        // Close should not throw here.
        decompressed.close();
        uncompressedData.close();
    }

    @Test
    public void testReadByteArray() throws Exception {
        InputStream decompressed = new LZMAInputStream(compressedData);
        
        byte[] buffer1 = new byte[4096];
        byte[] buffer2 = new byte[4096];
        int c1 = 0;
        int c2 = 0;
        long offset = 0;
        while((c1 = uncompressedData.read(buffer1)) != -1) {
            c2 = decompressed.read(buffer2);
            assertEquals("Read size mismatch at offset " + offset, c1, c2);
            assertArrayEquals(buffer1, buffer2);
            offset += c1;
        }
        
        // Next read should return -1 for EOF
        c2 = decompressed.read(buffer2);
        assertEquals("Mismatch at EOF", -1, c2);
        
        // Close should not throw here.
        decompressed.close();
        uncompressedData.close();
    }

    @Test
    public void testReadByteArrayIntInt() throws Exception {
        InputStream decompressed = new LZMAInputStream(compressedData);
        
        byte[] buffer1 = new byte[4096];
        byte[] buffer2 = new byte[4096];
        int c1 = 0;
        int c2 = 0;
        long offset = 0;
        while((c1 = uncompressedData.read(buffer1, 1, 2047)) != -1) {
            c2 = 0;
            
            // If you hit a pipe buffer boundary, it might take more than one read.
            while(c2 < c1) {
                c2 += decompressed.read(buffer2, c2+1, 2047-c2);
            }
            
            assertEquals("Read size mismatch at offset " + offset, c1, c2);
            assertArrayEquals(buffer1, buffer2);
            offset += c1;
        }
        
        // Next read should return -1 for EOF
        c2 = decompressed.read(buffer2);
        assertEquals("Mismatch at EOF", -1, c2);
        
        // Close should not throw here.
        decompressed.close();
        uncompressedData.close();
    }

    @Test
    public void testSkip() throws Exception {
        InputStream decompressed = new LZMAInputStream(compressedData);
        
        int in1, in2;
        long offset = 0;
        while((in1 = uncompressedData.read()) != -1) {
            in2 = decompressed.read();
            assertEquals("Mismatch at offset " + offset, in1, in2);
            
            // Skip some bytes.
            long offset1 = uncompressedData.skip(7);
            long offset2 = decompressed.skip(7);
            
            assertEquals("Skipped bytes mismatch at offset " + offset, offset1, offset2);
            
            offset += offset1 + 1;
        }
        
        // Should be -1 at EOF
        in2 = decompressed.read();
        assertEquals("Mismatch at EOF", -1, in2);        
        
        // Close should not throw here.
        decompressed.close();
        uncompressedData.close();
    }

    @Test
    public void testMarkSupported() throws Exception {
        InputStream decompressed = new LZMAInputStream(compressedData);
        
        assertFalse(decompressed.markSupported());
        
        // Close should not throw here.
        decompressed.close();
    }

}
