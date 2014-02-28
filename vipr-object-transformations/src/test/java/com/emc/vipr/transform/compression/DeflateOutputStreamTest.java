package com.emc.vipr.transform.compression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.emc.vipr.transform.TransformConstants;

public class DeflateOutputStreamTest {

    private byte[] data;

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

    @Test
    public void testWrite() throws Exception {
        // Since we're just wrapping the standard Java deflater output stream, we
        // only need to test metadata generation after compression.
        ByteArrayOutputStream compressedData = new ByteArrayOutputStream();
        DeflateOutputStream out = new DeflateOutputStream(compressedData, 5);
        out.write(data);
        
        // Should throw exception
        try {
            out.getStreamMetadata();
            fail("Should have thrown IllegalStateException that stream was not closed yet.");
        } catch(IllegalStateException e) {
            // ignore
        }
        
        out.close();
        
        Map<String, String> m = out.getStreamMetadata();
        assertEquals("Uncompressed digest incorrect", "027e997e6b1dfc97b93eb28dc9a6804096d85873",
                m.get(TransformConstants.META_COMPRESSION_UNCOMP_SHA1));
        assertEquals("Compression ratio incorrect", "91.1%",
                m.get(TransformConstants.META_COMPRESSION_COMP_RATIO));
        assertEquals("Uncompressed size incorrect", 2516125, Long.parseLong(m
                .get(TransformConstants.META_COMPRESSION_UNCOMP_SIZE)));
        assertEquals("Compressed size incorrect", 223548, Long.parseLong(m
                .get(TransformConstants.META_COMPRESSION_COMP_SIZE)));

    }

}
