package com.emc.vipr.transform.compression;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.emc.vipr.transform.TransformConstants;
import com.emc.vipr.transform.TransformConstants.CompressionMode;
import com.emc.vipr.transform.TransformException;

public class CompressionTransformFactoryTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testGetTransformClass() {
        CompressionTransformFactory factory = new CompressionTransformFactory();
        assertEquals(TransformConstants.COMPRESSION_CLASS, factory.getTransformClass());
    }

    @Test
    public void testGetCompressMode() {
        CompressionTransformFactory factory = new CompressionTransformFactory();
        //default
        assertEquals("default mode incorrect", CompressionMode.Deflate, factory.getCompressMode());
    }

    @Test
    public void testSetCompressMode() {
        CompressionTransformFactory factory = new CompressionTransformFactory();
        
        factory.setCompressMode(CompressionMode.LZMA);
        assertEquals("mode incorrect", CompressionMode.LZMA, factory.getCompressMode());
    }

    @Test
    public void testGetCompressionLevel() {
        CompressionTransformFactory factory = new CompressionTransformFactory();
        
        // default
        assertEquals("Default compression level incorrect", 5, factory.getCompressionLevel());
    }

    @Test
    public void testSetCompressionLevel() {
        CompressionTransformFactory factory = new CompressionTransformFactory();
        
        for(int i=0; i<10; i++) {
            factory.setCompressionLevel(i);
            assertEquals("Compression level did not set correctly", i, factory.getCompressionLevel());
        }
        
        // Bounds
        checkInvalidCompression(factory, CompressionMode.Deflate, -1);
        checkInvalidCompression(factory, CompressionMode.LZMA, -1);
        checkInvalidCompression(factory, CompressionMode.Deflate, 10);
        checkInvalidCompression(factory, CompressionMode.LZMA, 10);
    }
    
    private void checkInvalidCompression(CompressionTransformFactory factory, CompressionMode mode, int level) {
        factory.setCompressMode(mode);
        factory.setCompressionLevel(level);
        try {
            factory.getOutputTransform(new ByteArrayOutputStream(), new HashMap<String,String>());
            fail("invalid compression accepted, mode:" + mode + ", level: " + level);
        } catch(Exception e) {
            // ignore.
        }
    }

    @Test
    public void testGetOutputTransform() throws IOException {
        CompressionTransformFactory factory = new CompressionTransformFactory();
        CompressionOutputTransform t = factory.getOutputTransform(new ByteArrayOutputStream(), new HashMap<String, String>());
        assertNotNull("output transform null", t);
    }

    @Test
    public void testGetInputTransform() throws IOException {
        CompressionTransformFactory factory = new CompressionTransformFactory();
        byte[] lzmaConfig = new byte[] { 0, 0, 0, 0, 0 };
        CompressionInputTransform t = factory.getInputTransform("COMP:LZMA/9", new ByteArrayInputStream(lzmaConfig), new HashMap<String, String>());
        assertNotNull("input transform null", t);
    }

    @Test
    public void testCanDecode() {
        CompressionTransformFactory factory = new CompressionTransformFactory();
        checkDecode(factory, "COMP", "LZMA/9", true);
        checkDecode(factory, "COMP", "Deflate/5", true);
        // Technically the level parameter is not needed for decode.
        checkDecode(factory, "COMP", "Deflate", true);
        
        // Unsupported:
        checkDecode(factory, "COMP", "", false);
        checkDecode(factory, "COMP", null, false);
        checkDecode(factory, "COMP", "BZip2/2", false);
        checkDecode(factory, "COMP", "GZ/9", false);
        checkDecode(factory, "ENC", "Deflate", false);
        checkDecode(factory, "SomethingCrazy", "", false);
        checkDecode(factory, "", "", false);
        checkDecode(factory, null, null, false);
    }

    private void checkDecode(CompressionTransformFactory factory, String transformClass, String config, boolean shouldWork) {
        if(shouldWork) {
            assertTrue("Configuration " + transformClass + "|" + config + " should work", factory.canDecode(transformClass, config, new HashMap<String, String>()));
        } else {
            assertFalse("Configuration " + transformClass + "|" + config + " should not work", factory.canDecode(transformClass, config, new HashMap<String, String>()));
        }
    }
    
    // Test getting pull stream in push mode.
    @Test(expected=IllegalStateException.class) 
    public void testWrongMode() throws IOException {
        // Test the factory front-to-back.
        CompressionTransformFactory factory = new CompressionTransformFactory();
        factory.setCompressionLevel(4);
        factory.setCompressMode(CompressionMode.LZMA);
        

        ByteArrayOutputStream compressedOutput = new ByteArrayOutputStream();
        CompressionOutputTransform outTransform = factory.getOutputTransform(compressedOutput, null);
        
        outTransform.getEncodedInputStream();
    }
    
    // Test getting push stream in pull mode.
    @Test(expected=IllegalStateException.class) 
    public void testWrongMode2() throws IOException, TransformException {
        // Test the factory front-to-back.
        CompressionTransformFactory factory = new CompressionTransformFactory();
        factory.setCompressionLevel(4);
        factory.setCompressMode(CompressionMode.LZMA);
        

        ByteArrayInputStream compressedInput = new ByteArrayInputStream(new byte[32]);
        CompressionOutputTransform outTransform = factory.getOutputTransform(compressedInput, null);
        
        outTransform.getEncodedOutputStream();
    }

   
    @Test
    public void testEncodeDecodeLzma() throws Exception {
        // Test the factory front-to-back.
        CompressionTransformFactory factory = new CompressionTransformFactory();
        factory.setCompressionLevel(2);
        factory.setCompressMode(CompressionMode.LZMA);
        
        // Some generic metadata
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("name1", "value1");
        metadata.put("name2", "value2");
        
        // Get some data to compress.
        InputStream classin = this.getClass().getClassLoader()
                .getResourceAsStream("uncompressed.txt");
        ByteArrayOutputStream classByteStream = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int c = 0;
        while ((c = classin.read(buffer)) != -1) {
            classByteStream.write(buffer, 0, c);
        }
        byte[] uncompressedData = classByteStream.toByteArray();
        classin.close();

        // Compress
        ByteArrayOutputStream compressedOutput = new ByteArrayOutputStream();
        CompressionOutputTransform outTransform = factory.getOutputTransform(compressedOutput, metadata);
        
        OutputStream outStream = outTransform.getEncodedOutputStream();
        assertNotNull(outStream);
        outStream.write(uncompressedData);
        outStream.close();
        
        Map<String, String> objectData = outTransform.getEncodedMetadata();
        
        assertEquals("Uncompressed digest incorrect", "027e997e6b1dfc97b93eb28dc9a6804096d85873",
                objectData.get(TransformConstants.META_COMPRESSION_UNCOMP_SHA1));
        assertEquals("Uncompressed size incorrect", 2516125, Long.parseLong(objectData
                .get(TransformConstants.META_COMPRESSION_UNCOMP_SIZE)));
        assertEquals("Compression ratio incorrect", "95.1%",
                objectData.get(TransformConstants.META_COMPRESSION_COMP_RATIO));
        assertEquals("Compressed size incorrect", 124271, Long.parseLong(objectData
                .get(TransformConstants.META_COMPRESSION_COMP_SIZE)));
        assertEquals("name1 incorrect", "value1", objectData.get("name1"));
        assertEquals("name2 incorrect", "value2", objectData.get("name2"));

        
        String transformConfig = outTransform.getTransformConfig();
        assertEquals("Transform config string incorrect", "COMP:LZMA/2", transformConfig);
        
        // Decompress
        ByteArrayInputStream compressedInput = new ByteArrayInputStream(compressedOutput.toByteArray());
        CompressionInputTransform inTransform = factory.getInputTransform(transformConfig, compressedInput, objectData);
        assertNotNull(inTransform);
        
        InputStream decompressedStream = inTransform.getDecodedInputStream();
        assertNotNull(decompressedStream);
        byte[] uncompressedData2 = new byte[uncompressedData.length];
        
        c = 0;
        while(c < uncompressedData2.length) {
            int x = decompressedStream.read(uncompressedData2, c, uncompressedData2.length - c);
            if(x == -1) {
                break;
            }
            c += x;
        }
        
        assertEquals("stream length incorrect after decompression", uncompressedData.length, c);
        assertArrayEquals("data incorrect after decompression", uncompressedData, uncompressedData2);
        
        Map<String, String> decodedMetadata = inTransform.getDecodedMetadata();
        
        // Should be same as above.
        assertEquals("Uncompressed digest incorrect", "027e997e6b1dfc97b93eb28dc9a6804096d85873",
                decodedMetadata.get(TransformConstants.META_COMPRESSION_UNCOMP_SHA1));
        assertEquals("Compression ratio incorrect", "95.1%",
                decodedMetadata.get(TransformConstants.META_COMPRESSION_COMP_RATIO));
        assertEquals("Uncompressed size incorrect", 2516125, Long.parseLong(decodedMetadata
                .get(TransformConstants.META_COMPRESSION_UNCOMP_SIZE)));
        assertEquals("Compressed size incorrect", 124271, Long.parseLong(decodedMetadata
                .get(TransformConstants.META_COMPRESSION_COMP_SIZE)));
        assertEquals("name1 incorrect", "value1", decodedMetadata.get("name1"));
        assertEquals("name2 incorrect", "value2", decodedMetadata.get("name2"));
       
    }
    
    @Test
    public void testEncodeDecodeDeflate() throws Exception {
        // Test the factory front-to-back.
        CompressionTransformFactory factory = new CompressionTransformFactory();
        factory.setCompressionLevel(8);
        factory.setCompressMode(CompressionMode.Deflate);
        
        // Some generic metadata
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("name1", "value1");
        metadata.put("name2", "value2");
        
        // Get some data to compress.
        InputStream classin = this.getClass().getClassLoader()
                .getResourceAsStream("uncompressed.txt");
        ByteArrayOutputStream classByteStream = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int c = 0;
        while ((c = classin.read(buffer)) != -1) {
            classByteStream.write(buffer, 0, c);
        }
        byte[] uncompressedData = classByteStream.toByteArray();
        classin.close();

        // Compress
        ByteArrayOutputStream compressedOutput = new ByteArrayOutputStream();
        CompressionOutputTransform outTransform = factory.getOutputTransform(compressedOutput, metadata);
        
        OutputStream outStream = outTransform.getEncodedOutputStream();
        assertNotNull(outStream);
        outStream.write(uncompressedData);
        outStream.close();
        
        Map<String, String> objectData = outTransform.getEncodedMetadata();
        
        assertEquals("Uncompressed digest incorrect", "027e997e6b1dfc97b93eb28dc9a6804096d85873",
                objectData.get(TransformConstants.META_COMPRESSION_UNCOMP_SHA1));
        assertEquals("Compression ratio incorrect", "92.0%",
                objectData.get(TransformConstants.META_COMPRESSION_COMP_RATIO));
        assertEquals("Uncompressed size incorrect", 2516125, Long.parseLong(objectData
                .get(TransformConstants.META_COMPRESSION_UNCOMP_SIZE)));
        assertEquals("Compressed size incorrect", 201969, Long.parseLong(objectData
                .get(TransformConstants.META_COMPRESSION_COMP_SIZE)));
        assertEquals("name1 incorrect", "value1", objectData.get("name1"));
        assertEquals("name2 incorrect", "value2", objectData.get("name2"));

        
        String transformConfig = outTransform.getTransformConfig();
        assertEquals("Transform config string incorrect", "COMP:Deflate/8", transformConfig);
        
        // Decompress
        ByteArrayInputStream compressedInput = new ByteArrayInputStream(compressedOutput.toByteArray());
        CompressionInputTransform inTransform = factory.getInputTransform(transformConfig, compressedInput, objectData);
        assertNotNull(inTransform);
        
        InputStream decompressedStream = inTransform.getDecodedInputStream();
        assertNotNull(decompressedStream);
        byte[] uncompressedData2 = new byte[uncompressedData.length];
        
        c = 0;
        while(c < uncompressedData2.length) {
            int x = decompressedStream.read(uncompressedData2, c, uncompressedData2.length - c);
            if(x == -1) {
                break;
            }
            c += x;
        }
        
        assertEquals("stream length incorrect after decompression", uncompressedData.length, c);
        assertArrayEquals("data incorrect after decompression", uncompressedData, uncompressedData2);
        
        Map<String, String> decodedMetadata = inTransform.getDecodedMetadata();
        
        // Should be same as above.
        assertEquals("Uncompressed digest incorrect", "027e997e6b1dfc97b93eb28dc9a6804096d85873",
                decodedMetadata.get(TransformConstants.META_COMPRESSION_UNCOMP_SHA1));
        assertEquals("Compression ratio incorrect", "92.0%",
                decodedMetadata.get(TransformConstants.META_COMPRESSION_COMP_RATIO));
        assertEquals("Uncompressed size incorrect", 2516125, Long.parseLong(decodedMetadata
                .get(TransformConstants.META_COMPRESSION_UNCOMP_SIZE)));
        assertEquals("Compressed size incorrect", 201969, Long.parseLong(decodedMetadata
                .get(TransformConstants.META_COMPRESSION_COMP_SIZE)));
        assertEquals("name1 incorrect", "value1", decodedMetadata.get("name1"));
        assertEquals("name2 incorrect", "value2", decodedMetadata.get("name2"));

    }

    @Test
    public void testGetPriority() {
        CompressionTransformFactory factory = new CompressionTransformFactory();
        assertEquals("default priority incorrect", 1000, factory.getPriority());
    }

    @Test
    public void testSetPriority() {
        CompressionTransformFactory factory = new CompressionTransformFactory();
        factory.setPriority(500);
        assertEquals("priority incorrect", 500, factory.getPriority());
    }

}
