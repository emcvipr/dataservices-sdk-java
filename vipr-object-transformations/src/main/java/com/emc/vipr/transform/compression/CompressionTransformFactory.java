package com.emc.vipr.transform.compression;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import SevenZip.Compression.LZMA.Encoder;

import com.emc.vipr.transform.TransformConstants;
import com.emc.vipr.transform.TransformException;
import com.emc.vipr.transform.TransformConstants.CompressionMode;
import com.emc.vipr.transform.TransformFactory;

public class CompressionTransformFactory extends
        TransformFactory<CompressionOutputTransform, CompressionInputTransform> {
    
    private static final Logger logger = LoggerFactory.getLogger(CompressionTransformFactory.class);

    public CompressionMode compressMode = TransformConstants.DEFAULT_COMPRESSION_MODE;
    public int compressionLevel = TransformConstants.DEFAULT_COMPRESSION_LEVEL;
    
    public CompressionTransformFactory() {
        setPriority(1000);
    }

    public CompressionMode getCompressMode() {
        return compressMode;
    }

    public void setCompressMode(CompressionMode compressMode) {
        this.compressMode = compressMode;
    }

    public int getCompressionLevel() {
        return compressionLevel;
    }

    public void setCompressionLevel(int compressionLevel) {
        this.compressionLevel = compressionLevel;
    }
    

    @Override
    public CompressionOutputTransform getOutputTransform(
            OutputStream streamToEncodeTo, Map<String, String> metadataToEncode) throws IOException {
        switch(compressMode) {
        case Deflate:
            return new DeflateOutputTransform(streamToEncodeTo, metadataToEncode, compressionLevel);
        case LZMA:
            return new LZMAOutputTransform(streamToEncodeTo, metadataToEncode, compressionLevel);
        default:
            throw new IllegalArgumentException("Unsupported compression method " + compressMode); 
        }
    }
    
    @Override
    public CompressionOutputTransform getOutputTransform(
            InputStream streamToEncode, Map<String, String> metadataToEncode)
            throws IOException, TransformException {
        switch(compressMode) {
        case Deflate:
            return new DeflateOutputTransform(streamToEncode, metadataToEncode, compressionLevel);
        case LZMA:
            return new LZMAOutputTransform(streamToEncode, metadataToEncode, compressionLevel);
        default:
            throw new IllegalArgumentException("Unsupported compression method " + compressMode); 
        }
    }

    @Override
    public CompressionInputTransform getInputTransform(String transformConfig, InputStream streamToDecode, Map<String, String> metadata) throws IOException {
        String[] transformTuple = splitTransformConfig(transformConfig);
        if(!TransformConstants.COMPRESSION_CLASS.equals(transformTuple[0])) {
            throw new IllegalArgumentException("Unsupported transform class: " + transformTuple[0]);
        }
        
        // Decode mode
        String[] configParams = transformTuple[1].split("/");
        
        if(configParams.length < 1) {
            throw new IllegalArgumentException("Could not decode configuration: " + configParams);
        }
        
        // First arg is mode.  Others are compression config and informational only.
        CompressionMode mode = CompressionMode.valueOf(configParams[0]);
        
        switch(mode) {
        case Deflate:
            return new DeflateInputTransform(streamToDecode, metadata);
        case LZMA:
            return new LZMAInputTransform(streamToDecode, metadata);
        default:
            throw new IllegalArgumentException("Unknown compression method " + mode);
        }
    }

    @Override
    public String getTransformClass() {
        return TransformConstants.COMPRESSION_CLASS;
    }
    
    /**
     * Checks whether this class can decode the given transformation configuration.
     * @param transformClass the transformation class to check, e.g. "COMP"
     * @param config the configuration for the transformation, e.g. "LZMA/9"
     * @param metadata the additional metadata from the object in case additional fields
     * need to be checked.
     * @return true if this factory can decode the given object stream.
     */
    public boolean canDecode(String transformClass, String config, Map<String,String> metadata) {
        // null?
        if(config == null) {
            logger.warn("Configuration string null");
            return false;
        }
        
        // Decode mode
        String[] configParams = config.split("/");
        
        if(configParams.length < 1) {
            logger.warn("Could not decode config string {}", config);
            return false;
        }
        
        // First arg is mode.  Others are compression config and informational only.
        try {
            CompressionMode.valueOf(configParams[0]);
        } catch(IllegalArgumentException e) {
            logger.warn("Invalid compression mode {}", configParams[0]);
            return false;
        }
        
        return getTransformClass().equals(transformClass);
    }
    
    /**
     * Map LZMA compression parameters into the standard 0-9 compression levels.
     */
    public static LzmaProfile LZMA_COMPRESSION_PROFILE[] = { 
        new LzmaProfile(16*1024, 5, Encoder.EMatchFinderTypeBT2), // 0
        new LzmaProfile(64*1024, 64, Encoder.EMatchFinderTypeBT2), // 1
        new LzmaProfile(512*1024, 128, Encoder.EMatchFinderTypeBT2), // 2
        new LzmaProfile(1024*1024, 128, Encoder.EMatchFinderTypeBT2), // 3
        new LzmaProfile(8*1024*1024, 128, Encoder.EMatchFinderTypeBT2), // 4
        new LzmaProfile(16*1024*1024, 128, Encoder.EMatchFinderTypeBT2), // 5
        new LzmaProfile(24*1024*1024, 192, Encoder.EMatchFinderTypeBT2), // 6
        new LzmaProfile(32*1024*1024, 224, Encoder.EMatchFinderTypeBT4), // 7
        new LzmaProfile(48*1024*1024, 256, Encoder.EMatchFinderTypeBT4), // 8
        new LzmaProfile(64*1024*1024, 273, Encoder.EMatchFinderTypeBT4) // 9
    };


    public static long memoryRequiredForLzma(int compressionLevel) {
        return memoryRequiredForLzma(LZMA_COMPRESSION_PROFILE[compressionLevel]);
    }
    
    public static long memoryRequiredForLzma(LzmaProfile profile) {
        return (long)(profile.dictionarySize * 11.5);
    }

    
    public static class LzmaProfile {
        int dictionarySize;
        int fastBytes;
        int matchFinder;
        int lc;
        int lp;
        int pb;

        public LzmaProfile(int dictionarySize, int fastBytes, int matchFinder) {
            this(dictionarySize, fastBytes, matchFinder, 3, 0, 2);
        }
        
        public LzmaProfile(int dictionarySize, int fastBytes, int matchFinder, int lc, int lp, int pb) {
            this.dictionarySize = dictionarySize;
            this.fastBytes = fastBytes;
            this.matchFinder = matchFinder;
            this.lc = lc;
            this.lp = lp;
            this.pb = pb;
        }
    }



}