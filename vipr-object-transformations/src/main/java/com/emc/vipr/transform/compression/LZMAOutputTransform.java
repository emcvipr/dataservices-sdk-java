/**
 * 
 */
package com.emc.vipr.transform.compression;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import com.emc.vipr.transform.TransformConstants;
import com.emc.vipr.transform.TransformConstants.CompressionMode;

/**
 * @author cwikj
 * 
 */
public class LZMAOutputTransform extends CompressionOutputTransform {
   
    public LZMAOutputTransform(OutputStream streamToEncodeTo,
            Map<String, String> metadataToEncode, int level) throws IOException {
        super(streamToEncodeTo, metadataToEncode,
                TransformConstants.COMPRESSION_CLASS + ":"
                        + CompressionMode.LZMA + "/" + level);

        if (level > 9 || level < 0) {
            throw new IllegalArgumentException("Invalid compression level "
                    + level);
        }

        pushStream = new LZMAOutputStream(streamToEncodeTo, level);
    }
    
    public LZMAOutputTransform(InputStream streamToEncode,
            Map<String, String> metadataToEncode, int level) throws IOException {
        super(streamToEncode, metadataToEncode,
                TransformConstants.COMPRESSION_CLASS + ":"
                        + CompressionMode.LZMA + "/" + level);

        if (level > 9 || level < 0) {
            throw new IllegalArgumentException("Invalid compression level "
                    + level);
        }

        pullStream = new LZMACompressionFilter(streamToEncode, level);
    }
    
    

    @Override
    public Map<String, String> getEncodedMetadata() {
        Map<String, String> outputMetadata = new HashMap<String, String>();
        switch(getStreamMode()) {
        case PULL:
            outputMetadata.putAll(((LZMACompressionFilter)pullStream).getStreamMetadata());
            break;
        case PUSH:
            outputMetadata.putAll(((LZMAOutputStream)pushStream).getStreamMetadata());
            break;
        }

        // Merge original
        outputMetadata.putAll(metadataToEncode);

        return outputMetadata;
    }

}
