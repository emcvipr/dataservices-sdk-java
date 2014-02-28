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
public class DeflateOutputTransform extends CompressionOutputTransform {

    /**
     * @param streamToEncode
     * @param metadataToEncode
     * @throws IOException
     */
    public DeflateOutputTransform(OutputStream streamToEncode,
            Map<String, String> metadataToEncode, int compressionLevel)
            throws IOException {
        super(streamToEncode, metadataToEncode,
                TransformConstants.COMPRESSION_CLASS + ":"
                        + CompressionMode.Deflate + "/" + compressionLevel);

        if (compressionLevel > 9 || compressionLevel < 0) {
            throw new IllegalArgumentException(
                    "Invalid Deflate compression level: " + compressionLevel);
        }

        pushStream = new DeflateOutputStream(streamToEncode, compressionLevel);
    }
    
    public DeflateOutputTransform(InputStream streamToEncode,
            Map<String, String> metadataToEncode, int compressionLevel) throws IOException {
        super(streamToEncode, metadataToEncode,
                TransformConstants.COMPRESSION_CLASS + ":"
                        + CompressionMode.Deflate + "/" + compressionLevel);

        if (compressionLevel > 9 || compressionLevel < 0) {
            throw new IllegalArgumentException(
                    "Invalid Deflate compression level: " + compressionLevel);
        }

        pullStream = new DeflateInputFilter(streamToEncode, compressionLevel);
        
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.emc.vipr.transform.OutputTransform#getEncodedMetadata()
     */
    @Override
    public Map<String, String> getEncodedMetadata() {
        Map<String, String> metadata = new HashMap<String, String>();

        // Merge stream metadata
        switch(getStreamMode()) {
        case PULL:
            metadata.putAll(((CompressionStream) pullStream).getStreamMetadata());
            break;
        case PUSH:
            metadata.putAll(((CompressionStream) pushStream).getStreamMetadata());
            break;
        }

        // Merge original metadata
        metadata.putAll(metadataToEncode);

        return metadata;
    }
    

}
