/**
 * 
 */
package com.emc.vipr.transform.compression;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import com.emc.vipr.transform.OutputTransform;

/**
 * @author cwikj
 *
 */
public abstract class CompressionOutputTransform extends OutputTransform {

    /**
     * @param streamToEncode
     * @param metadataToEncode
     */
    public CompressionOutputTransform(OutputStream streamToEncode,
            Map<String, String> metadataToEncode, String transformConfig) {
        super(streamToEncode, metadataToEncode, transformConfig);
    }
    
    public CompressionOutputTransform(InputStream streamToEncode,
            Map<String, String> metadataToEncode, String transformConfig) {
        super(streamToEncode, metadataToEncode, transformConfig);
    }

}
