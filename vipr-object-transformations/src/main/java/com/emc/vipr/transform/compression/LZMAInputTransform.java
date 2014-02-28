/**
 * 
 */
package com.emc.vipr.transform.compression;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * @author cwikj
 *
 */
public class LZMAInputTransform extends CompressionInputTransform {
    private LZMAInputStream lzmaInput;

    /**
     * @param streamToDecode
     * @param metadataToDecode
     * @throws IOException 
     */
    public LZMAInputTransform(InputStream streamToDecode,
            Map<String, String> metadataToDecode) throws IOException {
        super(streamToDecode, metadataToDecode);
        lzmaInput = new LZMAInputStream(streamToDecode);
    }
    
    @Override
    public InputStream getDecodedInputStream() {
        return lzmaInput;
    }

}
