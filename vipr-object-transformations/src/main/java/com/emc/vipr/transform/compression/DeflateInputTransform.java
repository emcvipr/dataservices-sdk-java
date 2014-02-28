package com.emc.vipr.transform.compression;

import java.io.InputStream;
import java.util.Map;
import java.util.zip.InflaterInputStream;

public class DeflateInputTransform extends CompressionInputTransform {
    private InflaterInputStream inflater;

    public DeflateInputTransform(InputStream streamToDecode,
            Map<String, String> metadataToDecode) {
        super(streamToDecode, metadataToDecode);
        inflater = new InflaterInputStream(streamToDecode);
    }

    @Override
    public InputStream getDecodedInputStream() {
        return inflater;
    }
}
