/**
 * 
 */
package com.emc.vipr.transform.encryption;

import java.io.InputStream;
import java.security.Provider;
import java.util.Map;

import com.emc.vipr.transform.InputTransform;

/**
 * @author cwikj
 *
 */
public abstract class EncryptionInputTransform extends InputTransform {
    protected Provider provider;

    public EncryptionInputTransform(InputStream streamToDecode,
            Map<String, String> metadataToDecode, Provider provider) {
        super(streamToDecode, metadataToDecode);
        this.provider = provider;
    }

}
