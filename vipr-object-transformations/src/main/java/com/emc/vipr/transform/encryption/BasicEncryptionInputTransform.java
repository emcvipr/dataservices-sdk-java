/**
 * 
 */
package com.emc.vipr.transform.encryption;

import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.Provider;
import java.util.HashMap;
import java.util.Map;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.xml.transform.TransformerException;

import com.emc.vipr.transform.TransformConstants;
import com.emc.vipr.transform.TransformException;

/**
 * @author cwikj
 *
 */
public class BasicEncryptionInputTransform extends EncryptionInputTransform {
    private CipherInputStream decryptedInput;

    /**
     * @param transformConfig 
     * @param streamToDecode
     * @param metadataToDecode
     * @throws TransformerException 
     */
    public BasicEncryptionInputTransform(String transformConfig, InputStream streamToDecode,
            Map<String, String> metadataToDecode, KeyPair masterKey, Provider provider) throws TransformException {
        super(streamToDecode, metadataToDecode, provider);
        
        // Check the transformConfig
        String[] transformParams = transformConfig.split("/");
        if(transformParams.length != 3) {
            throw new TransformException("Encryption configuration should be in the form Alg/Mode/Padding: " + transformConfig);
        }
        
        // Decrypt the object key
        String encodedObjectKey = metadataToDecode.get(TransformConstants.META_ENCRYPTION_OBJECT_KEY);
        if(encodedObjectKey == null) {
            throw new TransformException("Object key not found in object metadata");
        }
        
        SecretKey sk = KeyUtils.decryptKey(encodedObjectKey, transformParams[0], provider, 
                masterKey.getPrivate());
        
        // Get IV
        String encodedIv = metadataToDecode.get(TransformConstants.META_ENCRYPTION_IV);
        if(encodedIv == null) {
            throw new TransformException("Initialization Vector (IV) not found in object metadata");
        }
        byte[] ivData = KeyUtils.urlSafeDecodeBase64(encodedIv);
        
        // Init the cipher
        try {
            Cipher cipher = null;
            if(provider != null) {
                cipher = Cipher.getInstance(transformConfig, provider);
            } else {
                cipher = Cipher.getInstance(transformConfig);
            }
            
            IvParameterSpec ivspec = new IvParameterSpec(ivData);
            cipher.init(Cipher.DECRYPT_MODE, sk, ivspec);
            
            decryptedInput = new CipherInputStream(streamToDecode, cipher);
        } catch(GeneralSecurityException e) {
            throw new TransformException("Could not initialize cipher", e);
        }
    }

    /* (non-Javadoc)
     * @see com.emc.vipr.transform.InputTransform#decodeInputStream(java.io.InputStream)
     */
    @Override
    public InputStream getDecodedInputStream() {
        return decryptedInput;
    }

    /* (non-Javadoc)
     * @see com.emc.vipr.transform.InputTransform#decodeMetadata(java.util.Map)
     */
    @Override
    public Map<String, String> getDecodedMetadata() {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.putAll(metadataToDecode);
        
        return metadata;
    }
    

}
