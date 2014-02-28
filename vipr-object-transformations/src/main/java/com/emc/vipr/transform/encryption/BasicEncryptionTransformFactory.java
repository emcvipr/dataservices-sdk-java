package com.emc.vipr.transform.encryption;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.emc.vipr.transform.TransformConstants;
import com.emc.vipr.transform.TransformException;

public class BasicEncryptionTransformFactory
        extends
        EncryptionTransformFactory<BasicEncryptionOutputTransform, BasicEncryptionInputTransform> {
    Logger logger = LoggerFactory
            .getLogger(BasicEncryptionTransformFactory.class);

    public KeyPair masterEncryptionKey;
    private String masterEncryptionKeyFingerprint;
    private Map<String, KeyPair> masterDecryptionKeys;

    public BasicEncryptionTransformFactory() throws InvalidKeyException,
            NoSuchAlgorithmException, NoSuchPaddingException {
        super();
        masterDecryptionKeys = new HashMap<String, KeyPair>();
    }
    
    public BasicEncryptionTransformFactory(KeyPair masterEncryptionKey, 
            Set<KeyPair> masterDecryptionKeys) throws InvalidKeyException,
            NoSuchAlgorithmException, NoSuchPaddingException {
        
        this.masterDecryptionKeys = new HashMap<String, KeyPair>();
        setMasterEncryptionKey(masterEncryptionKey);
        if(masterDecryptionKeys != null) {
            for(KeyPair kp : masterDecryptionKeys) {
                addMasterDecryptionKey(kp);
            }
        }
    }

    public BasicEncryptionTransformFactory(KeyPair masterEncryptionKey, 
            Set<KeyPair> masterDecryptionKeys, Provider provider) 
                    throws InvalidKeyException, NoSuchAlgorithmException, 
                    NoSuchPaddingException {
        
        super(TransformConstants.DEFAULT_ENCRYPTION_TRANSFORM,
                TransformConstants.DEFAULT_ENCRYPTION_KEY_SIZE, provider);

        this.masterDecryptionKeys = new HashMap<String, KeyPair>();
        setMasterEncryptionKey(masterEncryptionKey);
        if(masterDecryptionKeys != null) {
            for(KeyPair kp : masterDecryptionKeys) {
                addMasterDecryptionKey(kp);
            }
        }       
    }

    public void setMasterEncryptionKey(KeyPair pair) {
        if (!(pair.getPublic() instanceof RSAPublicKey)) {
            throw new IllegalArgumentException(
                    "Only RSA KeyPairs are allowed, not "
                            + pair.getPublic().getAlgorithm());
        }
        checkKeyLength(pair);
        this.masterEncryptionKey = pair;
        try {
            this.masterEncryptionKeyFingerprint = KeyUtils
                    .getRsaPublicKeyFingerprint((RSAPublicKey) pair.getPublic(), provider);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Error adding master key", e);
        }
        addMasterDecryptionKey(pair);
    }

    public void addMasterDecryptionKey(KeyPair pair) {
        try {
            String fingerprint = KeyUtils
                    .getRsaPublicKeyFingerprint((RSAPublicKey) pair.getPublic(), provider);
            masterDecryptionKeys.put(fingerprint, pair);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Error adding master key", e);
        }
    }

    @Override
    public Map<String, String> rekey(Map<String, String> metadata)
            throws TransformException {
        String oldKeyId = metadata
                .get(TransformConstants.META_ENCRYPTION_KEY_ID);
        if (oldKeyId == null) {
            throw new TransformException(
                    "Metadata does not contain a master key ID");
        }
        if (oldKeyId.equals(masterEncryptionKeyFingerprint)) {
            // This object is already using the current key.
            logger.info("Object is already using the current master key");
            throw new DoesNotNeedRekeyException(
                    "Object is already using the current master key");
        }
        // Make sure we have the old key
        if (!masterDecryptionKeys.containsKey(oldKeyId)) {
            throw new TransformException("Master key with fingerprint "
                    + oldKeyId + " not found");
        }
        
        KeyPair oldKey = masterDecryptionKeys.get(oldKeyId);
        String encodedKey = metadata.get(TransformConstants.META_ENCRYPTION_OBJECT_KEY);
        if(encodedKey == null) {
            throw new TransformException("Encrypted object key not found");
        }
        
        String algorithm = getEncryptionAlgorithm();
        
        SecretKey objectKey = KeyUtils.decryptKey(encodedKey, algorithm, provider, 
                oldKey.getPrivate());
        
        // Re-encrypt key with the current master key
        String newKey;
        try {
            newKey = KeyUtils.encryptKey(objectKey, provider, 
                    masterEncryptionKey.getPublic());
        } catch (GeneralSecurityException e) {
            throw new TransformException("Could not re-encrypt key: " + e, e);
        }
        
        Map<String, String> newMetadata = new HashMap<String, String>();
        newMetadata.putAll(metadata);
        newMetadata.remove(TransformConstants.META_ENCRYPTION_META_SIG);
        newMetadata.put(TransformConstants.META_ENCRYPTION_OBJECT_KEY, newKey);
        newMetadata.put(TransformConstants.META_ENCRYPTION_KEY_ID, 
                masterEncryptionKeyFingerprint);
        
        // Re-sign
        String signature = KeyUtils.signMetadata(newMetadata, 
                (RSAPrivateKey) masterEncryptionKey.getPrivate(), provider);
        newMetadata.put(TransformConstants.META_ENCRYPTION_META_SIG, signature);

        return newMetadata;
    }
    
    @Override
    public BasicEncryptionOutputTransform getOutputTransform(
            OutputStream streamToEncodeTo, Map<String, String> metadataToEncode)
            throws IOException {
        return new BasicEncryptionOutputTransform(streamToEncodeTo,
                metadataToEncode, masterEncryptionKeyFingerprint,
                masterEncryptionKey, encryptionTransform, keySize, provider);
    }
    
    @Override
    public BasicEncryptionOutputTransform getOutputTransform(
            InputStream streamToEncode, Map<String, String> metadataToEncode)
            throws IOException, TransformException {
        return new BasicEncryptionOutputTransform(streamToEncode,
                metadataToEncode, masterEncryptionKeyFingerprint,
                masterEncryptionKey, encryptionTransform, keySize, provider);
    }

    @Override
    public BasicEncryptionInputTransform getInputTransform(
            String transformConfig, InputStream streamToDecode,
            Map<String, String> metadata) throws IOException,
            TransformException {

        String[] transformTuple = splitTransformConfig(transformConfig);
        if (transformTuple.length != 2) {
            throw new TransformException("Invalid transform configuration: "
                    + transformConfig);
        }

        if (!TransformConstants.ENCRYPTION_CLASS.equals(transformTuple[0])) {
            throw new TransformException("Unsupported transform class: "
                    + transformTuple[0]);
        }

        // Find master key
        String masterKeyId = metadata
                .get(TransformConstants.META_ENCRYPTION_KEY_ID);
        if (masterKeyId == null) {
            throw new TransformException(
                    "Could not decrypt object. No master key ID set on object.");
        }

        KeyPair masterKey = masterDecryptionKeys.get(masterKeyId);
        if (masterKey == null) {
            throw new TransformException(
                    "Could not decrypt object. No master key with ID "
                            + masterKeyId + " found");
        }

        return new BasicEncryptionInputTransform(transformTuple[1],
                streamToDecode, metadata, masterKey, provider);
    }

    /**
     * Check for acceptable RSA key lengths. 1024-bit keys are not secure
     * anymore and 512-bit keys are unacceptable. Newer JDKs have already
     * removed support for the 512-bit keys and the 1024-bit keys may be removed
     * in the future:
     * http://mail.openjdk.java.net/pipermail/security-dev/2012-December/
     * 006195.html
     * 
     * @param pair
     */
    private void checkKeyLength(KeyPair pair) {
        // RSA key length is defined as the modulus of the public key
        int keySize = ((RSAPublicKey) pair.getPublic()).getModulus()
                .bitLength();
        if (keySize < 1024) {
            throw new IllegalArgumentException(
                    "The minimum RSA key size supported is 1024 bits. Your key is "
                            + keySize + " bits");
        } else if (keySize == 1024) {
            logger.info("1024-bit RSA key detected. Support for 1024-bit RSA keys may soon be removed from the JDK. Please upgrade to a stronger key (e.g. 2048-bit).");
        }
    }

}