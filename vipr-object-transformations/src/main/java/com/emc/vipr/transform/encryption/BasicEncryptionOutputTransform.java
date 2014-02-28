/**
 * 
 */
package com.emc.vipr.transform.encryption;

import java.io.InputStream;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.interfaces.RSAPrivateKey;
import java.util.HashMap;
import java.util.Map;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;

import com.emc.vipr.transform.TransformConstants;

/**
 * @author cwikj
 * 
 */
public class BasicEncryptionOutputTransform extends EncryptionOutputTransform {
    byte[] iv;
    SecretKey k;

    private String masterEncryptionKeyFingerprint;
    private KeyPair masterKey;

    /**
     * @param streamToEncodeTo
     * @param metadataToEncode
     * @param masterEncryptionKeyFingerprint
     * @throws NoSuchPaddingException
     * @throws NoSuchAlgorithmException
     * @throws InvalidKeyException
     */
    public BasicEncryptionOutputTransform(OutputStream streamToEncodeTo,
            Map<String, String> metadataToEncode,
            String masterEncryptionKeyFingerprint, KeyPair asymmetricKey,
            String encryptionTransform, int keySize, Provider provider) {
        super(streamToEncodeTo, metadataToEncode,
                TransformConstants.ENCRYPTION_CLASS + ":" + encryptionTransform,
                provider);

        this.masterEncryptionKeyFingerprint = masterEncryptionKeyFingerprint;
        this.masterKey = asymmetricKey;
        
        try {
            Cipher cipher = initCipher(encryptionTransform, keySize);
            MessageDigest sha1 = null;
            if (provider != null) {
                sha1 = MessageDigest.getInstance("SHA1", provider);
            } else {
                sha1 = MessageDigest.getInstance("SHA1");
            }

            pushStream = new EncryptionOutputStream(streamToEncodeTo, cipher, sha1);
        } catch (GeneralSecurityException e) {
            throw new RuntimeException("Error initializing output transform: "
                    + e.getMessage(), e);
        }
    }
    
    public BasicEncryptionOutputTransform(InputStream streamToEncode,
            Map<String, String> metadataToEncode,
            String masterEncryptionKeyFingerprint, KeyPair asymmetricKey,
            String encryptionTransform, int keySize, Provider provider) {
        super(streamToEncode, metadataToEncode,
                TransformConstants.ENCRYPTION_CLASS + ":" + encryptionTransform,
                provider);
        
        this.masterEncryptionKeyFingerprint = masterEncryptionKeyFingerprint;
        this.masterKey = asymmetricKey;
        
        try {
            Cipher cipher = initCipher(encryptionTransform, keySize);
            MessageDigest sha1 = null;
            if (provider != null) {
                sha1 = MessageDigest.getInstance("SHA1", provider);
            } else {
                sha1 = MessageDigest.getInstance("SHA1");
            }

            pullStream = new EncryptionInputFilter(streamToEncode, cipher, sha1);
        } catch (GeneralSecurityException e) {
            throw new RuntimeException("Error initializing output transform: "
                    + e.getMessage(), e);
        }
    }
    
    private Cipher initCipher(String encryptionTransform, int keySize) throws GeneralSecurityException {
        Cipher cipher = null;
        if (provider != null) {
            cipher = Cipher.getInstance(encryptionTransform, provider);
        } else {
            cipher = Cipher.getInstance(encryptionTransform);
        }

        // Per FIPS bulletin 2013-09, make sure we don't use Dual_EC_DRBG
        SecureRandom rand;
        if(provider != null) {
            rand = SecureRandom.getInstance("SHA1PRNG", provider);
        } else {
            rand = SecureRandom.getInstance("SHA1PRNG");
        }

        // Generate a secret key
        String[] algParts = encryptionTransform.split("/");
        KeyGenerator keygen = null;
        if (provider != null) {
            keygen = KeyGenerator.getInstance(algParts[0], provider);
        } else {
            keygen = KeyGenerator.getInstance(algParts[0]);
        }

        keygen.init(keySize, rand);
        k = keygen.generateKey();
        //System.out.println("Key: " + KeyUtils.toHexPadded(k.getEncoded()));
        
        cipher.init(Cipher.ENCRYPT_MODE, k, rand);
        iv = cipher.getIV();
        //System.out.println("IV: " + KeyUtils.toHexPadded(iv));

        return cipher;
    }


    /*
     * (non-Javadoc)
     * 
     * @see com.emc.vipr.transform.OutputTransform#getEncodedMetadata()
     */
    @Override
    public Map<String, String> getEncodedMetadata() {
        Map<String, String> encodedMetadata = new HashMap<String, String>();
        
        encodedMetadata.putAll(metadataToEncode);
        
        // Add x-emc fields
        String encodedIv = KeyUtils.urlSafeEncodeBase64(iv);
        encodedMetadata.put(TransformConstants.META_ENCRYPTION_IV, encodedIv);
        encodedMetadata.put(TransformConstants.META_ENCRYPTION_KEY_ID, 
                masterEncryptionKeyFingerprint);
        try {
            encodedMetadata.put(TransformConstants.META_ENCRYPTION_OBJECT_KEY, 
                    KeyUtils.encryptKey(k, provider, masterKey.getPublic()));
        } catch (GeneralSecurityException e) {
            throw new RuntimeException("Could not encrypt key: " + e, e);
        }
        
        switch(getStreamMode()) {
        case PULL:
            EncryptionInputFilter is = (EncryptionInputFilter)pullStream;
            encodedMetadata.put(TransformConstants.META_ENCRYPTION_UNENC_SHA1, 
                    KeyUtils.toHexPadded(is.getDigest()));
            encodedMetadata.put(TransformConstants.META_ENCRYPTION_UNENC_SIZE, 
                    ""+is.getByteCount());            
            break;
        case PUSH:
            EncryptionOutputStream os = (EncryptionOutputStream)pushStream;
            encodedMetadata.put(TransformConstants.META_ENCRYPTION_UNENC_SHA1, 
                    KeyUtils.toHexPadded(os.getDigest()));
            encodedMetadata.put(TransformConstants.META_ENCRYPTION_UNENC_SIZE, 
                    ""+os.getByteCount());
            break;
        }
        
        // Sign x-emc fields.
        encodedMetadata.put(TransformConstants.META_ENCRYPTION_META_SIG, 
                KeyUtils.signMetadata(encodedMetadata, 
                        (RSAPrivateKey) masterKey.getPrivate(), provider));
        
        return encodedMetadata;
    }

}
