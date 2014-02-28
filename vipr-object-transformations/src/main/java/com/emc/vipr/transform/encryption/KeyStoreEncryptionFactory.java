package com.emc.vipr.transform.encryption;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;

import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.emc.vipr.transform.TransformConstants;
import com.emc.vipr.transform.TransformException;

public class KeyStoreEncryptionFactory extends
        EncryptionTransformFactory<BasicEncryptionOutputTransform, BasicEncryptionInputTransform> {
    private static final Logger logger = LoggerFactory.getLogger(KeyStoreEncryptionFactory.class);

    private KeyStore keyStore;
    private String masterEncryptionKeyAlias;
    private String masterEncryptionKeyFingerprint;
    private char[] masterKeyPassword;
    private Map<String, String> idToAliasMap;

    public KeyStoreEncryptionFactory(KeyStore keyStore,
            String masterEncryptionKeyAlias, 
            char[] keyStorePassword) throws InvalidKeyException, 
                NoSuchAlgorithmException, NoSuchPaddingException, TransformException {
        this(keyStore, masterEncryptionKeyAlias, keyStorePassword, null);
    }

    public KeyStoreEncryptionFactory(KeyStore keyStore,
            String masterEncryptionKeyAlias, char[] masterKeyPassword, Provider provider) throws InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException, TransformException {
        super();
        this.keyStore = keyStore;
        this.masterEncryptionKeyAlias = masterEncryptionKeyAlias;
        this.masterKeyPassword = masterKeyPassword;
        this.idToAliasMap = new HashMap<String, String>();
        this.provider = provider;
        
        // Make sure the master encryption key alias exists.
        try {
            if(!keyStore.containsAlias(masterEncryptionKeyAlias)) {
                throw new InvalidKeyException("No certificate found in keystore for alias " + masterEncryptionKeyAlias);
            }
        } catch (KeyStoreException e) {
            throw new TransformException("Could not access KeyStore", e);
        }
        
        // Index all the certificate fingerprints
        try {
            for(Enumeration<String> aliases = keyStore.aliases(); aliases.hasMoreElements();) {
                String alias = aliases.nextElement();
                
                String fingerprint = getFingerprint(alias);
                idToAliasMap.put(fingerprint, alias);
                if(alias.equals(masterEncryptionKeyAlias)) {
                    masterEncryptionKeyFingerprint = fingerprint;
                }
            }
        } catch(KeyStoreException e) {
            throw new TransformException("Could not init factory from KeyStore", e);
        }
    }
    
    private String getFingerprint(String alias) throws KeyStoreException, NoSuchAlgorithmException {
        Certificate cert = keyStore.getCertificate(alias);
        if(cert instanceof X509Certificate) {
            // Get SKI
            byte[] ski = ((X509Certificate)cert).getExtensionValue("2.5.29.14");
            if(ski == null) {
                logger.debug("Certificate does not have SKI.  Computing fingerprint.");
                return KeyUtils.getRsaPublicKeyFingerprint((RSAPublicKey) cert.getPublicKey(),
                        provider);
            }
            String fingerprint = KeyUtils.toHexPadded(KeyUtils.extractSubjectKeyIdentifier(ski));
            logger.debug("Alias %s Subject Key Identifier: %s",
                    alias, fingerprint);
            return fingerprint;
        } else {
            // Compute the fingerprint
            return KeyUtils.getRsaPublicKeyFingerprint((RSAPublicKey) cert.getPublicKey(),
                    provider);
        }
        
    }

    @Override
    public Map<String, String> rekey(Map<String, String> metadata) throws TransformException, DoesNotNeedRekeyException{
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
        if (!idToAliasMap.containsKey(oldKeyId)) {
            throw new TransformException("Master key with fingerprint "
                    + oldKeyId + " not found");
        }
        
        String oldAlias = idToAliasMap.get(oldKeyId);
        KeyPair oldMasterKey = getKeyPair(oldAlias);
        String encodedKey = metadata.get(TransformConstants.META_ENCRYPTION_OBJECT_KEY);
        if(encodedKey == null) {
            throw new TransformException("Encrypted object key not found");
        }
        
        String algorithm = getEncryptionAlgorithm();
        
        SecretKey objectKey = KeyUtils.decryptKey(encodedKey, algorithm, provider, oldMasterKey.getPrivate());
        
        // Re-encrypt key with the current master key
        KeyPair newMasterKey = getKeyPair(masterEncryptionKeyAlias);
        String newKey;
        try {
            newKey = KeyUtils.encryptKey(objectKey, provider, newMasterKey.getPublic());
        } catch (GeneralSecurityException e) {
            throw new TransformException("Error encrypting key: " + e, e);
        }
        
        Map<String, String> newMetadata = new HashMap<String, String>();
        newMetadata.putAll(metadata);
        newMetadata.remove(TransformConstants.META_ENCRYPTION_META_SIG);
        newMetadata.put(TransformConstants.META_ENCRYPTION_OBJECT_KEY, newKey);
        newMetadata.put(TransformConstants.META_ENCRYPTION_KEY_ID, 
                masterEncryptionKeyFingerprint);
        
        // Re-sign
        String signature = KeyUtils.signMetadata(newMetadata, 
                (RSAPrivateKey) newMasterKey.getPrivate(), provider);
        newMetadata.put(TransformConstants.META_ENCRYPTION_META_SIG, signature);
   
        return newMetadata;
    }
    
    private KeyPair getKeyPair(String alias) throws TransformException {
        Certificate keyCert;
        PrivateKey privateKey;
        try {
            keyCert = keyStore.getCertificate(alias);
            privateKey =  (PrivateKey) keyStore.getKey(alias, 
                masterKeyPassword);
            if(keyCert == null) {
                throw new TransformException("Certificate for alias " + 
                        masterEncryptionKeyAlias + " not found");
            }
            if(privateKey == null) {
                throw new TransformException("Private key for alias " + 
                        masterEncryptionKeyAlias + " not found");
            }
        } catch (KeyStoreException e) {
            throw new TransformException("Could not access keystore", e);
        } catch(UnrecoverableKeyException e) {
            throw new TransformException("Error loading private key from keystore", e);
        } catch(NoSuchAlgorithmException e) {
            throw new TransformException("Error loading private key from keystore", e);
        }
        return new KeyPair(keyCert.getPublicKey(), privateKey);
    }

    @Override
    public BasicEncryptionOutputTransform getOutputTransform(
            OutputStream streamToEncodeTo, Map<String, String> metadataToEncode)
            throws IOException, TransformException {
        // Load the key
        KeyPair asymmetricKey = getKeyPair(masterEncryptionKeyAlias);
        
        return new BasicEncryptionOutputTransform(streamToEncodeTo, metadataToEncode, 
                masterEncryptionKeyFingerprint, asymmetricKey, encryptionTransform, 
                keySize, provider);
    }
    
    @Override
    public BasicEncryptionOutputTransform getOutputTransform(
            InputStream streamToEncode, Map<String, String> metadataToEncode)
            throws IOException, TransformException {
        // Load the key
        KeyPair asymmetricKey = getKeyPair(masterEncryptionKeyAlias);
        
        return new BasicEncryptionOutputTransform(streamToEncode, metadataToEncode, 
                masterEncryptionKeyFingerprint, asymmetricKey, encryptionTransform, 
                keySize, provider);
    }

    @Override
    public BasicEncryptionInputTransform getInputTransform(
            String transformConfig, InputStream streamToDecode,
            Map<String, String> metadata) throws IOException, TransformException {
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
        String masterKeyAlias = idToAliasMap.get(masterKeyId);
        if(masterKeyAlias == null) {
            throw new TransformException("Could not find master key for ID " + masterKeyId);
        }
        
        KeyPair asymmetricKey = getKeyPair(masterKeyAlias);
        
        return new BasicEncryptionInputTransform(transformTuple[1], streamToDecode, 
                metadata, asymmetricKey, provider);
    }

    public String getMasterEncryptionKeyAlias() {
        return masterEncryptionKeyAlias;
    }

    public void setMasterEncryptionKeyAlias(String alias) throws TransformException {
        try {
            // Make sure it exists
            if(!keyStore.containsAlias(alias)) {
                throw new TransformException("Certificate with alias " + alias + " not found in keystore");
            }
            
            // Get the fingerprint too
            String fingerprint = getFingerprint(alias);
            masterEncryptionKeyFingerprint = fingerprint;
            masterEncryptionKeyAlias = alias;
        } catch (KeyStoreException e) {
            throw new TransformException("Could not access keystore", e);
        } catch (NoSuchAlgorithmException e) {
            throw new TransformException("Could not load certificate for alias " + alias );
        }
    }
    
}