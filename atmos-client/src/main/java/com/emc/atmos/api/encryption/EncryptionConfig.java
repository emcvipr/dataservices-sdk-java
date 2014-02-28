/*
 * Copyright 2013 EMC Corporation. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.emc.atmos.api.encryption;

import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.util.Set;

import javax.crypto.NoSuchPaddingException;

import com.emc.vipr.transform.TransformConstants;
import com.emc.vipr.transform.TransformException;
import com.emc.vipr.transform.encryption.BasicEncryptionInputTransform;
import com.emc.vipr.transform.encryption.BasicEncryptionOutputTransform;
import com.emc.vipr.transform.encryption.BasicEncryptionTransformFactory;
import com.emc.vipr.transform.encryption.EncryptionTransformFactory;
import com.emc.vipr.transform.encryption.KeyStoreEncryptionFactory;

/**
 * Creates an encryption configuration for use with the {@link AtmosEncryptionClient}.
 * Both keystore keys and bare RSA KeyPairs are supported.
 */
public class EncryptionConfig {
    private EncryptionTransformFactory<BasicEncryptionOutputTransform, BasicEncryptionInputTransform> factory;

    /**
     * Creates a new EncryptionConfig object that will retrieve keys from a Keystore
     * object.
     * @param keystore the Keystore containing the master encryption key and any
     * additional decryption key(s).
     * @param masterKeyPassword password for the master keys.  Note that this
     * implementation assumes that all master keys use the same password.
     * @param masterKeyAlias name of the master encryption key in the Keystore object.
     * @param provider (optional) if not-null, the Provider object to use for all 
     * encryption operations.  If null, the default provider(s) will be used from your
     * java.security file.
     * @param keySize size of encryption key to use, either 128 or 256.  Note that to use
     * 256-bit AES keys, you will probably need the unlimited strength jurisdiction files
     * installed in your JRE. 
     * @throws InvalidKeyException if the master encryption key cannot be loaded.
     * @throws NoSuchAlgorithmException if the AES encryption algorithm is not available.
     * @throws NoSuchPaddingException if PKCS5Padding is not available.
     * @throws TransformException if some other error occurred initializing the encryption.
     */
    public EncryptionConfig(KeyStore keystore, char[] masterKeyPassword, 
            String masterKeyAlias, Provider provider, int keySize) 
                    throws InvalidKeyException, NoSuchAlgorithmException, 
                    NoSuchPaddingException, TransformException {
        if(provider == null) {
            factory = new KeyStoreEncryptionFactory(keystore, masterKeyAlias, masterKeyPassword);
        } else {
            factory = new KeyStoreEncryptionFactory(keystore, masterKeyAlias, masterKeyPassword, provider);
        }
        factory.setEncryptionSettings(TransformConstants.DEFAULT_ENCRYPTION_TRANSFORM, keySize, provider);
    }
    
    /**
     * Creates a new EncryptionConfig object that uses bare KeyPair objects.
     * @param masterEncryptionKey the KeyPair to use for encryption.
     * @param decryptionKeys (optional) additional KeyPair objects available to 
     * decrypt objects.
     * @param provider (optional) if not-null, the Provider object to use for all 
     * encryption operations.  If null, the default provider(s) will be used from your
     * java.security file.
     * @param keySize size of encryption key to use, either 128 or 256.  Note that to use
     * 256-bit AES keys, you will probably need the unlimited strength jurisdiction files
     * installed in your JRE. 
     * @throws InvalidKeyException if the master encryption key is not valid
     * @throws NoSuchAlgorithmException if the AES encryption algorithm is not available.
     * @throws NoSuchPaddingException if PKCS5Padding is not available.
     * @throws TransformException if some other error occurred initializing the encryption.
     */
    public EncryptionConfig(KeyPair masterEncryptionKey, Set<KeyPair> decryptionKeys, 
            Provider provider, int keySize)
                    throws InvalidKeyException, NoSuchAlgorithmException, 
                    NoSuchPaddingException, TransformException {

        if(provider == null) {
            factory = new BasicEncryptionTransformFactory(masterEncryptionKey, decryptionKeys);
        } else {
            factory = new BasicEncryptionTransformFactory(masterEncryptionKey, decryptionKeys, provider);
        }
        factory.setEncryptionSettings(TransformConstants.DEFAULT_ENCRYPTION_TRANSFORM, keySize, provider);
    }

    /**
     * Returns the configured EncryptionTransformFactory.
     * @return the configured EncryptionTransformFactory.
     */
    public EncryptionTransformFactory<BasicEncryptionOutputTransform, BasicEncryptionInputTransform> getFactory() {
        return factory;
    }

}
