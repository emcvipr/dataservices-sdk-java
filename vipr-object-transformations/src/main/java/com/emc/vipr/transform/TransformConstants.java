package com.emc.vipr.transform;

public interface TransformConstants {
    // Some predefined transformation classes
    public static final String ENCRYPTION_CLASS = "ENC";
    public static final String COMPRESSION_CLASS = "COMP";

    public static final Integer DEFAULT_ENCRYPTION_PRIORITY = 100;
    public static final Integer DEFAULT_COMPRESSION_PRIORITY = 1000;
    
    public static final String METADATA_PREFIX = "x-emc-";
    
    public static final String META_TRANSFORM_MODE = METADATA_PREFIX + "transform-mode";
    
    //////////////////////////
    // Encryption Constants //
    //////////////////////////
    public static final String DEFAULT_ENCRYPTION_TRANSFORM = "AES/CBC/PKCS5Padding";
    public static final int DEFAULT_ENCRYPTION_KEY_SIZE = 128;
    public static final String METADATA_SIGNATURE_ALGORITHM = "SHA256withRSA";
    public static final String KEY_ENCRYPTION_TRANSFORM = "RSA/ECB/OAEPWithSHA-1AndMGF1Padding";
    
    public static final String META_ENCRYPTION_KEY_ID = METADATA_PREFIX + "enc-key-id";
    public static final String META_ENCRYPTION_OBJECT_KEY = METADATA_PREFIX + "enc-object-key";
    public static final String META_ENCRYPTION_IV = METADATA_PREFIX + "iv";
    public static final String META_ENCRYPTION_UNENC_SIZE = METADATA_PREFIX + "enc-unencrypted-size";
    public static final String META_ENCRYPTION_UNENC_SHA1 = METADATA_PREFIX + "enc-unencrypted-sha1";
    public static final String META_ENCRYPTION_META_SIG = METADATA_PREFIX + "enc-metadata-signature";
    
    ///////////////////////////
    // Compression Constants //
    ///////////////////////////
    public enum CompressionMode { LZMA, Deflate, NONE };
    
    public static final CompressionMode DEFAULT_COMPRESSION_MODE = CompressionMode.Deflate;
    public static final int DEFAULT_COMPRESSION_LEVEL = 5;
    
    public static final String META_COMPRESSION_UNCOMP_SIZE = METADATA_PREFIX + "comp-uncompressed-size";
    public static final String META_COMPRESSION_COMP_SIZE = METADATA_PREFIX + "comp-compressed-size";
    public static final String META_COMPRESSION_COMP_RATIO = METADATA_PREFIX + "comp-compression-ratio";
    public static final String META_COMPRESSION_UNCOMP_SHA1 = METADATA_PREFIX + "comp-uncompressed-sha1";

}
