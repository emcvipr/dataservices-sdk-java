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

import com.emc.vipr.transform.TransformConstants;
import com.emc.vipr.transform.TransformConstants.CompressionMode;
import com.emc.vipr.transform.compression.CompressionTransformFactory;

/**
 * Contains the configuration for compression transformation in the Atmos client.  Note
 * that while LZMA compression produces high compression ratios it requires large 
 * amounts of RAM.  In particular, even level 5 can exceed the default Java heap size.
 * @see CompressionTransformFactory#memoryRequiredForLzma(int) to estimate the amount
 * of RAM required for LZMA.
 * 
 */
public class CompressionConfig {
    private CompressionTransformFactory factory;
    
    /**
     * Creates the default compression configuration (Deflate/5)
     */
    public CompressionConfig() {
        this(TransformConstants.DEFAULT_COMPRESSION_MODE, 
                TransformConstants.DEFAULT_COMPRESSION_LEVEL);
    }

    /**
     * Creates the compression configuration.
     * @param mode the compression mode (e.g. LZMA or Deflate)
     * @param level the compression level 1-9.
     */
    public CompressionConfig(CompressionMode mode, int level) {
        factory = new CompressionTransformFactory();
        factory.setCompressMode(mode);
        factory.setCompressionLevel(level);
    }
    
    /**
     * Returns the {@link CompressionTransformFactory} with the current compression
     * configuration.
     * @return a configured {@link CompressionTransformFactory}
     */
    public CompressionTransformFactory getFactory() {
        return factory;
    }

}
