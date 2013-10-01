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
package com.emc.atmos.api;

/**
 * Represents a static checksum value.
 */
public class ChecksumValueImpl extends ChecksumValue {
    private ChecksumAlgorithm algorithm;
    private long offset;
    private String value;

    public ChecksumValueImpl( ChecksumAlgorithm algorithm, long offset, String value ) {
        this.algorithm = algorithm;
        this.offset = offset;
        this.value = value;
    }

    /**
     * Constructs a new checksum value from a header string of the format <code>{algorithm}/[{offset}/]{value}</code>,
     * where the offset may or may not be present.
     */
    public ChecksumValueImpl( String headerValue ) {
        String[] parts = headerValue.split( "/" );
        this.algorithm = ChecksumAlgorithm.valueOf( parts[0] );
        if ( parts.length > 2 ) {
            this.offset = Long.parseLong( parts[1] );
            this.value = parts[2];
        } else {
            this.value = parts[1];
        }
    }

    public ChecksumAlgorithm getAlgorithm() {
        return algorithm;
    }

    public long getOffset() {
        return offset;
    }

    public String getValue() {
        return value;
    }
}
