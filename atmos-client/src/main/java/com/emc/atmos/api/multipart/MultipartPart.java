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
package com.emc.atmos.api.multipart;

import com.emc.atmos.api.Range;

/**
 * Represents one part of a multipart response. Each part has a content-type, byte range and data.
 */
public class MultipartPart {
    private String contentType;
    private Range contentRange;
    private byte[] data;

    public MultipartPart( String contentType, Range contentRange, byte[] data ) {
        this.contentType = contentType;
        this.contentRange = contentRange;
        this.data = data;
    }

    public String getContentType() {
        return contentType;
    }

    public Range getContentRange() {
        return contentRange;
    }

    public byte[] getData() {
        return data;
    }
}
