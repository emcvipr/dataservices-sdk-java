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
package com.emc.esu.api;

/**
 * A buffer segment is used to select a range of bytes from within an existing
 * byte array.
 */
public class BufferSegment {
    private byte[] buffer;
    private int offset;
    private int size;

    /**
     * Creates a new BufferSegment.
     * @param buffer the byte array
     * @param offset starting offset into the byte array in bytes
     * @param size the number of bytes in the segment
     */
    public BufferSegment( byte[] buffer, int offset, int size ) {
        this.buffer = buffer;
        this.offset = offset;
        this.size = size;
    }
    
    /**
     * Creates a BufferSegment that specifies the whole byte array (offset=0
     * and size=buffer.length).
     * @param buffer the byte array
     */
    public BufferSegment( byte[] buffer ) {
        this.buffer = buffer;
        this.offset = 0;
        this.size = buffer.length;
    }

    /**
     * @return the buffer
     */
    public byte[] getBuffer() {
        return buffer;
    }

    /**
     * @param buffer the buffer to set
     */
    public void setBuffer(byte[] buffer) {
        this.buffer = buffer;
    }

    /**
     * @return the offset
     */
    public int getOffset() {
        return offset;
    }

    /**
     * @param offset the offset to set
     */
    public void setOffset(int offset) {
        this.offset = offset;
    }

    /**
     * @return the size
     */
    public int getSize() {
        return size;
    }

    /**
     * @param size the size to set
     */
    public void setSize(int size) {
        this.size = size;
    }
}
