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
 * The progress listener interface is used to report upload and download
 * progress.  Create a class that implements this interface and register it
 * with the helper by calling addListener().
 */
public interface ProgressListener {
    /**
     * This method will be called after a chunk has been transferred.
     * @param currentBytes the number of bytes transferred.  Note that
     * the value will be passed as a string since it may be >2GB.
     * @param bytesTotal the total number of bytes to transfer.  If
     * the total number of bytes is unknown, this value will be -1.  Note that
     * the value will be passed as a string since it may be >2GB.
     */
    public void onProgress( long currentBytes, long bytesTotal );
    
    /**
     * This callback will be invoked after the transfer has completed.
     */
    public void onComplete();
    
    /**
     * This callback will be invoked if there is an error during the transfer.
     * @param exception the exception that caused the transfer to
     * fail.
     */
    public void onError( Exception exception );

}
