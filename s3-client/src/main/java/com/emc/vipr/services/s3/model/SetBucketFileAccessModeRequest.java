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
package com.emc.vipr.services.s3.model;

import com.amazonaws.AmazonWebServiceRequest;
import com.emc.vipr.services.s3.model.ViPRConstants.FileAccessMode;

import java.util.Arrays;
import java.util.List;

public class SetBucketFileAccessModeRequest extends AmazonWebServiceRequest {
    private String bucketName;
    private FileAccessMode accessMode;
    private long duration = -1;
    private List<String> hostList;
    private String uid;
    private String token;

    private static final FileAccessMode[] ALLOWED_ACCESS_MODES = {
            FileAccessMode.disabled, FileAccessMode.readOnly, FileAccessMode.readWrite};

    /**
     * @return the accessMode
     */
    public FileAccessMode getAccessMode() {
        return accessMode;
    }

    /**
     * @param accessMode the accessMode to set
     */
    public void setAccessMode( FileAccessMode accessMode ) {
        if ( !Arrays.asList( ALLOWED_ACCESS_MODES ).contains( accessMode ) )
            throw new IllegalArgumentException( "Access mode must be one of "
                                                + Arrays.toString( ALLOWED_ACCESS_MODES ) );
        this.accessMode = accessMode;
    }

    /**
     * @return the duration
     */
    public long getDuration() {
        return duration;
    }

    /**
     * @param duration the duration to set
     */
    public void setDuration(long duration) {
        this.duration = duration;
    }

    /**
     * @return the hostList
     */
    public List<String> getHostList() {
        return hostList;
    }

    /**
     * @param hostList the hostList to set
     */
    public void setHostList( List<String> hostList ) {
        this.hostList = hostList;
    }

    /**
     * @return the uid
     */
    public String getUid() {
        return uid;
    }

    /**
     * @param uid the uid to set
     */
    public void setUid( String uid ) {
        this.uid = uid;
    }

    /**
     * @return the token
     */
    public String getToken() {
        return token;
    }

    /**
     * @param token the token to set
     */
    public void setToken( String token ) {
        this.token = token;
    }

    /**
     * @return the bucketName
     */
    public String getBucketName() {
        return bucketName;
    }

    /**
     * @param bucketName the bucketName to set
     */
    public void setBucketName( String bucketName ) {
        this.bucketName = bucketName;
    }
}
