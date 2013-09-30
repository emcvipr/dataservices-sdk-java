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

import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.Region;

/**
 * Enhanced create bucket request that contains ViPR-specific parameters, specifically
 * the optional ViPR Project ID and Object Virtual Pool ID for the new bucket.
 */
public class ViPRCreateBucketRequest extends CreateBucketRequest {
    public String projectId;
    public String vpoolId;

    /**
     * @see CreateBucketRequest#CreateBucketRequest(String)
     */
    public ViPRCreateBucketRequest(String bucketName) {
        super(bucketName);
    }

    /**
     * @see CreateBucketRequest#CreateBucketRequest(String, Region)
     */
    public ViPRCreateBucketRequest(String bucketName, Region region) {
        super(bucketName, region);
    }

    /**
     * @see CreateBucketRequest#CreateBucketRequest(String, String)
     */
    public ViPRCreateBucketRequest(String bucketName, String region) {
        super(bucketName, region);
    }

    /**
     * Gets the ViPR Project ID for this request.  May be null.
     */
    public String getProjectId() {
        return projectId;
    }

    /**
     * Sets the ViPR Project ID for the new subtenant.  If null, the default project
     * for the current tenant's namespace will be used.  If null and the namespace does
     * not have a default project the request will fail.
     * @param projectId the ID (a URN) of the project for the new subtenant.
     */
    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    /**
     * Gets the virtual pool ID for this request.  May be null.
     */
    public String getVpoolId() {
        return vpoolId;
    }

    /**
     * Sets the ViPR Object Virtual Pool ID for the new namespace.  If null, the default
     * object virtual pool for the current tenant's namespace will be used.
     * @param vpoolId the ID (a URN) of the Object Virtual Pool for the new
     * subtenant.
     */
    public void setVpoolId(String vpoolId) {
        this.vpoolId = vpoolId;
    }

}
