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
package com.emc.atmos.api.request;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.emc.atmos.api.RestUtil;

/**
 * Represents a request to create a new Atmos Subtenant in EMC ViPR.
 */
public class CreateSubtenantRequest extends Request {
    private String projectId;
    private String objectVirtualPoolId;

    public CreateSubtenantRequest() {
    }

    @Override
    public String getServiceRelativePath() {
        return "/subtenant";
    }

    @Override
    public String getMethod() {
        return "PUT";
    }

    @Override
    public Map<String, List<Object>> generateHeaders() {
        Map<String, List<Object>> headers = new HashMap<String, List<Object>>();
        if(projectId != null) {
            headers.put(RestUtil.XHEADER_PROJECT, Arrays.asList(new Object[]{ projectId }));
        }
        if(objectVirtualPoolId != null) {
            headers.put(RestUtil.XHEADER_OBJECT_VPOOL, 
                    Arrays.asList(new Object[]{ objectVirtualPoolId }));
            
        }
        return headers;
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
     * @param project the ID (a URN) of the project for the new subtenant.
     */
    public void setProjectId(String project) {
        this.projectId = project;
    }

    /**
     * Gets the virtual pool ID for this request.  May be null.
     */
    public String getObjectVirtualPoolId() {
        return objectVirtualPoolId;
    }

    /**
     * Sets the ViPR Object Virtual Pool ID for the new namespace.  If null, the default
     * object virtual pool for the current tenant's namespace will be used.
     * @param objectVirtualPoolId the ID (a URN) of the Object Virtual Pool for the new 
     * subtenant.
     */
    public void setObjectVirtualPoolId(String objectVirtualPoolId) {
        this.objectVirtualPoolId = objectVirtualPoolId;
    }

}
