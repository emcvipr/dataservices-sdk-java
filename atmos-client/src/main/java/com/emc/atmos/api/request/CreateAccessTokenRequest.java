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

import com.emc.atmos.api.Acl;
import com.emc.atmos.api.ObjectId;
import com.emc.atmos.api.ObjectPath;
import com.emc.atmos.api.RestUtil;
import com.emc.atmos.api.bean.AccessTokenPolicy;
import com.emc.util.HttpUtil;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Represents a request to create an anonymous access token.
 */
public class CreateAccessTokenRequest extends ObjectRequest<CreateAccessTokenRequest> implements ContentRequest {
    protected Acl acl;
    protected AccessTokenPolicy policy;

    @Override
    public String getServiceRelativePath() {
        return "accesstokens";
    }

    @Override
    public String getMethod() {
        return "POST";
    }

    @Override
    public Map<String, List<Object>> generateHeaders() {
        Map<String, List<Object>> headers = super.generateHeaders();

        // target object
        if ( identifier != null ) {
            if ( identifier instanceof ObjectId )
                RestUtil.addValue( headers, RestUtil.XHEADER_OBJECTID, identifier );
            else if ( identifier instanceof ObjectPath ) {
                // enable UTF-8
                RestUtil.addValue( headers, RestUtil.XHEADER_UTF8, "true" );
                RestUtil.addValue( headers, RestUtil.XHEADER_PATH, HttpUtil.encodeUtf8( identifier.toString() ) );
            } else
                throw new UnsupportedOperationException(
                        "Only object ID and path are currently supported in access tokens" );
        }

        // acl (applied to uploads)
        if ( acl != null ) {
            headers.put( RestUtil.XHEADER_USER_ACL, acl.getUserAclHeader() );
            headers.put( RestUtil.XHEADER_GROUP_ACL, acl.getGroupAclHeader() );
        }

        return headers;
    }

    @Override
    protected CreateAccessTokenRequest me() {
        return this;
    }

    @Override
    public String getContentType() {
        return "application/xml";
    }

    @Override
    public Object getContent() {
        return policy;
    }

    @Override
    public long getContentLength() {
        return -1;
    }

    /**
     * Builder method for {@link #setAcl(com.emc.atmos.api.Acl)}
     */
    public CreateAccessTokenRequest acl( Acl acl ) {
        this.acl = acl;
        return this;
    }

    /**
     * Builder method for {@link #setPolicy(com.emc.atmos.api.bean.AccessTokenPolicy)}
     */
    public CreateAccessTokenRequest policy( AccessTokenPolicy policy ) {
        this.policy = policy;
        return this;
    }

    /**
     * Gets the ACL that will be assigned to objects created using this access token.
     */
    public Acl getAcl() {
        return acl;
    }

    /**
     * Gets the token policy for the new access token.
     */
    public AccessTokenPolicy getPolicy() {
        return policy;
    }

    /**
     * Sets the ACL that will be assigned to objects created using this access token.
     */
    public void setAcl( Acl acl ) {
        this.acl = acl;
    }

    /**
     * Sets the token policy for the new access token.
     */
    public void setPolicy( AccessTokenPolicy policy ) {
        this.policy = policy;
    }
}
