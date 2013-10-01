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
package com.emc.atmos.api.bean;

import com.emc.atmos.api.Acl;
import com.emc.atmos.api.RestUtil;

public class GetAccessTokenResponse extends BasicResponse {
    AccessToken token;
    Acl acl;

    public GetAccessTokenResponse() {
    }

    public GetAccessTokenResponse( AccessToken token ) {
        this.token = token;
    }

    public AccessToken getToken() {
        return token;
    }

    public void setToken( AccessToken token ) {
        this.token = token;
    }

    public synchronized Acl getAcl() {
        if ( acl == null ) {
            acl = new Acl( RestUtil.parseAclHeader( getFirstHeader( RestUtil.XHEADER_USER_ACL ) ),
                           RestUtil.parseAclHeader( getFirstHeader( RestUtil.XHEADER_GROUP_ACL ) ) );
        }
        return acl;
    }
}
