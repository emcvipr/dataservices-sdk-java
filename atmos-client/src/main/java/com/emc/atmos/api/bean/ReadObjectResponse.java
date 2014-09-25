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
import com.emc.atmos.api.ChecksumValue;
import com.emc.atmos.api.ChecksumValueImpl;
import com.emc.atmos.api.RestUtil;

import java.util.Map;
import java.util.TreeMap;

public class ReadObjectResponse<T> extends BasicResponse {
    private T object;
    private ObjectMetadata metadata;

    public ReadObjectResponse() {
    }

    public ReadObjectResponse( T object ) {
        this.object = object;
    }

    public T getObject() {
        return object;
    }

    public void setObject( T object ) {
        this.object = object;
    }

    public synchronized ObjectMetadata getMetadata() {
        if ( metadata == null ) {
            Acl acl = new Acl( RestUtil.parseAclHeader( getFirstHeader( RestUtil.XHEADER_USER_ACL ) ),
                               RestUtil.parseAclHeader( getFirstHeader( RestUtil.XHEADER_GROUP_ACL ) ) );

            Map<String, Metadata> metaMap = new TreeMap<String, Metadata>();
            metaMap.putAll( RestUtil.parseMetadataHeader( getFirstHeader( RestUtil.XHEADER_META ), false ) );
            metaMap.putAll( RestUtil.parseMetadataHeader( getFirstHeader( RestUtil.XHEADER_LISTABLE_META ), true ) );

            String wsChecksumHeader = getFirstHeader( RestUtil.XHEADER_WSCHECKSUM );
            ChecksumValue wsChecksum = wsChecksumHeader == null ? null : new ChecksumValueImpl( wsChecksumHeader );
            String serverChecksumHeader = getFirstHeader( RestUtil.XHEADER_CONTENT_CHECKSUM );
            ChecksumValue serverChecksum = serverChecksumHeader == null ? null :
                                           new ChecksumValueImpl( getFirstHeader( RestUtil.XHEADER_CONTENT_CHECKSUM ) );

            metadata = new ObjectMetadata( metaMap, acl, getContentType(), wsChecksum, serverChecksum );
        }
        return metadata;
    }

    public ChecksumValue getWsChecksum() {
        return getMetadata().getWsChecksum();
    }

    public ChecksumValue getServerGeneratedChecksum() {
        return getMetadata().getServerChecksum();
    }
}
