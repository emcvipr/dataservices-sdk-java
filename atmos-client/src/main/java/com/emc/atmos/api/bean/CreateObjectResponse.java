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

import com.emc.atmos.api.ChecksumValue;
import com.emc.atmos.api.ChecksumValueImpl;
import com.emc.atmos.api.ObjectId;
import com.emc.atmos.api.RestUtil;

public class CreateObjectResponse extends BasicResponse {
    public ObjectId getObjectId() {
        return RestUtil.parseObjectId( location );
    }

    public ChecksumValue getWsChecksum() {
        return new ChecksumValueImpl( getFirstHeader( RestUtil.XHEADER_WSCHECKSUM ) );
    }

    public ChecksumValue getServerGeneratedChecksum() {
        return new ChecksumValueImpl( getFirstHeader( RestUtil.XHEADER_CONTENT_CHECKSUM ) );
    }
}
