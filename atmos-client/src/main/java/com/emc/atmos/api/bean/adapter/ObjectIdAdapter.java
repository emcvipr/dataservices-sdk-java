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
package com.emc.atmos.api.bean.adapter;

import com.emc.atmos.api.ObjectId;

import javax.xml.bind.annotation.adapters.XmlAdapter;

public class ObjectIdAdapter extends XmlAdapter<String, ObjectId> {

    @Override
    public ObjectId unmarshal( String s ) throws Exception {
        return new ObjectId( s );
    }

    @Override
    public String marshal( ObjectId objectId ) throws Exception {
        return objectId.toString();
    }
}
