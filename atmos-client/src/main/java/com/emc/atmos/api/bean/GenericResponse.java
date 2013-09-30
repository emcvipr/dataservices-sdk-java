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

public class GenericResponse<T> extends BasicResponse {
    T content;

    public GenericResponse() {
    }

    public GenericResponse( T content ) {
        this.content = content;
    }

    public T getContent() {
        return content;
    }

    public void setContent( T content ) {
        this.content = content;
    }
}
