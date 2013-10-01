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
package com.emc.vipr.services.s3;

import com.amazonaws.AmazonWebServiceResponse;
import com.amazonaws.http.HttpResponse;
import com.amazonaws.services.s3.internal.S3ObjectResponseHandler;
import com.amazonaws.services.s3.model.S3Object;

/**
 * AWS code looks for a hyphen in the ETag to signify a multipart upload, in which case it will not use the ETag as a
 * checksum. ViPR v1 returns "00" in the ETag for objects uploaded via multipart (no hyphen). This handler will add the
 * hyphen to the ETag so that it is correctly interpreted as a multipart upload and the ETag is ignored. Otherwise
 * it is treated as a checksum and an exception is thrown when it fails to validate.
 * TODO: remove post v1 when ViPR will return a hyphen in the ETag for multipart uploads
 */
public class ViPRS3ObjectResponseHandler extends S3ObjectResponseHandler {
    @Override
    public AmazonWebServiceResponse<S3Object> handle(HttpResponse response) throws Exception {
        // check for "00" in the ETag and append a hyphen so AWS code knows this was a multipart upload (does not have
        // an accurate MD5 checksum in the ETag).
        String etag = response.getHeaders().get("ETag");
        if (etag != null && etag.matches("\"?[0-9][0-9]\"?"))
            response.getHeaders().put("ETag", etag + "-");

        return super.handle(response);
    }
}
