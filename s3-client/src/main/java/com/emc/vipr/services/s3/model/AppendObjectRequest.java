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

import java.io.File;
import java.io.InputStream;

import com.amazonaws.services.s3.model.ObjectMetadata;

public class AppendObjectRequest extends UpdateObjectRequest {
	private static final String UPDATE_RANGE = "-1-";

	public AppendObjectRequest(String bucketName, String key, File file) {
		super(bucketName, key, file);
		
		setUpdateRange(UPDATE_RANGE);
	}

	public AppendObjectRequest(String bucketName, String key,
			InputStream input, ObjectMetadata metadata) {
		super(bucketName, key, input, metadata);
		
		setUpdateRange(UPDATE_RANGE);
	}

	public AppendObjectRequest(String bucketName, String key,
			String redirectLocation) {
		super(bucketName, key, redirectLocation);
		
		setUpdateRange(UPDATE_RANGE);
	}

}
