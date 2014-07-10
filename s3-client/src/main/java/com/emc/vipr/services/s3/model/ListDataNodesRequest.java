/*
 * Copyright 2014 EMC Corporation. All Rights Reserved.
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

import com.amazonaws.AmazonWebServiceRequest;

/**
 * Holds request data for a ViPR list-data-nodes request. Only exists to allow standard AWS request parameters as
 * generic request class was not found.
 */
public class ListDataNodesRequest extends AmazonWebServiceRequest {
}
