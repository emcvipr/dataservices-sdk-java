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
package com.emc.vipr.services.s3;

import com.emc.vipr.services.s3.model.ListDataNodesRequest;
import com.emc.vipr.services.s3.model.ListDataNodesResult;
import org.junit.Assert;
import org.junit.Test;

public class EndpointTest extends AbstractViPRS3Test {
    @Test
    public void testEndpoints() throws Exception {
        ListDataNodesResult result = viprS3.listDataNodes(new ListDataNodesRequest());
        Assert.assertNotNull("list-data-node-result is null", result);
        Assert.assertTrue("multiple data nodes not returned", result.getHosts().size() > 1);
        Assert.assertTrue("No version in response", result.getVersion().length() > 0);
    }
}
