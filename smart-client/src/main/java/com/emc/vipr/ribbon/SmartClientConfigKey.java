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
package com.emc.vipr.ribbon;

import com.netflix.client.config.IClientConfigKey;

public enum SmartClientConfigKey implements IClientConfigKey {
    ViPRDataServicesProtocol("ViPRDataServicesProtocol"),
    ViPRDataServicesInitialNodes("ViPRDataServicesInitialNodes"),
    ViPRDataServicesUser("ViPRDataServicesUser"),
    ViPRDataServicesUserSecret("ViPRDataServicesUserSecret"),
    ViPRDataServicesTimeout("ViPRDataServicesTimeout");

    private String key;

    private SmartClientConfigKey(String key) {
        this.key = key;
    }

    @Override
    public String key() {
        return key;
    }
}
