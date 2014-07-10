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

public interface ViPRConstants {

    // Header names
    static final String EMC_PREFIX = "x-emc-";

    static final String NAMESPACE_HEADER = "x-emc-namespace";
    static final String APPEND_OFFSET_HEADER = "x-emc-append-offset";
    static final String FILE_ACCESS_MODE_HEADER = "x-emc-file-access-mode";
    static final String FILE_ACCESS_DURATION_HEADER = "x-emc-file-access-duration";
    static final String FILE_ACCESS_HOST_LIST_HEADER = "x-emc-file-access-host-list";
    static final String FILE_ACCESS_UID_HEADER = "x-emc-file-access-uid";
    static final String FILE_ACCESS_TOKEN_HEADER = "x-emc-file-access-token";
    static final String FILE_ACCESS_START_TOKEN_HEADER = "x-emc-file-access-start-token";
    static final String FILE_ACCESS_END_TOKEN_HEADER = "x-emc-file-access-end-token";
    static final String FILE_ACCESS_PRESERVE_INGEST_PATHS = "x-emc-file-access-on-ingestion-device";

    static final String FS_ACCESS_ENABLED = "x-emc-file-system-access-enabled";
    
    static final String PROJECT_HEADER = "x-emc-project-id";
    static final String VPOOL_HEADER = "x-emc-vpool";

    // Parameter names
    static final String ACCESS_MODE_PARAMETER = "accessmode";
    static final String FILE_ACCESS_PARAMETER = "fileaccess";
    static final String ENDPOINT_PARAMETER = "endpoint";
    static final String MARKER_PARAMETER = "marker";
    static final String MAX_KEYS_PARAMETER = "max-keys";

    enum FileAccessMode {
        disabled(false, null),
        readOnly(false, null),
        readWrite(false, null),
        switchingToDisabled(true, disabled),
        switchingToReadOnly(true, readOnly),
        switchingToReadWrite(true, readWrite);

        private boolean transitionState;
        private FileAccessMode targetState;

        private FileAccessMode(boolean transitionState, FileAccessMode targetState) {
            this.transitionState = transitionState;
            this.targetState = targetState;
        }

        public boolean isTransitionState() {
            return transitionState;
        }

        public boolean transitionsToTarget(FileAccessMode targetState) {
            return targetState == this.targetState;
        }
    }

    enum FileAccessProtocol {NFS, CIFS}
}
