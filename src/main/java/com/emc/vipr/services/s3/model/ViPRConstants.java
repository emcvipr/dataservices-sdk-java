package com.emc.vipr.services.s3.model;

public interface ViPRConstants {

    // Header names
    static final String EMC_PREFIX = "x-emc-";

    static final String NAMESPACE_HEADER = "x-emc-namespace";
    static final String APPEND_OFFSET_HEADER = "x-emc-append-offset";
    static final String FILE_ACCESS_MODE_HEADER = "x-emc-file-access-mode";
    static final String FILE_ACCESS_PROTOCOL_HEADER = "x-emc-file-access-protocol";
    static final String FILE_ACCESS_DURATION_HEADER = "x-emc-file-access-duration";
    static final String HOST_LIST_HEADER = "x-emc-host-list";
    static final String USER_HEADER = "x-emc-user";
    static final String TOKEN_HEADER = "x-emc-token";
    static final String BUCKET_FILE_ACCESS_HEADER = "x-emc-bucket-file-access";
    static final String BUCKET_FILE_ACCESS_PROTOCOL_HEADER = "x-emc-bucket-file-access-protocol";
    static final String BUCKET_ACCESS_RESTRICTION_DURATION_HEADER = "x-emc-bucket-access-restriction-duration";

    // Parameter names
    static final String ACCESS_MODE_PARAMETER = "accessmode";
    static final String FILE_ACCESS_PARAMETER = "fileaccess";
    static final String MARKER_PARAMETER = "marker";
    static final String MAX_KEYS_PARAMETER = "max-keys";

    enum FileAccessMode {
        Disabled(false, null),
        ReadOnly(false, null),
        ReadWrite(false, null),
        switchingToDisabled(true, Disabled),
        switchingToReadOnly(true, ReadOnly),
        switchingToReadWrite(true, ReadWrite);

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
