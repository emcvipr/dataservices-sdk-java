package com.emc.vipr.services.s3.model;

public interface ViPRConstants {
	public enum FileAccessMode { Disabled, ReadOnly, ReadWrite, SwitchingToDisabled, SwitchingToReadOnly, SwitchingToReadWrite };
	public enum FileAccessProtocol { NFS, CIFS };
	
}
