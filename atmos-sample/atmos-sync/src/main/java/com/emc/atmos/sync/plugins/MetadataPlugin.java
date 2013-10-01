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
package com.emc.atmos.sync.plugins;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

/**
 * @author cwikj
 *
 */
public class MetadataPlugin extends SyncPlugin {
	public static final String ADD_META_OPTION = "add-meta";
	public static final String ADD_META_DESC = "Adds a regular metadata element to items";
	
	public static final String ADD_LISTABLE_META_OPTION = "add-listable-meta";
	public static final String ADD_LISTABLE_META_DESC = "Adds a listable metadata element to items";

	/* (non-Javadoc)
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#filter(com.emc.atmos.sync.plugins.SyncObject)
	 */
	@Override
	public void filter(SyncObject obj) {
		// TODO Auto-generated method stub
		getNext().filter(obj);
	}

	/* (non-Javadoc)
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#getOptions()
	 */
	@SuppressWarnings("static-access")
	@Override
	public Options getOptions() {
		Options opts = new Options();
		
		opts.addOption(OptionBuilder.withDescription(ADD_META_DESC)
				.withLongOpt(ADD_META_OPTION).create());
		opts.addOption(OptionBuilder.withDescription(ADD_LISTABLE_META_DESC)
				.withLongOpt(ADD_LISTABLE_META_OPTION).create());
		
		return opts;
	}

	/* (non-Javadoc)
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#parseOptions(org.apache.commons.cli.CommandLine)
	 */
	@Override
	public boolean parseOptions(CommandLine line) {
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#validateChain(com.emc.atmos.sync.plugins.SyncPlugin)
	 */
	@Override
	public void validateChain(SyncPlugin first) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getName() {
		return "Metadata Plugin";
	}

	@Override
	public String getDocumentation() {
		return "The metadata plugin allows manipulation of metadata like adding and removing regular and listable metadata";
	}

}
