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
package com.emc.atmos.sync.monitor;

import java.awt.event.ActionEvent;

public class SyncEvent extends ActionEvent {
    public static final int EVENT_ID = 0;

    private Command command;
    private Exception exception;

    public SyncEvent( Object source, Command command ) {
        this( source, command, null );
    }

    public SyncEvent( Object source, Command command, Exception exception ) {
        super( source, EVENT_ID, command.toString() );
        this.command = command;
        this.exception = exception;
    }

    public Command getCommand() {
        return this.command;
    }

    public Exception getException() {
        return this.exception;
    }

    public enum Command {
        START_SYNC, SYNC_COMPLETE, ERROR
    }
}
