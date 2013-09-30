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
package com.emc.acdp.report;

import java.util.EventObject;

public class StatusEvent extends EventObject {
    private String currentTask;
    private Float percentComplete;

    public StatusEvent( Object source ) {
        this( source, null, null );
    }

    public StatusEvent( Object source, String currentTask, Float percentComplete ) {
        super( source );
        this.currentTask = currentTask;
        this.percentComplete = percentComplete;
    }

    public String getCurrentTask() {
        return currentTask;
    }

    public void setCurrentTask( String currentTask ) {
        this.currentTask = currentTask;
    }

    public Float getPercentComplete() {
        return percentComplete;
    }

    public void setPercentComplete( Float percentComplete ) {
        this.percentComplete = percentComplete;
    }
}
