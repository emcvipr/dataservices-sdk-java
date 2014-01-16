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
package com.emc.atmos.api.test;

import com.emc.atmos.api.bean.adapter.Iso8601Adapter;

import org.junit.Assert;
import org.junit.Test;

import java.util.Calendar;
import java.util.TimeZone;

public class Iso8601AdapterTest {
    private static Iso8601Adapter adapter = new Iso8601Adapter();

    @Test
    public void testNegativeOffset() throws Exception {
        Calendar cal = Calendar.getInstance();
        cal.setTimeZone( TimeZone.getTimeZone( "GMT-0600" ) );
        cal.clear();

        cal.set( 2012, Calendar.DECEMBER, 1, 5, 0, 0 );
        Assert.assertEquals( "2012-12-01T11:00:00Z", adapter.marshal( cal.getTime() ) );
        Assert.assertEquals( cal.getTime(), adapter.unmarshal( "2012-12-01T05:00:00-0600" ) );
        Assert.assertEquals( cal.getTime(), adapter.unmarshal( "2012-12-01T05:00:00-06" ) );
    }

    @Test
    public void testGmt() throws Exception {
        Calendar cal = Calendar.getInstance();
        cal.setTimeZone( TimeZone.getTimeZone( "GMT" ) );
        cal.clear();

        cal.set( 2012, Calendar.DECEMBER, 1, 5, 0, 0 );
        Assert.assertEquals( "2012-12-01T05:00:00Z", adapter.marshal( cal.getTime() ) );
        Assert.assertEquals( cal.getTime(), adapter.unmarshal( "2012-12-01T05:00:00Z" ) );
        Assert.assertEquals( cal.getTime(), adapter.unmarshal( "2012-12-01T05:00:00+00" ) );
        Assert.assertEquals( cal.getTime(), adapter.unmarshal( "2012-12-01T05:00:00+0000" ) );
    }

    @Test
    public void testPositiveOffset() throws Exception {
        Calendar cal = Calendar.getInstance();
        cal.setTimeZone( TimeZone.getTimeZone( "GMT+0600" ) );
        cal.clear();

        cal.set( 2012, Calendar.DECEMBER, 1, 10, 0, 0 );
        Assert.assertEquals( "2012-12-01T04:00:00Z", adapter.marshal( cal.getTime() ) );
        Assert.assertEquals( cal.getTime(), adapter.unmarshal( "2012-12-01T10:00:00+0600" ) );
        Assert.assertEquals( cal.getTime(), adapter.unmarshal( "2012-12-01T10:00:00+06" ) );
    }
}
