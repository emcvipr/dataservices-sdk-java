package com.emc.atmos.api.test;

import com.emc.util.HttpUtil;
import org.junit.Assert;
import org.junit.Test;

public class HttpUtilTest {
    @Test
    public void testUtf8Encoding() {
        String value = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789`-=[]\\;',./~!@#$%^&*()_+{}|:\"<>?";
        Assert.assertEquals(value, HttpUtil.decodeUtf8(HttpUtil.encodeUtf8(value)));
    }
}
