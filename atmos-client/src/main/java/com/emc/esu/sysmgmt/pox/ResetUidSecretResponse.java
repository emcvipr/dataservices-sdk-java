package com.emc.esu.sysmgmt.pox;

import java.io.IOException;
import java.net.HttpURLConnection;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;

import com.emc.esu.sysmgmt.SysMgmtUtils;

public class ResetUidSecretResponse extends PoxResponse {
    private String newSecret;

    public ResetUidSecretResponse(HttpURLConnection con) throws IOException, JDOMException {
        // Parse response
        Document doc = SysMgmtUtils.parseResponseXml(con);
        
        Element root = doc.getRootElement(); //shared_secret
        newSecret = root.getText();
    }

    public String getNewSecret() {
        return newSecret;
    }

}
