package com.emc.esu.sysmgmt.pox;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.text.MessageFormat;

import com.emc.esu.sysmgmt.SysMgmtApi;

public class ResetUidSecretRequest extends PoxRequest<ResetUidSecretResponse> {
    private String uid;
    private String subTenantName;

    
    public ResetUidSecretRequest(SysMgmtApi api, String uid, String subTenantName) {
        super(api);
        this.uid = uid;
        this.subTenantName = subTenantName;
    }

    public ResetUidSecretResponse call() throws Exception {
        HttpURLConnection con = getConnection("/sub_tenant_admin/add_application",
                MessageFormat.format("app_name={0}&sub_tenant_name={1}&regenerate=yes", 
                        uid, subTenantName));

        con.setDoOutput(true);
        con.connect();
        OutputStream out = con.getOutputStream();
        out.close();

        return new ResetUidSecretResponse(con);
    }
}
