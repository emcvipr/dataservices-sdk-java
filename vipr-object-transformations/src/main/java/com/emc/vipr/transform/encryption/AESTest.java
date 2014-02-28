package com.emc.vipr.transform.encryption;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;

import com.emc.vipr.transform.TransformConstants;

public class AESTest {
    private static enum Mode { ENCRYPT, DECRYPT };
    
    public static void main(String[] args) {
        try {
            Mode m = Mode.valueOf(args[0]);
            
            if(m == Mode.ENCRYPT) {
                int bits = Integer.valueOf(args[1]);
                String infile = args[2];
                String outfile = args[3];
                
                // Generate key
                KeyGenerator kg = KeyGenerator.getInstance("AES");
                kg.init(bits);
                SecretKey sk = kg.generateKey();
                
                Cipher cipher = Cipher.getInstance(TransformConstants.DEFAULT_ENCRYPTION_TRANSFORM);
                cipher.init(Cipher.ENCRYPT_MODE, sk);
                
                System.out.println("Key: " + new String(Base64.encodeBase64(sk.getEncoded()), "US-ASCII"));
                System.out.println("IV: " + new String(Base64.encodeBase64(cipher.getIV()), "US-ASCII"));
                
                CipherOutputStream out = new CipherOutputStream(new FileOutputStream(new File(outfile)), cipher);
                InputStream in = new FileInputStream(new File(infile));
                
                doStream(in, out);
                
                out.close();
                in.close();
            } else if(m == Mode.DECRYPT) {
                String key = args[1];
                String iv = args[2];
                String infile = args[3];
                String outfile = args[4];
                
                SecretKeySpec sk = new SecretKeySpec(Base64.decodeBase64(key.getBytes("US-ASCII")), "AES");
                IvParameterSpec ivspec = new IvParameterSpec(Base64.decodeBase64(iv.getBytes("US-ASCII")));
                Cipher cipher = Cipher.getInstance(TransformConstants.DEFAULT_ENCRYPTION_TRANSFORM);
                cipher.init(Cipher.DECRYPT_MODE, sk, ivspec);

                CipherInputStream in = new CipherInputStream(new FileInputStream(new File(infile)), cipher);
                OutputStream out = new FileOutputStream(new File(outfile));
                
                doStream(in, out);
                
                out.close();
                in.close();
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    private static void doStream(InputStream in, OutputStream out) throws IOException {
        byte[] buffer = new byte[128*1024];
        int c = 0;
        while((c = in.read(buffer)) != -1) {
            out.write(buffer, 0, c);
        }
    }

}
