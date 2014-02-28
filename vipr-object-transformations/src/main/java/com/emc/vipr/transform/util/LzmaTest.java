package com.emc.vipr.transform.util;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;

import com.emc.vipr.transform.compression.LZMAOutputStream;

import SevenZip.Compression.LZMA.Decoder;

public class LzmaTest {

    public static void main(String[] args) {
        File fin = new File(args[0]);
        File fout = new File(args[1]);
        
        try {
            FileInputStream fis = new FileInputStream(fin);
            LZMAOutputStream compOut = new LZMAOutputStream(new BufferedOutputStream(new FileOutputStream(fout)), 4);
            
            byte[] buffer = new byte[4096];
            int c = 0;
            while((c = fis.read(buffer)) != -1) {
                compOut.write(buffer, 0, c);
            }
            
            fis.close();
            compOut.close();
            compOut = null;
            Runtime.getRuntime().gc();
            
            System.out.printf("Done.  Input size %d compressed size %d\n", fin.length(), fout.length());
            
            System.out.printf("Free: %d max: %d total: %d\n", Runtime.getRuntime().freeMemory(), Runtime.getRuntime().maxMemory(), Runtime.getRuntime().totalMemory());
            
            // Decompress
            InputStream compIn = new FileInputStream(fout);
            ByteArrayOutputStream decompOut = new ByteArrayOutputStream((int) fin.length());
            Decoder d = new Decoder();
            // Read props
            byte[] props = new byte[5];
            compIn.read(props);
            d.SetDecoderProperties(props);
            d.Code(compIn, decompOut, -1);
            
            System.out.printf("Done. Input size %d uncompressed size %d\n", fout.length(), decompOut.size());
            
            
        } catch(Exception e) {
            e.printStackTrace();
        }

    }

}
