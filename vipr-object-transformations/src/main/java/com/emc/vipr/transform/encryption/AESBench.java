package com.emc.vipr.transform.encryption;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

public class AESBench {

    public static void main(String[] args) {
        if(args.length != 1) {
            System.out.println("Usage: java -Xmx<heap>M -jar AESBench.jar <test_size_in_mb>");
            System.out.println("Where <test_size_in_mb> is the number of megabytes to test (recommended 1024)");
            System.out.println("and <heap> is the java heap size.  You will need up to 1600M of heap for 1024M test depending on JVM.");
        }
        int mb = Integer.parseInt(args[0]);
        
        Properties props = System.getProperties();
        
//        for(Object key : props.keySet()) {
//            printProp(props, ""+key);
//        }
        
        System.out.println("AES Java Benchmark");
        System.out.println("==================");
        
        printProp(props, "java.vm.vendor");
        printProp(props, "java.vm.name");
        printProp(props, "java.version");
        printProp(props, "os.name");
        printProp(props, "os.arch");
        printProp(props, "os.version");
        
        System.out.printf("cpu.count: %d\n", Runtime.getRuntime().availableProcessors());
        
        Map<String, String> cpuinfo = getCpuinfo(props.getProperty("os.name"));
        System.out.printf("cpu.type: %s\n", cpuinfo.get("cpu"));
        System.out.printf("cpu.speed: %s MHz\n", cpuinfo.get("mhz"));
        
        System.out.println();
        System.out.println("==================");
        System.out.printf("Generating %dMB of test data...", mb);
        byte[] data = new byte[mb*1024*1024];
        try {
            Random r = new Random();
            r.nextBytes(data);
            System.out.println("Done");
            
            cipherBench(new ByteArrayInputStream(data), "AES", 128, "CBC", "PKCS5Padding");
            cipherBench(new ByteArrayInputStream(data), "AES", 256, "CBC", "PKCS5Padding");
            
            digestBench(new ByteArrayInputStream(data), "SHA-1");
            digestBench(new ByteArrayInputStream(data), "SHA-256");
            digestBench(new ByteArrayInputStream(data), "SHA-384");
            digestBench(new ByteArrayInputStream(data), "SHA-512");
            
        } catch(Exception e) {
            e.printStackTrace();
        } 

    }
    
    private static void digestBench(InputStream in, String digestMode) throws IOException, GeneralSecurityException {
        MessageDigest md = MessageDigest.getInstance(digestMode);
        System.out.println("==================");
        System.out.println("Digest Benchmark");
        System.out.println("Mode: " + digestMode);
        System.out.println("Provider: " + md.getProvider().getName() + ": " + md.getProvider().getClass());
        byte[] buffer = new byte[128*1024];
        int c;
        long bytes=0;
        long start = System.currentTimeMillis();
        while((c = in.read(buffer)) != -1) {
            md.update(buffer, 0, c);
            bytes+=c;
        }
        byte[] digest = md.digest();
        long end = System.currentTimeMillis();
        System.out.println("Digest=" + KeyUtils.toHexPadded(digest));
        printPerf(bytes, end-start);
        in.close();
    }

    private static void printPerf(long bytes, long ms) {
        System.out.printf("%d bytes in %d ms %.2f MB/s\n", bytes, ms, ((double)bytes)/(ms/1000.0)/(1024*1024));
    }

    private static void cipherBench(InputStream in, String alg, int sz, String mode, String padding) throws GeneralSecurityException, IOException {
        KeyGenerator kg = KeyGenerator.getInstance(alg);
        String cipherMode = alg + "/" + mode + "/" + padding;
        Cipher cipher = Cipher.getInstance(cipherMode);
        kg.init(sz);
        SecretKey sk = kg.generateKey();
        System.out.println("==================");
        System.out.println("Cipher Benchmark");
        System.out.println("Mode: " + cipherMode);
        System.out.println("Key size: " + sz);
        System.out.println("Max key size: " + Cipher.getMaxAllowedKeyLength(cipherMode));
        System.out.println("Provider: " + cipher.getProvider().getName() + ": " + cipher.getProvider().getClass());
        
        if(sz > Cipher.getMaxAllowedKeyLength(cipherMode)) {
            System.out.printf("***\n*** Your JVM does not support %d-bit %s encryption.  Please download and install the 'Java Cryptography Extension (JCE) Unlimited Strength Jurisdiction Policy Files' from http://www.oracle.com/technetwork/java/javase/downloads/index.html\n***\n", sz, alg);
            return;
        }
        cipher.init(Cipher.ENCRYPT_MODE, sk, new SecureRandom());
        
        byte[] buffer = new byte[128*1024];
        int c;
        long bytes=0;
        long start = System.currentTimeMillis();
        while((c = in.read(buffer)) != -1) {
            cipher.update(buffer, 0, c);
            bytes+=c;
        }
        byte[] finalBlock = cipher.doFinal();
        long end = System.currentTimeMillis();
        System.out.println("End Block=" + KeyUtils.toHexPadded(finalBlock));
        printPerf(bytes, end-start);
        in.close();
    }

    private static void printProp(Properties props, String key) {
        System.out.printf("%s: %s\n", key, props.getProperty(key));
    }

    public static Map<String,String> getCpuinfo(String osName) {
        Map<String,String> cpuinfo = new HashMap<String, String>();
        cpuinfo.put("cpu", "unknown");
        cpuinfo.put("mhz", "unknown");
        
        try {
            if("Mac OS X".equals(osName)) {
                cpuinfo.put("cpu", exec("sysctl -n machdep.cpu.brand_string").trim());
                long hz = Long.parseLong(exec("sysctl -n machdep.tsc.frequency").trim());
                cpuinfo.put("mhz", ""+(hz/1000000));
            } else if("Linux".equals(osName)) {
                String data = readfile(new File("/proc/cpuinfo"));
                String cpu = extractString(data, "^model name\\s*:\\s*(.*)$");
                if(cpu != null) {
                    cpuinfo.put("cpu", cpu);
                }
                
                String mhz = extractString(data, "cpu MHz\\s*:\\s*([0-9]+).*$");
                if(mhz != null) {
                    cpuinfo.put("mhz", mhz);                  
                }
            } else if(osName.startsWith("Windows")) {
                String data = exec("reg query HKLM\\HARDWARE\\DESCRIPTION\\System\\CentralProcessor\\0 /v ProcessorNameString");
                String cpu = extractString(data, "^.*REG_SZ[ ]+(.*)$");
                if(cpu != null) {
                    cpuinfo.put("cpu", cpu);
                }
                data = exec("reg query HKLM\\HARDWARE\\DESCRIPTION\\System\\CentralProcessor\\0 /v ~MHZ");
                String mhz = extractString(data, "^.*0x([0-9a-f]+).*$");
                if(mhz != null) {
                    long mval = Long.parseLong(mhz, 16);
                    cpuinfo.put("mhz", ""+mval);                  
                }
            }
        } catch(Exception e) {
            System.err.println("Error getting cpu info: " + e);
        }
        
        return cpuinfo;
    }
    
    private static String extractString(String data, String pattern) {
        Pattern p = Pattern.compile(pattern, Pattern.MULTILINE);
        Matcher m = p.matcher(data);
        if(m.find()) {
            return m.group(1).trim();
        } else {
            System.err.println("Could not extract data with pattern " + pattern);
            return null;
        }
    }
    
    private static String readfile(File f) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(f));
        String s;
        StringBuffer sb = new StringBuffer();
        while((s = br.readLine()) != null) {
            sb.append(s);
            sb.append("\n");
        }
        br.close();
        return sb.toString();
    }
    
    private static String exec(String cmdline) throws IOException {
        Process p = Runtime.getRuntime().exec(cmdline);
        BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
        StringBuffer sb = new StringBuffer();
        String s;
        while((s = br.readLine()) != null) {
            sb.append(s);
            sb.append("\n");
        }
            
        return sb.toString();
    }

}
