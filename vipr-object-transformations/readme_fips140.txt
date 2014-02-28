Most Java implementations do *not* ship FIPS-140 compliant encryption modules.  Refer to
your implementation's documentation.  To ensure FIPS-140 compliant encryption, it is 
recommended to use a 3rd party provider like RSA Crypto-J.

1) You can obtain Crypto-J by visiting the following website.  An evaluation verison is
available free of charge.

http://www.emc.com/security/rsa-bsafe.htm

2) Follow the instructions provided to install the Crypto-J provider.  This will involve
installing some JAR files into jre/lib/ext and editing jre/lib/security/java.security
to register the provider.

3) (optional) If you are using a JVM provided by Oracle, download and install the 
"Java Cryptography Extension (JCE) Unlimited Strength Jurisdiction Policy Files".  This 
will allow you to configure 256-bit AES encryption.

http://www.oracle.com/technetwork/java/javase/downloads/index.html
(at the bottom of the page)

4) After your application starts, you should verify that Crypto-J is in FIPS-140 mode:

import com.rsa.jsafe.crypto.CryptoJ;

assert CryptoJ.isFIPS140Compliant();
assert CryptoJ.isInFIPS140Mode();

-- Note that if you use Crypto-J's level 2 authentication mode, the above is not needed.

5) If you registered the Crypto-J JCE provider as the first provider, it should be used
by all operations without furhter configuration.  You can also explicitly construct the
com.rsa.jsafe.provider.JsafeJCE provider class and pass it into the encryption factories
where it will be used for all operations to obtain Cipher, Signature, and MessageDigest
instances.
  

