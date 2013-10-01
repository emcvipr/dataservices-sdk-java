-----------------------------------
| Java REST Testcases for EMC ESU |
-----------------------------------

This API allows Java developers to easily connect to EMC's ESU 
Storage.  It handles all of the low-level tasks such as generating and signing 
requests, connecting to the server, and parsing server responses.  It also 
includes helper classes to help automate basic tasks such as creating, updating,
and downloading object content from the cloud.

Requirements
------------
 * Java 1.5+
 * Log4J 1.2+ (included, Apache License)
 * JDOM 1.0+  (included, see jdom-license.txt)
 * JUnit 4.4+ (included, CPL 1.0 license) 
 * Apache Commons Codec v1.3+ (Included, Apache License.)
 * Apache Ant 1.7+ (for running build scripts only)
 
Running the Testcases
---------------------
Before running the testcases, please compile the REST API or include the
emcesu.jar in your classpath.

To run the testcases, first edit the testcase class and set the IP address of
the ESU node you want to connect to, your UID, and shared secret.  If you are
developing under Eclipse, simply right-click on the test class (EsuRestApiTest)
and select Run As->JUnit Test.  Otherwise, you may execute the testcases under
Apache Ant using the 'test' target.


Socket errors on older versions of Windows
------------------------------------------
On Windows XP and older versions of windows, a client making many requests
may run out of client sockets.  The error you see will look like:

java.net.BindException: Address already in use: connect

This is because Windows only allows client sockets up to port 5000 by default
and when a socket is closed it remains in TIME_WAIT for 180 seconds.  Thus, if
you make more than 4000 requests in 3 minutes you will likely see these errors.
You can fix this problem by editing the registry and changing MaxUserPort and
TCPTimedWaitDelay settings.  This is discussed in the article:
http://support.microsoft.com/kb/196271/en-us

