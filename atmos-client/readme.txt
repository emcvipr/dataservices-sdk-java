-----------------------------
| Java REST API for EMC Atmos |
-----------------------------

This API allows Java developers to easily connect to EMC's Atmos
Storage.  It handles all of the low-level tasks such as generating and signing 
requests, connecting to the server, and parsing server responses.

Requirements
------------
 * Java 6+
 * Log4J 1.2+ (included, Apache License)
 * JDOM 1.0+  (included, see jdom-license.txt)
 * Apache Commons Codec v1.4+ (Included, Apache License.)
 * Apache Ant 1.7+ (for running build scripts only)
 * Apache HTTP Client (included, Apache license)
 
Usage
-----
To use the API, include atmos-api-x.x.x.x.jar as well as the required above libraries to
your project's classpath.

In order to use the API, you need to construct an instance of the AtmosApiClient
class, passing in an instance of AtmosConfig.  This class contains the parameters
used to connect to the server.

AtmosApi atmos = new AtmosApiClient( new AtmosConfig( "uid",
                                                      "shared secret",
                                                      new URI( "http://host:port" ) ) );

Where host is the hostname or IP address of an Atmos node that you're authorized
to access, port is the IP port number used to connect to the server (generally
80 for HTTP), UID is the full token ID to connect as, and the shared secret is the
shared secret key assigned to the UID you're using.  The UID and shared secret
are available from your Atmos tenant administrator.  The secret key should be
a base-64 encoded string as shown in the tenant administration console, e.g
"jINDh7tV/jkry7o9D+YmauupIQk=".

After you have created your AtmosApiClient object, you can use the methods on the
object to manipulate data in the cloud.  For instance, to create a new, empty
object in the cloud, you can simply call:

ObjectId id = atmos.createObject( null, null );

The createObject method will return an ObjectId you can use in subsequent calls
to modify the object.

Source Code
-----------

To build the atmos-api-x.x.x.x.jar from source, use the 'jar' target in the supplied
build.xml using Apache Ant (http://ant.apache.org).

Logging
-------

The API uses Log4J for logging functionality.  See the file src/log4j.properties
for a sample logging configuration that appends all debug output to atmos.log.

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

New in 2.1: Anonymous access tokens, pre-signed requests, key-pools (requires
Atmos 2.1+)
----------------------------
There is a completely new interface in this release (AtmosApi).  Follow the javadoc
for more information.  There is also an EsuApiJerseyAdapter that implements the old
EsuApi for backward compatibility.  In this release, EsuApi is deprecated.  It may
be removed in the future.

New in 2.0.3: Hardlinks and content-disposition for shareable URLs (requires
Atmos 2.0.3+)
------------------------------------------------------------
Using EsuRestApi20 (and EsuApi20), you have access to additional features such as
hardlinks and providing a content-disposition value for shareable URLs (i.e.
"attachment; filename=\"filename.txt\"")

New in 1.4: EsuRestApiApache
----------------------------
In version 1.4 a new implementation is available: EsuRestApiApache.  This version
implements the same EsuApi interface, but uses the Apache Commons HTTP client 
instead of the built-in Java HTTP client.  This version should offer more options
with connection pooling and HTTP keep-alive requests for higher throughput than
the standard client.  It also does a better job of handling URLs with unicode
characters than the default Java client does.  The only downside is that you'll 
need to include the JAR files in the commons-httpclient folder with your 
application, adding about 1MB to your disk footprint.

"Load Balanced" implementations
--------------------------------
There is also now a couple "Load Balanced" implementations available for 
testing:  LBEsuRestApi and LBEsuRestApiApache.  These implementations accept a 
List of hostnames in your Atmos cluster and will round-robin the requests 
across those nodes.  When issuing many small requests in a highly threaded 
environment, this can improve your application's performance dramatically.  Do 
not use this code though when your application creates an object and then 
updates it immediately (e.g. create/append like UploadHelper) since there is a 
very small window after a create where other cluster nodes may not find the 
newly created object.
