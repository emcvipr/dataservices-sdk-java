-----------------------------
| Java REST API for EMC ESU |
-----------------------------

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
 * Apache Commons Codec v1.3+ (Included, Apache License.)
 * Apache Ant 1.7+ (for running build scripts only)
 * Apache HTTP Client (optional, required to use EsuRestApiApache)
 
Usage
-----
To use the API, include emcesu.jar as well as the required above libraries to
your project's classpath.

In order to use the API, you need to construct an instance of the EcuRestApi
class.  This class contains the parameters used to connect to the server.

EsuApi esu= new EsuRestApiApache( "host", port, "uid", "shared secret" );

Where host is the hostname or IP address of an ESU node that you're authorized
to access, port is the IP port number used to connect to the server (generally
80 for HTTP), UID is the username to connect as, and the shared secret is the
shared secret key assigned to the UID you're using.  The UID and shared secret
are available from your ESU tennant administrator.  The secret key should be
a base-64 encoded string as shown in the tennant administration console, e.g
"jINDh7tV/jkry7o9D+YmauupIQk=".

After you have created your EsuRestApi object, you can use the methods on the
object to manipulate data in the cloud.  For instance, to create a new, empty
object in the cloud, you can simply call:

ObjectId id = esu.createObject( null, null, null, null );

The createObject method will return an ObjectId you can use in subsequent calls
to modify the object.

The helper classes provide some basic functionality when working with ESU like
uploading a file to the cloud.  To create a helper, simply construct the
appropriate class (UploadHelper or DownloadHelper).  The first, required 
argument is your EsuResApi object.  The second argument is optional and defines
the transfer size used for requests.  By default, your file will be uploaded
to the server in 4MB chunks.  After constructing the helper object, there are
a couple ways to upload and download objects.  You can either give the helper
a file to transfer or a stream.  When passing a stream, you can optionally pass
an extra argument telling the helper whether you want the descriptor closed 
after the transfer has completed.

UploadHelper helper = new UploadHelper( esu, null );
ObjectId id = helper.createObjectFromFile( new File( "readme.txt" ) );

The helper classes also allow you to register listener classes that implement
the ProgressListener interface.  If you register listeners, they will be 
notified of transfer progress, when the transfer completes, or when an error 
occurs.  You can also access the same status information through the helper 
object's methods.

Note that since a transfer's status is directly connected to the helper class,
the helper class should not be used for more than one transfer.  Doing so can
produce undesired results.

Source Code
-----------

The source code is broken into four packages.

 * com.emc.esu.api - This package contains the core java objects used in the
   API (Metadata, Acl, Extent, etc) as well as the generic CosApi interface.
 * com.emc.esu.api.rest - This package contains the REST implementation of
   the CosApi interface.

To build the emcesu.jar from source, use the 'jar' target in the supplied
build.xml using Apache Ant (http://ant.apache.org).

Logging
-------

The API uses Log4J for logging functionality.  See the file src/log4j.properties
for a sample logging configuration that appends all debug output to esu.log.

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
