---------------------------------
| Java REST Testcases for Atmos |
---------------------------------

Configuration
-------------
Before running the testcases, a file named vipr.properties must be either put on the
classpath (e.g. src/test/resources) or present in your home directory.  The file should
contain the following configuration keys:

Atmos:
vipr.atmos.uid - Atmos full token uid (e.g. 123daba33425413251/user1)
vipr.atmos.secret_key - Shared secret key for the uid
vipr.atmos.endpoints - Comma separated list of endpoint URIs (more than one is optional)

ACDP:
acdp.admin.endpoint - ACDP admin endpoint, usually http://admin_node:8080/
acdp.admin.username - ACDP administrator username
acdp.admin.password - ACDP administrator password
acdp.mgmt.endpoint - ACDP management endpoint, usually http://portal_node:8080/
acdp.mgmt.username - ACDP management user (account user)
acdp.mgmt.password - ACDP management password

Atmos System Management:
atmos.sysmgmt.proto - System management protocol (usually https)
atmos.sysmgmt.host - System management host (primary or secondary node in RMG)
atmos.sysmgmt.port - System management port (usually 443)
atmos.sysmgmt.username - System management user
atmos.sysmgmt.password - System management password
    
If a particular configuration key is missing, that test group will be skipped.

 
Running the Testcases
---------------------
The tests are run through Maven.  You can run them with:

mvn test

