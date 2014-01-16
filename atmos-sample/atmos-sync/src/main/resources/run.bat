@echo off
REM ################
REM # This is a sample batch file to run the AtmosSync tool
REM ################

REM set CLASSPATH=.;sqljdbc4.jar;AtmosSync.jar
set CLASSPATH=.;mysql-jdbc.jar;AtmosSync.jar

set SOURCE=http://15f02fc57acd4e17bb79c3e83822e2c9/A50231654454554f87ba:uYcwlJOhJ+Ayw0X6jXWM4CBPTtY=@api.atmosonline.com:80

set SRC_QUERY=select oid from oid_list

set DEST=http://9b27373a6613448da56bed9d2ce3a3e4/as:yoXN/Y+y+y9cHU/Chl0kNymlfXY=@10.242.25.63:80

set THREADS=10

REM set DB_URL=jdbc:sqlserver://ccnidcucsitws1;databaseName=Atmos
REM set DB_CLASS=com.microsoft.sqlserver.jdbc.SQLServerDriver
REM set DB_USER=atmossynch
REM set DB_PASSWORD=synch
set DB_URL=jdbc:mysql://10.242.25.160/atmossync
set DB_CLASS=com.mysql.jdbc.Driver
set DB_USER=atmossync
set DB_PASSWORD=password

set ID_MAP_SELECT=select new_oid from oid_map where old_oid=:source_id
set ID_MAP_INSERT=insert into oid_map (old_oid, new_oid, size) values (:source_id, :dest_id, :size)
set ID_MAP_ADD_META=size

REM java -classpath %CLASSPATH% com.emc.atmos.sync.AtmosSync2 --source %SOURCE% --source-sql-query "%SRC_QUERY%" --query-jdbc-url %DB_URL% --query-jdbc-driver-class %DB_CLASS% --query-user %DB_USER% --query-password %DB_PASSWORD% --destination %DEST% --include-retention-expiration --source-threads 10 --id-map-jdbc-url %DB_URL% --id-map-jdbc-driver-class %DB_CLASS% --id-map-user %DB_USER% --id-map-password %DB_PASSWORD% --id-map-select-sql "%ID_MAP_SELECT%" --id-map-insert-sql "%ID_MAP_INSERT%" --id-map-add-metadata %ID_MAP_ADD_META% --id-map-raw-ids

java -classpath %CLASSPATH% com.emc.atmos.sync.AtmosSync2 --source %SOURCE% --source-sql-query "%SRC_QUERY%" --query-jdbc-url %DB_URL% --query-jdbc-driver-class %DB_CLASS% --query-user %DB_USER% --query-password %DB_PASSWORD% --include-retention-expiration --source-threads 10 -destination dummy --sink-data

