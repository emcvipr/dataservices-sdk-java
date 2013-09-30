#!/bin/sh
################
# This is a sample shell script to run the AtmosSync tool
################

CLASSPATH=.:mysql-jdbc.jar:Projects/atmos-java/samples/AtmosSync/dist/AtmosSync.jar
ATMOS_SYNC_JAR=~/Projects/atmos-java/samples/AtmosSync/dist/AtmosSync.jar

SOURCE=http://15f02fc57acd4e17bb79c3e83822e2c9/A50231654454554f87ba:uYcwlJOhJ+Ayw0X6jXWM4CBPTtY=@api.atmosonline.com:80

SRC_QUERY="select oid from oid_list"

#DEST=file:///Users/arnetc/tmp
DEST=http://9b27373a6613448da56bed9d2ce3a3e4/as:yoXN/Y+y+y9cHU/Chl0kNymlfXY=@10.242.25.63:80

THREADS=10

DB_URL=jdbc:mysql://10.242.25.160/atmossync
DB_CLASS=com.mysql.jdbc.Driver
DB_USER=atmossync
DB_PASSWORD=password

ID_MAP_SELECT="select new_oid from oid_map where old_oid=:source_id"
ID_MAP_INSERT='insert into oid_map (old_oid, new_oid, size) values (:source_id, :dest_id, :size)'
ID_MAP_ADD_META=size

java -classpath $CLASSPATH com.emc.atmos.sync.AtmosSync2 --source $SOURCE --source-sql-query "$SRC_QUERY" --query-jdbc-url $DB_URL --query-jdbc-driver-class $DB_CLASS --query-user $DB_USER --query-password $DB_PASSWORD --destination $DEST --include-retention-expiration --source-threads 10 --id-map-jdbc-url $DB_URL --id-map-jdbc-driver-class $DB_CLASS --id-map-user $DB_USER --id-map-password $DB_PASSWORD --id-map-select-sql "$ID_MAP_SELECT" --id-map-insert-sql "$ID_MAP_INSERT" --id-map-add-metadata $ID_MAP_ADD_META --id-map-raw-ids

