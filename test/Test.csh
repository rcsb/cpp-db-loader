#!/bin/tcsh -f
#


# Use only two files, since this is various database formats test and not
# performance test


rm -rf MySqlSchema MySqlBcp MySqlSql NdbMySqlSchema NdbMySqlSql
rm -rf Db2Schema Db2Bcp Db2Sql
rm -rf SybaseSchema SybaseBcp SybaseSql
rm -rf OracleSchema OracleBcp OracleSql
rm -rf Xml

mkdir MySqlSchema MySqlBcp MySqlSql NdbMySqlSchema NdbMySqlSql
mkdir Db2Schema Db2Bcp Db2Sql
mkdir SybaseSchema SybaseBcp SybaseSql
mkdir OracleSchema OracleBcp OracleSql
mkdir Xml

echo 1jj2.cif > LIST
echo 354d.cif >> LIST


### MySql testing with BCP and SQL output ###
#
#  Produce SQL to create schema defined in mapping file schema_map_pdbx_na.cif
#
../bin/db-loader -map schema_map_pdbx_na.cif -schema \
    -server mysql -db testdb

mv DB_LOADER_SCHEMA_COMMANDS.csh MySqlSchema
mv DB_LOADER_SCHEMA_DROP.sql MySqlSchema
mv DB_LOADER_SCHEMA.sql MySqlSchema

#
../bin/db-loader -map schema_map_pdbx_na.cif -script -server mysql \
                 -db testdb -ft '&##&\t' -rt '$##$\n' 
#

mv DB_LOADER_COMMANDS.csh MySqlSchema


#
# Produce loadable files and load scripts for MySql to populate the above
# schema.
#
../bin/db-loader -map schema_map_pdbx_na.cif \
                 -revise revised_schema_map_pdbx_na.cif -list LIST -bcp \
                 -server mysql -db testdb -ft '&##&\t' -rt '$##$\n'
#

mv DB_LOADER_COMMANDS.csh MySqlBcp
mv DB_LOADER_DELETE.sql MySqlBcp
mv DB_LOADER_LOAD.sql MySqlBcp

mv *.bcp MySqlBcp
mv revised_schema_map_pdbx_na.cif MySqlBcp


#
# Produce loadable files and load scripts for MySql to populate the above
# schema.
#
../bin/db-loader -map schema_map_pdbx_na.cif \
                 -revise revised_schema_map_pdbx_na.cif -list LIST -sql \
                 -server mysql -db testdb -ft '&##&\t' -rt '$##$\n'
#


mv DB_LOADER.sql MySqlSql
mv DB_LOADER_COMMANDS.csh MySqlSql
mv revised_schema_map_pdbx_na.cif MySqlSql


### Db2 testing with BCP and SQL output ###
#
#  Produce SQL to create schema defined in mapping file schema_map_pdbx_na.cif
#
../bin/db-loader -map schema_map_pdbx_na.cif -schema \
    -server db2 -db testdb

mv DB_LOADER_SCHEMA_COMMANDS.csh Db2Schema
mv DB_LOADER_SCHEMA_DROP.sql Db2Schema
mv DB_LOADER_SCHEMA.sql Db2Schema

#
../bin/db-loader -map schema_map_pdbx_na.cif -script -server db2 \
                 -db testdb -ft '&##&\t' -rt '$##$\n' 
#

mv DB_LOADER_COMMANDS.csh Db2Schema

#
# Produce loadable files and load scripts for DB2 to populate the above schema.
#
../bin/db-loader -map schema_map_pdbx_na.cif \
                 -revise revised_schema_map_pdbx_na.cif -list LIST -bcp \
                 -server db2 -db testdb -ft '&##&\t' -rt '$##$\n'
#

mv DB_LOADER_COMMANDS.csh Db2Bcp
mv DB_LOADER_DELETE.sql Db2Bcp
mv DB_LOADER_LOAD_COMMANDS.csh Db2Bcp

mv *.bcp Db2Bcp
mv revised_schema_map_pdbx_na.cif Db2Bcp


#
# Produce loadable files and load scripts for DB2 to populate the above schema.
#
../bin/db-loader -map schema_map_pdbx_na.cif \
                 -revise revised_schema_map_pdbx_na.cif -list LIST -sql \
                 -server db2 -db testdb -ft '&##&\t' -rt '$##$\n'
#

mv DB_LOADER.sql Db2Sql
mv DB_LOADER_COMMANDS.csh Db2Sql
mv revised_schema_map_pdbx_na.cif Db2Sql


### Sybase testing with BCP and SQL output ###
#
#  Produce SQL to create schema defined in mapping file schema_map_pdbx_na.cif
#
../bin/db-loader -map schema_map_pdbx_na.cif -schema \
    -server sybase -db testdb

mv DB_LOADER_SCHEMA_COMMANDS.csh SybaseSchema
mv DB_LOADER_SCHEMA_DROP.sql SybaseSchema
mv DB_LOADER_SCHEMA.sql SybaseSchema

#
../bin/db-loader -map schema_map_pdbx_na.cif -script -server sybase \
                 -db testdb -ft '&##&\t' -rt '$##$\n' 
#

mv DB_LOADER_COMMANDS.csh SybaseSchema

#
# Produce loadable files and load scripts for Sybase to populate the above
# schema.
#
../bin/db-loader -map schema_map_pdbx_na.cif \
                 -revise revised_schema_map_pdbx_na.cif -list LIST -bcp \
                 -server sybase -db testdb -ft '&##&\t' -rt '$##$\n'
#

mv DB_LOADER_COMMANDS.csh SybaseBcp
mv DB_LOADER_DELETE.sql SybaseBcp
mv DB_LOADER_LOAD_COMMANDS.csh SybaseBcp

mv *.bcp SybaseBcp
mv revised_schema_map_pdbx_na.cif SybaseBcp

#
# Produce loadable files and load scripts for Sybase to populate the above
# schema.
#
../bin/db-loader -map schema_map_pdbx_na.cif \
                 -revise revised_schema_map_pdbx_na.cif -list LIST -sql \
                 -server sybase -db testdb -ft '&##&\t' -rt '$##$\n'
#

mv DB_LOADER.sql SybaseSql
mv DB_LOADER_COMMANDS.csh SybaseSql
mv revised_schema_map_pdbx_na.cif SybaseSql


### Oracle testing with BCP and SQL output ###
#
#  Produce SQL to create schema defined in mapping file schema_map_pdbx_na.cif
#
../bin/db-loader -map schema_map_pdbx_na.cif -schema \
    -server oracle -db testdb

mv DB_LOADER_SCHEMA_COMMANDS.csh OracleSchema
mv DB_LOADER_SCHEMA_DROP.sql OracleSchema
mv DB_LOADER_SCHEMA.sql OracleSchema

#
../bin/db-loader -map schema_map_pdbx_na.cif -script -server oracle \
                 -db testdb -ft '&##&\t' -rt '$##$\n' 
#

mv DB_LOADER_COMMANDS.csh OracleSchema

#
# Produce loadable files and load scripts for Oracle to populate the above
# schema.
#
../bin/db-loader -map schema_map_pdbx_na.cif \
                 -revise revised_schema_map_pdbx_na.cif -list LIST -bcp \
                 -server oracle -db testdb -ft '&##&\t' -rt '$##$\n'
#

mv DB_LOADER_COMMANDS.csh OracleBcp
mv DB_LOADER_DELETE.sql OracleBcp
mv DB_LOADER_LOAD_COMMANDS.csh OracleBcp

mv *.bcp OracleBcp
mv *.ctl OracleBcp
mv revised_schema_map_pdbx_na.cif OracleBcp


#
# Produce loadable files and load scripts for Oracle to populate the above
# schema.
#
../bin/db-loader -map schema_map_pdbx_na.cif \
                 -revise revised_schema_map_pdbx_na.cif -list LIST -sql \
                 -server oracle -db testdb -ft '&##&\t' -rt '$##$\n'
#


mv DB_LOADER.sql OracleSql
mv DB_LOADER_COMMANDS.csh OracleSql
mv revised_schema_map_pdbx_na.cif OracleSql


#
# Create XML schema
#

../bin/db-loader -map schema_map_pdbx_na.cif -schema -xml \
  -dictOdb mmcif_pdbx.odb -dictName mmcif_pdbx.dic -ns PDBx

#
# Produce XML output.
#
../bin/db-loader -map schema_map_pdbx_na.cif \
  -revise revised_schema_map_pdbx_na.cif -list LIST -xml \
  -dictOdb mmcif_pdbx.odb -dictName mmcif_pdbx.dic -ns PDBx

mv DB_LOADER_SCHEMA_XML.xsd Xml

#

mv *.cif.xml Xml
mv revised_schema_map_pdbx_na.cif Xml


### Old NDB schema testing: MySql testing with SQL output ###
#
#  Produce SQL to create schema defined in mapping file schema_map_pdbx_na.cif
#
../bin/db-loader -map ref_schema_map_internal-ndb.cif -schema \
    -server mysql -db testdb

mv DB_LOADER_SCHEMA_COMMANDS.csh NdbMySqlSchema
mv DB_LOADER_SCHEMA_DROP.sql NdbMySqlSchema
mv DB_LOADER_SCHEMA.sql NdbMySqlSchema

#
../bin/db-loader -map ref_schema_map_internal-ndb.cif -script -server mysql \
                 -db testdb -ft '&##&\t' -rt '$##$\n' 
#

mv DB_LOADER_COMMANDS.csh NdbMySqlSchema


#
# Produce loadable files and load scripts for MySql to populate the above
# schema.
#
../bin/db-loader -map ref_schema_map_internal-ndb.cif \
                 -revise revised_ref_schema_map_internal-ndb.cif \
                 -f 105d.cif.cif -sql \
                 -server mysql -db testdb -ft '&##&\t' -rt '$##$\n'
#


mv DB_LOADER.sql NdbMySqlSql
mv DB_LOADER_COMMANDS.csh NdbMySqlSql
mv revised_ref_schema_map_internal-ndb.cif NdbMySqlSql

