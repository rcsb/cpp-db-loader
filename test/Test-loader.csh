#!/bin/tcsh -f
#
echo 1jj2.cif > LIST
echo 1ffk.cif >> LIST
echo 354d.cif >> LIST
#
#  Produce SQL to create schema defined in mapping file schema_map_pdbx_na.cif
#
../bin/db-loader -map schema_map_pdbx_na.cif -schema \
    -server mysql -db testdb
#
# Produce loadable files and load scripts for DB2 to populate the above schema.
#
../bin/db-loader -map  schema_map_pdbx_na.cif \
                 -revise revised_schema_map_pdbx_na.cif -list LIST -bcp \
                 -server mysql -db testdb -ft '&##&\t' -rt '$##$\n'
#
rm -f DB_LOADER_DELETE.sql
rm -f DB_LOADER_LOAD.sql
#
../bin/db-loader -map schema_map_pdbx_na.cif -script -server mysql \
                 -db testdb -ft '&##&\t' -rt '$##$\n' 
#
