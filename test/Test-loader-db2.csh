#!/bin/tcsh -f
#
echo 1jj2.cif > LIST
echo 1ffk.cif >> LIST
echo 354d.cif >> LIST
#
#  Produce SQL to create schema defined in mapping file schema_map_pdbx_na.cif
#
../bin/db-loader -map schema_map_pdbx_na.cif -schema \
    -db testdb -server db2
#
# Produce loadable files and load scripts for DB2 to populate the above schema.
#
../bin/db-loader -list LIST -map  schema_map_pdbx_na.cif -bcp -db testdb \
                 -server db2 -revise revised_schema_map_pdbx_na.cif
#
echo "DONE"
ls -la *.sql *.csh
#
rm -f DB_LOADER_LOAD.sql
rm -f DB_LOADER_LOAD_COMMANDS.csh 
rm -f DB_LOADER_COMMANDS.csh 
#
../bin/db-loader -script  -map  schema_map_pdbx_na.cif -db testdb -bcp \
                 -server db2 
ls -la *.sql *.csh
###
