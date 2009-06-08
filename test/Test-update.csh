#!/bin/tcsh -f
#
#  Update schema map - 
#
#
../bin/db-loader -map  schema_map_pdbx_na.cif  \
		 -revise revised_schema_map_pdbx_na.cif \
		 -update update_schema_map_pdbx_na.cif \
#
#
