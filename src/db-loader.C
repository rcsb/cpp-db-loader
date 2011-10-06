/*$$FILE$$*/
/*$$VERSION$$*/
/*$$DATE$$*/
/*$$LICENSE$$*/


#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <exception>
#include <cstring>
#include <string>
#include <iostream>
#include <fstream>

#include "Db.h"
#include "DbOutput.h"
#include "XmlOutput.h"
#include "CifSchemaMap.h"

using std::exception;
using std::string;
using std::cout;
using std::cerr;
using std::endl;
using std::ifstream;


#ifdef VLAD_DOCUMENTATION

Source of schema mapping file:
  -map <schema_mapping_file.cif>
  -mapodb <output_mapping_file.odb> (if specified with -map, then it will be
    created. If -map not specified, then schema map is read from this file)

Main operations (mutually exclusive):
  L1:
  Create schema only
  -schema
    -populated (this is to write the schema for only those items in the
     dictionary for which data in cif files exist. Other items are not
     processed and hence the database is made smaller.
       This is what John says. Does not seem so from the code, since data
       in CIF files is never accessed during -populated processing.

  L1:
  For initial database creation. Takes the schema from the serialized file
  and generates only data loading scripts.
  -script

  L1:
  For data conversion
  -f
  -list

    Auxiliary operations for -list data conversion main operation to BCP only
      Write revised map file
      -revise <revised file>
        The schema mapping is written out under revised map file name with
          updated attribute definitions (UpdateAttributeDef())
          but only if BCP output is specified. Q: Why not in case of other
          output formats?
        Note: -revise has impact on written data (-f or -list main options),
          in the sense that when it is selected, the values will be written
          with maximum calculated field size. If not selected, they will be
          written with current field size.

  L1:
  For updating (i.e. creating) a new schema map file based on the current
  schema map file and revised schema map file
  -update (The in memory schema mapping file is updated with possibly longer
    widths, population flags from the revised file and then written out as an
    updated file, which in later iterations becomes schema mapping file)
    Requires -revise flag that identifies the name of the revised map file
      that is to be read

This is always needed regardless of the main operation: 
  -map (optional)
  -mapodb (optional)

  Output mode (optional, default is sql)
  -sql
  -bcp
  -xml

Optional:
  Server type and related details (default is Sybase)
  -server sybase|mysql|oracle|db2
  -db (database name), default: msd1
  -ft, (default \t)
  -rt, (default \n)

  -v (verbose)

#endif


const unsigned int MODE_SQL = 1;
const unsigned int MODE_BCP = 2;
const unsigned int MODE_XML = 3;


struct Args
{
    string mFileODB;
    string lFile;
    string iFile;
    string dbName;
    string mFile;
    string rTerm;
    string fTerm;
    string updateMapFile;
    string reviseMapFile;
    string serverType;
    string stFile;
    string dictOdbFileName;
    string dictName;
    string ns;

    int mode;
    int iHash;

    bool iSchema;
    bool iScript;
    bool iOnlyPopulated;
    bool verbose;
};


static void usage(const string& progName)
{
    cerr << progName << " usage:" << endl
      << "  [-map <schema mapping ASCII CIF file>]" << endl
      << "  [-mapodb <schema mapping serialized (binary) CIF file>]" << endl
      << "  [-server sybase | mysql | oracle | db2] (default is \"sybase\")" <<
      endl
      << "  [-db <database name>] (default is \"msd1\")" << endl
      << "  [-ft <field terminator>] (default is \"\\t\", used only for bcp)" << endl
      << "  [-rt <row terminator>] (default is \"\\n\", used only for bcp)" << endl
      << "  (-sql | -bcp | -xml ) (output format)" << endl
      << "  -schema [-populated] |" << endl
      << "  -script |" << endl
      << "  -f <ASCII CIF file> [-revise <revised schema file>] |" << endl
      << "  -list <file list> [-revise <revised schema file>] [-stop <stop "\
      "file>] |" << endl
      << "  -update <update schema file> -revise <revised schema file>" <<
      endl 
      << "  [-dictOdb <dict odb> -dict <dict name> [-ns namespace]] (only with -xml flag)"
      << endl
      << "  [-v] (default verbose mode is off)" << endl << endl
      << "  Notes:" << endl
      << "    1. Either -map or -mapodb or both of them must be specified." <<
      endl
      << "       If both are specified schema mapping will be serialized." <<
      endl
      << "    2. -schema generates only the schema (and schema scripts)." <<
      endl
      << "    3. -script generates only data loading scripts." << endl
      << "    4. -f converts one CIF file, generates loading scripts and" << 
      endl
      << "       optionally writes revised schema." << endl
      << "    5. -list takes a file name which contains a list of CIF" <<
      endl
      << "       files (separated by newlines) that are to be converted," <<
      endl
      << "       generates loading scripts and optionally writes revised" <<
      endl
      << "       schema. -stop option indicates the name of the file" <<
      endl
      << "       (in the list) at which conversion is to stop." << endl
      << "    6. -update uses the schema and the revised schema to generate" <<
      endl
      << "       an updated schema." << endl;
}


static void GetArgs(Args& args, unsigned int argc, char* argv[])
{

    string progName = argv[0];

    if (argc < 3)
    {
        usage(progName);
        throw InvalidOptionsException();
    }

    args.mode = MODE_SQL;
    args.iHash = 0;
    args.iSchema = false;
    args.iScript = false;
    args.iOnlyPopulated = false;
    args.verbose = false;

    for (unsigned int i = 1; i < argc; ++i)
    {
        if (argv[i][0] == '-')
        {
            if (strcmp(argv[i], "-f") == 0)
            {
                i++;
                args.iFile = argv[i];
            }
            else if (strcmp(argv[i], "-map") == 0)
            {
                i++;
                args.mFile = argv[i];
            }
            else if (strcmp(argv[i], "-mapodb") == 0)
            {
                i++;
                args.mFileODB = argv[i];
            }
            else if (strcmp(argv[i], "-db") == 0)
            {
                i++;
                args.dbName = argv[i];
            }
            else if (strcmp(argv[i], "-list") == 0)
            {
                i++;
                args.lFile = argv[i];
            }
            else if (strcmp(argv[i], "-bcp") == 0)
            {
                args.mode = MODE_BCP;
            }
            else if (strcmp(argv[i], "-sql") == 0)
            {
                args.mode = MODE_SQL;
            }
            else if (strcmp(argv[i], "-xml") == 0)
            {
                args.mode = MODE_XML;
            }
            else if (strcmp(argv[i], "-schema") == 0)
            {
                args.iSchema = true;
            }
            else if (strcmp(argv[i], "-script") == 0)
            {
                args.iScript = true;
            }
            else if (strcmp(argv[i], "-server") == 0)
            {
                i++;
                args.serverType = argv[i];
            }
            else if (strcmp(argv[i], "-rt") == 0)
            {
                i++;
                String::UnEscape(args.rTerm, argv[i]);
            }
            else if (strcmp(argv[i], "-ft") == 0)
            {
                i++;
                String::UnEscape(args.fTerm, argv[i]);
            }
            else if (strcmp(argv[i], "-v") == 0)
            {
                args.verbose = true;
            }
            else if (strcmp(argv[i], "-populated") == 0)
            {
                args.iOnlyPopulated = true;
            }
            else if (strcmp(argv[i], "-revise") == 0)
            {
                ++i;
                args.reviseMapFile = argv[i];
            }
            else if (strcmp(argv[i], "-stop") == 0)
            {
                i++;
                args.stFile = argv[i];
            }
            else if (strcmp(argv[i], "-update") == 0)
            {
                ++i;
                args.updateMapFile = argv[i];
            }
#ifdef DB_HASH_ID
            else if (strcmp(argv[i], "-hash") == 0)
            {
                args.iHash = atoi(argv[i]);
            }
#endif
            else if (strcmp(argv[i], "-dictOdb") == 0)
            {
                ++i;
                args.dictOdbFileName = argv[i];
            }
            else if (strcmp(argv[i], "-dictName") == 0)
            {
                ++i;
                args.dictName = argv[i];
            }
            else if (strcmp(argv[i], "-ns") == 0)
            {
                ++i;
                args.ns = argv[i];
            }
            else
            {
                usage(progName);
                throw InvalidOptionsException();
            }
        }
        else
        {
            usage(progName);
            throw InvalidOptionsException();
        }
    }

}


static void GetFileNames(vector<string>& fileNames, const string& lFile)
{

    fileNames.clear();

    ifstream infile(lFile.c_str());

    while (true)
    {
        string fileName;
        getline(infile, fileName);

        if (infile.eof() || infile.fail())
            break;

        fileNames.push_back(fileName);
    }

    infile.close();

}


static Db* CreateDb(Args& args, SchemaMap& schemaMapping)
{

    Db* dbP = NULL;

    unsigned int _serverType;

    const unsigned int _SERVER_TYPE_SYBASE = 1;
    const unsigned int _SERVER_TYPE_MYSQL = 2;
    const unsigned int _SERVER_TYPE_ORACLE = 3;
    const unsigned int _SERVER_TYPE_DB2 = 4;

    if (args.serverType.empty())
        args.serverType = "sybase";

    if (args.serverType == "sybase")
        _serverType = _SERVER_TYPE_SYBASE;
    else if (args.serverType == "mysql")
        _serverType = _SERVER_TYPE_MYSQL;
    else if (args.serverType == "oracle")
        _serverType = _SERVER_TYPE_ORACLE;
    else if (args.serverType == "db2")
        _serverType = _SERVER_TYPE_DB2;
    else
         return(dbP);

    string dbName;

    if (args.dbName.empty())
    {
        dbName = Db::DB_DEFAULT_NAME;
    }
    else
    {
        dbName = args.dbName;
    }

    switch (_serverType)
    {
        case _SERVER_TYPE_SYBASE:
        {
            dbP = new DbSybase(schemaMapping, dbName);
            break;
        }
        case _SERVER_TYPE_MYSQL:
        {
            dbP = new DbMySql(schemaMapping, dbName);
            dbP->SetAppendFlag(false);
            break;
        }
        case _SERVER_TYPE_ORACLE:
        {
            dbP = new DbOracle(schemaMapping, dbName);
            dbP->SetAppendFlag(true);
            break;
        }
        case _SERVER_TYPE_DB2:
        {
            dbP = new DbDb2(schemaMapping, dbName);
            args.rTerm.clear();
            args.fTerm.clear();
            args.rTerm.push_back('\n');
            args.fTerm.push_back(',');
            break;
        }
        default:
            return(dbP);
            break;
    }

    Db& db = *dbP;

    if (!args.rTerm.empty())
        db.SetRowSeparator(args.rTerm);
    if (!args.fTerm.empty())
        db.SetFieldSeparator(args.fTerm);
 
    if (args.iOnlyPopulated)
       db.SetUseOnlyPopulated();

    return(dbP);

}


static DbOutput* CreateDbOutput(Args& args, Db& db)
{

    DbOutput* dbOutputP = NULL;

    switch (args.mode)
    {
        case MODE_BCP:
        {
            dbOutputP = new BcpOutput(db);
            db.SetAppendFlag(true);
            break;
        }
        case MODE_SQL:
        {
            dbOutputP = new SqlOutput(db);
            db.SetAppendFlag(true);
            break;
        }
        case MODE_XML:
        {
            dbOutputP = new XmlOutput(db, args.dictOdbFileName, args.dictName,
              args.ns);
            db.SetAppendFlag(false);
            break;
        }
        default:
            return(dbOutputP);
            break;
    }

    return(dbOutputP);

}
 

int main(int argc, char* argv[])
{

    Args args;
    try
    {
    GetArgs(args, argc, argv);

    SchemaMap* schemaMappingP = new SchemaMap(args.mFile,
      args.mFileODB, args.verbose);

    if (!args.reviseMapFile.empty())
        schemaMappingP->SetReviseSchemaMode();

    Db* dbP = CreateDb(args, *schemaMappingP);
    if (dbP == NULL)
    {
        delete(schemaMappingP);
        return(1);
    }

    DbOutput* dbOutputP = CreateDbOutput(args, *dbP);
    if (dbOutputP == NULL)
    {
        delete(dbP);
        delete(schemaMappingP);
        return(1);
    }

    if (!args.updateMapFile.empty())
    {
        cout << "Updating schema map file in:" << args.updateMapFile << endl;

        // Read the revised schema mapping from revised mapping file
        SchemaMap* schU = new SchemaMap(args.reviseMapFile,
          string(), args.verbose);

        // Update current schema mapping with information from revised file
        schemaMappingP->updateSchemaMapDetails(*schU);

        // Write out the modified current schema mapping into the updated file
        schemaMappingP->ReviseSchemaMap(args.updateMapFile);

        delete(schU);
        delete(dbOutputP);
        delete(dbP);
        delete(schemaMappingP);

        return(0);
    }

    if (args.iSchema)
    {
        dbOutputP->WriteSchema();

        delete(dbOutputP);
        delete(dbP);
        delete(schemaMappingP);

        return(0);
    }

    DbLoader* dbl = new DbLoader(*schemaMappingP, *dbOutputP,
      args.verbose);

#ifdef DB_HASH_ID
    dbl->SetHashMode(args.iHash);
#endif

    if (args.iScript)
    {
        dbOutputP->SetInputFile(args.mFileODB);
        dbl->AsciiFileToDb(args.mFileODB, DbLoader::eSCRIPTS_ONLY);

        delete(dbl);
        delete(dbOutputP);
        delete(dbP);
        delete(schemaMappingP);

        return(0);
    }

    if (!args.iFile.empty())
    {
        dbOutputP->SetInputFile(args.iFile);
        dbl->AsciiFileToDb(args.iFile, DbLoader::eDATA_WITH_SCRIPTS);
    }
    else if (!args.lFile.empty())
    {
        vector<string> fileNames;

        GetFileNames(fileNames, args.lFile);

        unsigned int nFiles = fileNames.size();
        for (unsigned int i = 0; i < nFiles; ++i)
        {
            int istat = 1;

            time_t tStart, tEnd;

            time(&tStart);

            dbOutputP->SetInputFile(fileNames[i]);
            if (i == (nFiles - 1))
                // This is the last processed file. Also generate script.
                dbl->AsciiFileToDb(fileNames[i], DbLoader::eDATA_WITH_SCRIPTS);
            else
                dbl->AsciiFileToDb(fileNames[i], DbLoader::eDATA_ONLY);

            time(&tEnd);

            cout << "Loaded file " << fileNames[i] << " (" <<
              i + 1 << " of " << nFiles << ")" << " in " <<
              difftime(tEnd, tStart) << " seconds." << endl;

            struct stat statbuf;
            if (!args.stFile.empty())
                istat = stat(args.stFile.c_str(), &statbuf);

            if (istat == 0)
            {
                cout << "Stopping after file " << fileNames[i] <<
                  " (" << i + 1 << " of " << nFiles << ")" <<
                  " in " << difftime(tEnd, tStart) <<  " seconds." << endl;
                break;
            }
        }
    }

    if (!args.reviseMapFile.empty())
        schemaMappingP->ReviseSchemaMap(args.reviseMapFile);

    delete(dbl);
    delete(dbP);
    delete(dbOutputP);
    delete(schemaMappingP);
    }
    catch (const exception& exc)
    {
        cerr << exc.what();

        return(1);
    } 

}

