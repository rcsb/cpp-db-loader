/*$$FILE$$*/
/*$$VERSION$$*/
/*$$DATE$$*/
/*$$LICENSE$$*/

#include <string>
#include <vector>
#include <ostream>
#include <fstream>
 
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "GenString.h"
#include "CifParserBase.h"
#include "DictObjFile.h"
#include "DictObjCont.h"
#include "CifSchemaMap.h"

using std::string;
using std::vector;
using std::ostream;
using std::endl;
using std::cerr;
using std::ios;
using std::ofstream;

// using std::string::size_type;

// For controlling schema only related operations
const string SqlOutput::_SCHEMA_LOADING_SCRIPT =
  "DB_LOADER_SCHEMA_COMMANDS.csh";
const string SqlOutput::_SCHEMA_DELETE_FILE = "DB_LOADER_SCHEMA_DROP.sql";

// For controlling data loading only related operations
const string DbOutput::_DATA_LOADING_SCRIPT = "DB_LOADER_COMMANDS.csh";

// For deleting existing tables
const string BcpOutput::_DATA_DELETE_FILE = "DB_LOADER_DELETE.sql";

// For data loading
// For BCP loading done via SQL statements
const string DbMySql::_SQL_LOADING_FILE = "DB_LOADER_LOAD.sql";

// For BCP loading done via shell invocations of DB loader
const string Db::_SCRIPT_LOADING_FILE = "DB_LOADER_LOAD_COMMANDS.csh";

// For SQL loading via SQL statements
const string SqlOutput::_DATA_FILE = "DB_LOADER.sql";

const string DbLoader::_LOG_FILE = "SchemaMap.log";

const string Db::DB_DEFAULT_NAME = "msd1";

static void escapeString(string& outStr, const string& inStr);

const long _MAXFILESIZE = std::numeric_limits<std::streamsize>::max();

static void name_conversion_first_last(char *aString);


Db::Db(SchemaMap& schemaMapping, const string& dbName) :
  _schemaMapping(schemaMapping), _useOnlyPopulated(false), _appendFlag(false),
  _dbName(dbName), _firstTextNewLineSpecial(false)
{

  _fieldSeparator.push_back('\t');

  _rowSeparator.push_back('\n');

  _dataLoadingFileName = _SCRIPT_LOADING_FILE;

}


Db::~Db()
{

}


void Db::SetAppendFlag(const bool appendFlag)
{
    _appendFlag = appendFlag;
}


bool Db::GetAppendFlag()
{

    return(_appendFlag);

}


bool DbOutput::IsFirstTextNewLineSpecial()
{

    return(false);

}


const string& DbOutput::GetCommandScriptName()
{

    return(_DATA_LOADING_SCRIPT);

}


bool SqlOutput::IsFirstTextNewLineSpecial()
{

    return(_db.IsFirstTextNewLineSpecial());

}


bool Db::IsFirstTextNewLineSpecial()
{

    return(_firstTextNewLineSpecial);

}

void Db::SetFieldSeparator(const string& fieldSeparator)
{

    if (fieldSeparator.empty())
        return;

    _fieldSeparator = fieldSeparator;

}

void Db::SetRowSeparator(const string& rowSeparator)
{

    if (rowSeparator.empty())
        return;

    _rowSeparator = rowSeparator;

}


string Db::GetCommandTerm()
{

    return(_cmdTerm);

}

string Db::GetFieldSeparator()
{

    return(_fieldSeparator);

}


string Db::GetRowSeparator()
{

    return(_rowSeparator);

}


void DbMySql::GetStart(string& start)
{

    start = "USE " + _dbName + _cmdTerm + '\n';

}


void DbSybase::GetStart(string& start)
{

    start = "USE " + _dbName + _cmdTerm + '\n';

}


void Db::WriteSchemaStart(ostream& io)
{

    io << "USE " << _dbName << _cmdTerm << endl;

}


void Db::WriteDeleteTable(ostream& io, const string& fromTable,
  const string& where, const string& what)
{

    io << "DELETE FROM " << fromTable << " WHERE " << where << " = '"  <<
      what << "'" << _cmdTerm << endl;

}


void SqlOutput::WriteSqlScriptSchemaInfo(ostream& io)
{

    // VLAD - IMPROVE
    // WriteScriptSchemaInfo can be created and used by BcpOutput.
 
    WriteHeader(io);
    io << endl;

    io << "if (\"x$1\" == \"xdrop\") then " << endl;

    WriteDbExec(io, _SCHEMA_DELETE_FILE, 1);

    io << "endif" << endl;
    io << endl;

    WriteDbExec(io, _SCHEMA_FILE);

    io << endl;

}


void DbOutput::WriteDbExec(ostream& io, const string& fileName,
  const unsigned int indentLevel)
{

    for (unsigned int i = 0; i < indentLevel; ++i)
    {
        io << "    ";
    }

    io << "if (-e " << fileName << " && ! -z " << fileName << ") then" << endl;

    WriteDbExecOnly(io, fileName, indentLevel + 1);

    for (unsigned int i = 0; i < indentLevel; ++i)
    {
        io << "    ";
    }

    io << "endif" << endl;

}


void DbOutput::WriteDbExecOnly(ostream& io, const string& fileName,
  const unsigned int indentLevel)
{

    const string& connect = _db.GetConnect();

    if (!connect.empty())
    {
        for (unsigned int i = 0; i < indentLevel; ++i)
        {
            io << "    ";
        }

        io << connect << endl;

    }

    for (unsigned int i = 0; i < indentLevel; ++i)
    {
        io << "    ";
    }

    io << _db.GetDbCommand() << " " << fileName << endl;

    const string& terminate = _db.GetTerminate();

    if (!terminate.empty())
    {
        for (unsigned int i = 0; i < indentLevel; ++i)
        {
            io << "    ";
        }

        io << terminate << endl;
    }

}


void Db::DropTableSql(ostream& io, const string& tableNameDb)
{

    io << "DROP TABLE " << tableNameDb << _cmdTerm << endl;

}


const string& Db::GetExec()
{

    return(_exec);

}


const string& Db::GetExecOption()
{

    return(_execOption);

}


const string& Db::GetUserOption()
{

    return(_userOption);

}


const string& Db::GetPassOption()
{

    return(_passOption);

}


const string& Db::GetEnvDbUser()
{

    return(_envDbUser);

}


const string& Db::GetEnvDbPass()
{

    return(_envDbPass);

}


const string& Db::GetDataLoadingFileName()
{

    return(_dataLoadingFileName);

}


const string& Db::GetBcpStringDelimiter()
{

    return(_bcpStringDelimiter);

}


void Db::WriteLoadingStart(ostream& io)
{


}


void Db::WriteLoadingEnd(ostream& io)
{


}


void Db::WriteLoadingTable(ostream& io, const string& tableName,
  const string& workDir)
{


}


void Db::WritePrint(ostream& io, const string& tableNameDb)
{

}


void DbSybase::WritePrint(ostream& io, const string& tableNameDb)
{

    io << "PRINT \"Creating table " << tableNameDb << "\"" << endl;

}


void Db::GetChar(string& dType, const unsigned int width)
{

    dType = "varchar(" + String::IntToString(width) + ")";

}


void Db::GetText(string& dType, const unsigned int width)
{


}


void DbOracle::GetText(string& dType, const unsigned int width)
{

    if (width > 4000)
    {
        dType = "varchar(4000)";
    }
    else
    {
        dType = "clob";
    }

}


void DbDb2::GetText(string& dType, const unsigned int width)
{

    if (width == 0)
    {
        dType = "varchar(4000)";
    }
    else if (width < 32000)
    {
        dType = "varchar(" + String::IntToString(width) + ")";
    }
    else
    {
        dType = "clob(" + String::IntToString(width) + ")";
    }

}


void Db::GetFloat(string& dType)
{

}


void Db::GetDate(string& dType)
{

}

void Db::WriteNull(ostream& io, const int iNull,
  const unsigned int curr, const unsigned int attSize)
{

}


void Db::WriteBcpDoubleQuotes(ostream& io)
{

    io << "\"";

}


void DbOutput::_FormatData(ostream& io, const string& cs,
  const unsigned int type, const unsigned int width)
{
    switch (type)
    {
        case eTYPE_CODE_INT: 
        case eTYPE_CODE_FLOAT:
        case eTYPE_CODE_BIGINT:
        {
            _FormatNumericData(io, cs);
            break;
        }
        case eTYPE_CODE_STRING:
        {
            _FormatStringData(io, cs, width);
            break;
        }
        case eTYPE_CODE_TEXT:
        {
            _FormatTextData(io, cs);
            break;
        }
        case eTYPE_CODE_DATETIME:
        {
            _FormatDateData(io, cs, width);
            break;
        }
        default:
        {
            throw out_of_range("Invalid type code in DbOutput::_FormatData");
        }
    }
}


void DbOutput::WriteNewLine(ostream& io, bool special)
{

    io << "\n";

}


void DbOutput::GetTableStart(string& tableStart, const string& tableName)
{

    tableStart.clear();

}


void DbOutput::GetTableEnd(string& tableEnd)
{

    tableEnd.clear();

}


void SqlOutput::GetTableStart(string& tableStart, const string& tableName)
{

    tableStart.clear();

    if (tableName.empty())
    {
        return;
    }

    string tableNameDb;
    _db._schemaMapping.GetTableNameAbbrev(tableNameDb, tableName);

    tableStart = "INSERT INTO " + tableNameDb + "\n" + "VALUES (";
 
}


void SqlOutput::GetTableEnd(string& tableEnd)
{

    tableEnd = "\n)" + _db.GetCommandTerm() + "\n\n";

}


const string& DbOutput::GetItemSeparator()
{

    return(_itemSeparator);

}


const string& DbOutput::GetRowSeparator()
{

    return(_rowSeparator);

}


void DbOutput::_WriteTable(ostream& io, ISTable* tIn,
  vector<unsigned int>& widths, const bool reCalcWidth,
  const vector<eTypeCode>& typeCodes)
{
    if (!io || !tIn)
        return;

    string tableStart;
    GetTableStart(tableStart, tIn->GetName());

#ifdef DEBUG_POINT
    if (tIn->GetName() == "citation")
    {
        int a = 1;
        int b = a + 3;
        ++b;
    }
#endif

    string tableEnd;
    GetTableEnd(tableEnd);

    // Get all of the attributes for this table.
    const vector<AttrInfo>& aI =
      _db._schemaMapping.GetTableAttributeInfo(tIn->GetName(),
      tIn->GetColumnNames(), tIn->GetColCaseSense());

    unsigned int nRows = tIn->GetNumRows();
    unsigned int nCols = tIn->GetNumColumns();

    for (unsigned int i = 0; i < nRows; ++i)
    {
        const vector<string>& row = tIn->GetRow(i);

        if (row.empty())
        {
            continue;
        }

        if (!SchemaMap::AreValuesValid(row, aI))
        {
            continue;
        }

        io << tableStart;

        for (unsigned int j = 0; j < nCols; ++j)
        {
	    if (j != 0)
                io << GetItemSeparator();

            if (reCalcWidth)
            {
                if (row[j].size() > widths[j])
                    widths[j] = row[j].size();
            }

            _FormatData(io, row[j], typeCodes[j], widths[j]);
        }

        io << GetRowSeparator();

        io << tableEnd;
    }
}


void SqlOutput::WriteNewLine(ostream& io, bool special)
{

    _db.WriteNewLine(io, special);

}


void Db::WriteNewLine(ostream& io, bool special)
{

    io << "\n";

}


void DbMySql::WriteNewLine(ostream& io, bool special)
{

    io << "\\n";

}


void DbOracle::WriteNewLine(ostream& io, bool special)
{

    if (special)
        io << " \\" << endl;
    else
        io << "\\" << endl;

}


void DbDb2::WriteBcpDoubleQuotes(ostream& io)
{

    io << "\"\"";

}


void DbDb2::WriteNull(ostream& io, const int iNull,
  const unsigned int curr, const unsigned int attSize)
{

    if (iNull)
    {
        io << "not null";
    }
    else
    {
        io << "        ";
    }

    io << "," << endl;

}


void DbOracle::WriteNull(ostream& io, const int iNull,
  const unsigned int curr, const unsigned int attSize)
{

    if (iNull)
    {
        io << "not null";
    }
    else
    {
        io << "    null";
    }

    io << "," << endl;

}


void DbMySql::WriteNull(ostream& io, const int iNull, const unsigned int curr,
  const unsigned int attSize)
{

    if (iNull)
    {
        io << "not null";
    }
    else
    {
        io << "    null";
    }

    if (curr < attSize - 1)
        io << "," << endl;
    else
        io << endl;

}


void DbSybase::WriteNull(ostream& io, const int iNull,
  const unsigned int curr, const unsigned int attSize)
{

    if (iNull)
    {
        io << "not null";
    }
    else
    {
        io << "    null";
    }

    if (curr < attSize - 1)
        io << "," << endl;
    else
        io << endl;

}


void Db::WriteTableIndex(ostream& io, const string& tableNameDb,
      const vector<string>& indexList)
{

}


void DbOracle::WriteTableIndex(ostream& io, const string& tableNameDb,
      const vector<string>& indexList)
{

    io << "PRIMARY KEY (";

    for (unsigned int i = 0; i < indexList.size(); ++i)
    {
        io << indexList[i];

        if (i < indexList.size() - 1)
            io << ",";
        else
            io << " ";
    }

    io << ")" << endl;

    io << ")" <<  _cmdTerm << endl;  // end of create table clause

}


void DbDb2::WriteTableIndex(ostream& io, const string& tableNameDb,
      const vector<string>& indexList)
{

    io << "PRIMARY KEY (";
    for (unsigned int i = 0; i < indexList.size(); ++i)
    {
        io << indexList[i];

        if (i < indexList.size() - 1)
            io << ",";
        else
            io << " ";
    }

    io << ")" << endl;

    io << ")" <<  _cmdTerm << endl;  // end of create table clause


    if (!indexList.empty())
    {
        io << _cmdTerm << endl;
    }

}


void DbMySql::WriteTableIndex(ostream& io, const string& tableNameDb,
      const vector<string>& indexList)
{

    io << ")" <<  _cmdTerm << endl << endl;  // end of create table clause

    if (!indexList.empty())
    {
        io << "CREATE UNIQUE INDEX primary_index ON " << tableNameDb << endl;
        io << "(" << endl;
        unsigned int indLen  = indexList.size();
        if (indLen > 16) indLen = 16;
        for (unsigned int i = 0; i < indLen; ++i)
        {
            io << indexList[i];
            if (i < indLen - 1)
                io << "," << endl;
            else
                io << endl;
        }

        io << ")";

        io << _cmdTerm << endl;
    }

}


void DbSybase::WriteTableIndex(ostream& io, const string& tableNameDb,
      const vector<string>& indexList)
{

    io << ")" <<  _cmdTerm << endl << endl;  // end of create table clause


    if (!indexList.empty())
    {
        io << "CREATE CLUSTERED INDEX primary_index ON " << tableNameDb <<
          endl;
        io << "(" << endl;
        for (unsigned int i = 0; i < indexList.size(); ++i)
        {
            io << indexList[i];
            if (i < indexList.size() - 1)
                io << "," << endl;
            else
                io << endl;
        }
        io << ")";

        io << " WITH ignore_dup_row" << _cmdTerm << endl;
    }

}


void DbOracle::GetDate(string& dType)
{

    dType = "date";

}


void DbDb2::GetDate(string& dType)
{

    dType = "date";

}


void DbDb2::GetFloat(string& dType)
{

    dType = "decimal(14,4)";

}


const string& Db::GetConnect()
{

    return(_connect);

}


const string& Db::GetTerminate()
{

    return(_terminate);

}


const string& Db::GetDbCommand()
{

    return(_dbCommand);

}


DbOracle::DbOracle(SchemaMap& schemaMapping, const string& dbName) :
  Db(schemaMapping, dbName)
{

    _cmdTerm.push_back(';');

    _dbCommand = "$ORACLE_HOME/bin/sqlplus $dbuser/$dbpw@" + _dbName + " <";

    _envDbUser = "NDB_XDBUSER";
    _envDbPass = "NDB_XDBPW";

    _firstTextNewLineSpecial = true;

}


DbOracle::~DbOracle()
{

}


void DbOracle::WriteLoadingStart(ostream& io)
{

    io << "#!/bin/csh -f" << endl << endl;

    io << "set dbuser = $" << GetEnvDbUser() << endl;
    io << "set dbpw = $" << GetEnvDbPass() << endl;

    io << endl;

}


void DbOracle::WriteLoadingTable(ostream& io, const string& tableName,
  const string& workDir)
{

    string fs;
    escapeString(fs, _fieldSeparator);

    string rs;
    escapeString(rs, _rowSeparator);

    string tableNameDb;
    _schemaMapping.GetTableNameAbbrev(tableNameDb, tableName);

    vector<string> colNames;
    _schemaMapping.GetAttributeNames(colNames, tableName);
    if (colNames.empty())
    {
        return;
    }

    unsigned int nCols = colNames.size();

    const vector<AttrInfo>& aI = _schemaMapping.GetAttributesInfo(tableName);

    string directOption = "true";
    for (unsigned int j = 0; j < nCols - 1; ++j)
    {
        if (aI[j].iTypeCode == eTYPE_CODE_TEXT)
        {
            directOption = "false";
            break;
        }
    }

    string extension = ".ctl";

    for (unsigned int filesI = 0; filesI < 4; ++filesI, extension += "+")
    {
        io << "if (-e " << tableName << extension << " && ! -z " <<
          tableName << extension << ") then" << endl;
        io << "   echo \"Loading table " << tableName << "\" "<< endl;

        io << "   $ORACLE_HOME/bin/sqlldr $dbuser/$dbpw@" << _dbName <<
          " " << tableName << extension << " direct=" << directOption <<
          endl;

        io << "endif" << endl;

        io << endl;
    }

    struct stat statbuf;

    string tName = workDir + tableName + ".bcp";

    string oFile = workDir + tableName + ".ctl";

    string bFile = workDir + tableName + ".bad";

    bool ifirst = true;

    int istat = stat(tName.c_str(), &statbuf);
    while (ifirst || (istat == 0 && statbuf.st_size > 0))
    {
#ifdef VLAD_LOG_SEPARATION
        if (_verbose) _log << "Writing control file for " << tName << endl;
#endif
        ofstream ioctl(oFile.c_str(), ios::out | ios::trunc);

        ifirst = false;

        ioctl << "OPTIONS ( ERRORS=100000 )" << endl;
        ioctl << "LOAD DATA" << endl;
        ioctl << "INFILE '" << tName << "'  \"str '" << rs << "'\"" << endl;
        ioctl << "BADFILE '" << bFile <<"'" << endl;
        ioctl << "DISCARDMAX 100000"  << endl;
  
        if (_appendFlag)
            ioctl << "APPEND" << endl;
        else 
            ioctl << "TRUNCATE" << endl;
      
        ioctl << "INTO TABLE " << tableNameDb << endl;
        ioctl << "FIELDS TERMINATED BY '" << fs << "'" << endl;
        ioctl << "TRAILING NULLCOLS" << endl;
  
        ioctl << "(";
  
        for (unsigned int j = 0; j < nCols; ++j)
        {
            string columnNameDb;
            _schemaMapping.GetAttributeNameAbbrev(columnNameDb,
              tableName, colNames[j]);

            switch (aI[j].iTypeCode)
            {
                case eTYPE_CODE_INT: 
                case eTYPE_CODE_FLOAT:
	        case eTYPE_CODE_BIGINT:
	            ioctl <<  columnNameDb;
	            break;
                case eTYPE_CODE_STRING:
                    ioctl <<  columnNameDb << " char(" <<
                      aI[j].iWidth << ")";
                    break;
                case eTYPE_CODE_TEXT:
                    ioctl << columnNameDb << " char(100000)";
                    break;
                case eTYPE_CODE_DATETIME:
                    ioctl << columnNameDb <<
                      " date(30) \"YYYY-MM-DD HH24:MI:SS\"";
                    break;
                default:
                {
                    throw out_of_range("Invalid type code in "\
                      "DbOracle::WriteLoadingTable");
                }
            }

	    if (j != nCols-1)
                ioctl << "," << endl;
        }
        ioctl << ")" << endl << endl;
        ioctl.close();

        // +++++++++++++++++
        tName += "+";
        istat = stat(tName.c_str(),&statbuf);
        oFile += "+";
        bFile += "+";
    }

}


void Db::GetStart(string& start)
{

    start.clear();

}


void DbOracle::WriteSchemaStart(ostream& io)
{

    io << "SET DEFINE OFF" << _cmdTerm << endl;
    io << "SET ESCAPE \\" << _cmdTerm << endl;

}


DbDb2::DbDb2(SchemaMap& schemaMapping, const string& dbName) :
  Db(schemaMapping, dbName)
{

    _cmdTerm.push_back(';');

    _connect = "db2 CONNECT TO " + _dbName + " USER $dbuser USING $dbpw";
    _terminate = "db2 TERMINATE ";
    _dbCommand = "db2 -tf";
    _envDbUser = "DB2_DB_USER";
    _envDbPass = "DB2_DB_PW";

    _bcpStringDelimiter = "\"";

}


DbDb2::~DbDb2()
{

}


void DbDb2::GetStart(string& start)
{

    start = "CONNECT TO " + _dbName + " USER $dbuser USING $dbpw" +
      _cmdTerm + '\n';

}


void DbDb2::WriteSchemaStart(ostream& io)
{

    string schemaStart;
    GetStart(schemaStart);

    io << schemaStart;

}


void SqlOutput::CreateTableSql(ostream& io, const string& tableName)
{

    if (!io)
        return;

    const vector<AttrInfo>& attrInfo =
      _db._schemaMapping.GetAttributesInfo(tableName);

    if (attrInfo.empty())
    {
        return;
    }

    string tableNameDb;
    _db._schemaMapping.GetTableNameAbbrev(tableNameDb, tableName);

    _db.WritePrint(io, tableNameDb);

    io << "CREATE TABLE " << tableNameDb << endl;
    io << "(" << endl;

    vector<string> indexList;

    for (unsigned int i = 0; i < attrInfo.size(); ++i)
    {
        if (_db.GetUseOnlyPopulated() && (attrInfo[i].populated != "Y"))
        {
            continue;
        }

        const string& columnName = attrInfo[i].attribName;

        string columnNameDb;
        _db._schemaMapping.GetAttributeNameAbbrev(columnNameDb,
          tableName,columnName);

        unsigned int iLen = columnNameDb.size();
        if (iLen > _MAX_SQL_NAME_LENGTH)
            iLen = _MAX_SQL_NAME_LENGTH;
        for (unsigned int j = 0; j < iLen; ++j)
        {
            io << columnNameDb[j];
        }

        for (unsigned int j = iLen; j < _MAX_SQL_NAME_LENGTH+1; ++j)
        {
            io << " ";
        }

        string dType = attrInfo[i].dataType;

        if (String::IsCiEqual(attrInfo[i].dataType, "char"))
        {
            _db.GetChar(dType, attrInfo[i].iWidth);
        }

        if (String::IsCiEqual(attrInfo[i].dataType, "text"))
        {
            _db.GetText(dType, attrInfo[i].iWidth);
        }

        if (String::IsCiEqual(attrInfo[i].dataType, "datetime"))
        {
            _db.GetDate(dType);
        }

        if (String::IsCiEqual(attrInfo[i].dataType, "float"))
        {
            _db.GetFloat(dType);
        }

        io << dType;
        for (unsigned int j = dType.size(); j < 15; ++j)
        {
            io << " ";
        }

        _db.WriteNull(io, attrInfo[i].iNull, i, attrInfo.size());

        if (attrInfo[i].iIndex)
            indexList.push_back(columnNameDb);
    }

    io << endl;

    _db.WriteTableIndex(io, tableNameDb, indexList);

}

void DbDb2::WriteLoadingStart(ostream& io)
{

    io << "#!/bin/csh -f" << endl << endl;

    io << "set dbuser = $" << GetEnvDbUser() << endl;
    io << "set dbpw = $" << GetEnvDbPass() << endl;

    io << endl;

    io << _connect << endl;

    io << endl;

}


void DbDb2::WriteLoadingEnd(ostream& io)
{

    io << endl;
    io << _terminate << endl;

}


void DbDb2::WriteLoadingTable(ostream& io, const string& tableName,
  const string& workDir)
{

    string tableNameDb;
    _schemaMapping.GetTableNameAbbrev(tableNameDb, tableName);

#ifdef VLAD_LOG_SEPARATION
    if (_verbose) _log  << "Writing DB2 load commands for table "
      <<  tableNameDb << endl;
#endif

    string tName = workDir + tableName + ".bcp";

    string mFile = workDir + tableName + ".msg";

    struct stat statbuf;

    int istat = stat(tName.c_str(), &statbuf);
    while (istat == 0 && statbuf.st_size > 0)
    {
        io << "echo \"Loading data from file  " << tName << "\"" << endl; 
        io << "db2 \"load from " << tName <<
          " of del modified by delprioritychar anyorder ";
        io << " messages " << mFile << " insert into ";
        io << tableNameDb << "\"" <<  endl;
        io << endl;
      
        // +++++++++++++++++
        tName += "+";
        istat = stat(tName.c_str(), &statbuf);
    }

}


DbMySql::DbMySql(SchemaMap& schemaMapping, const string& dbName) :
  Db(schemaMapping, dbName)
{

    _cmdTerm.push_back(';');

    _exec = "mysql";
    _execOption = "-f ";
    _userOption = "--user=";
    _passOption = "--password=";
 
    _dbCommand = _exec + " " + _execOption + _userOption +
      "$dbuser" + " " + _passOption + "$dbpw <";

    _envDbUser = "NDB_XDBUSER";
    _envDbPass = "NDB_XDBPW";

    _dataLoadingFileName = _SQL_LOADING_FILE;
}


DbMySql::~DbMySql()
{

}


void DbMySql::DropTableSql(ostream& io, const string& tableNameDb)
{

    io << "DROP TABLE IF EXISTS " << tableNameDb << _cmdTerm << endl;

}


void DbMySql::WriteLoad(ostream& io)
{

    io << "mysql -v -f --user=$dbuser --password=$dbpw < " <<
      GetDataLoadingFileName()  << endl;

}


void DbMySql::WriteLoadingStart(ostream& io)
{

    io << "USE " << _dbName << _cmdTerm << endl;

}


void DbMySql::WriteLoadingTable(ostream& io, const string& tableName,
  const string& workDir)
{
    string fs;
    escapeString(fs, _fieldSeparator);

    string rs;
    escapeString(rs, _rowSeparator);

    string tName = workDir + tableName + ".bcp";

    struct stat statbuf;
    int istat = stat(tName.c_str(), &statbuf);
    while (istat == 0 && statbuf.st_size > 0)
    {
        string tableNameDb;
        _schemaMapping.GetTableNameAbbrev(tableNameDb, tableName);

        io << "LOAD DATA LOCAL INFILE \"" << tName << "\"" << endl
          << "IGNORE INTO TABLE " << tableNameDb << endl
          << "FIELDS TERMINATED BY '" << fs << "'" << endl
          << "LINES  TERMINATED BY '" << rs << "'" << _cmdTerm << endl <<
          endl;

        tName += "+";
        istat = stat(tName.c_str(), &statbuf);      
    }
}


DbSybase::DbSybase(SchemaMap& schemaMapping, const string& dbName) :
  Db(schemaMapping, dbName)
{

    _cmdTerm.push_back('\n');
    _cmdTerm.push_back('G');
    _cmdTerm.push_back('O');

    _exec = "$SYBASE/bin/isql";
    _userOption = "-U";
    _passOption = "-P";

    _dbCommand = _exec + " " + _execOption + _userOption +
      "$dbuser" + " " + _passOption + "$dbpw <";

    _envDbUser = "NDB_XDBUSER";
    _envDbPass = "NDB_XDBPW";

}


DbSybase::~DbSybase()
{

}


void DbSybase::WriteLoadingStart(ostream& io)
{

    io << "#!/bin/csh -f" << endl << endl;

    io << "set dbuser = $" << GetEnvDbUser() << endl;
    io << "set dbpw = $" << GetEnvDbPass() << endl;

    io << endl;

}


void DbSybase::WriteLoadingTable(ostream& io, const string& tableName,
  const string& workDir)
{
    string fs;
    string rs;
    escapeString(fs, _fieldSeparator);
    escapeString(rs, _rowSeparator);

    io << "if (-e " << tableName << ".bcp && ! -z " 
      << tableName << ".bcp) then" << endl;
    io << "   echo \"Loading table " << tableName << "\" "<< endl;
    io << "   $SYBASE/bin/bcp " << _dbName << ".." << tableName <<
      " in " << tableName << ".bcp -c -U$dbuser -P$dbpw -b 10000" <<
      " -t '" << fs  << "'" << " -r '" << rs  << "'" << " -Y" << endl;
    io << "endif" << endl << endl;
}


DbOutput::DbOutput(Db& db) : _db(db)
{

}


DbOutput::~DbOutput()
{

}


void DbOutput::WriteData(Block& block, const string& workDir)
{

}


void DbOutput::WriteSchema(const string& workDir)
{

}


void DbOutput::WriteDataLoadingScripts(const string& workDir)
{

}


void DbOutput::WriteEmptyNumeric(ostream& io)
{

}


void DbOutput::WriteEmptyString(ostream& io)
{

}


void DbOutput::WriteEmptyDate(ostream& io)
{

}


void SqlOutput::WriteEmptyDate(ostream& io)
{

    WriteEmptyString(io);

}


void BcpOutput::WriteEmptyString(ostream& io)
{

    io << _stringDelimiter << _stringDelimiter;

}


void SqlOutput::WriteEmptyString(ostream& io)
{

    io << "NULL";

}


void DbOutput::WriteSpecialChar(ostream& io, const char& specChar)
{

    io << specChar << specChar;

}


void DbOutput::WriteSpecialDateChar(ostream& io, const char& specDateChar)
{


}


void BcpOutput::WriteSpecialDateChar(ostream& io, const char& specDateChar)
{

    if (specDateChar == '"')
        _db.WriteBcpDoubleQuotes(io);

}


void DbOutput::_FormatNumericData(ostream &io,  const string &cs)
{

    if (CifString::IsEmptyValue(cs))
    {
        WriteEmptyNumeric(io); 
        return;
    }

    unsigned int len = cs.size();
    for (unsigned int i = 0; i < len; ++i)
    {
        if (isspace(cs[i]))
        {
            // Skip any white space
            continue;
        }
        else
        {
            io << cs[i];
        }
    }

}


void SqlOutput::WriteEmptyNumeric(ostream& io)
{

    io << "NULL";

}


void DbOutput::_FormatStringData(ostream& io,  const string& cs,
  const unsigned int maxWidth)
{

    // A null string or the special CIF characters are treated as NULL . 
    if (CifString::IsEmptyValue(cs))
    {
        WriteEmptyString(io);
        return;
    }

    io << _stringDelimiter;

    unsigned int len = cs.size();

    if (len > maxWidth)
        len = maxWidth;

    bool inLeadWhiteSpace = true;

    for (unsigned int i = 0; i < len; ++i)
    {
        // Skip the leading white space
        if (isspace(cs[i]) && inLeadWhiteSpace)
        {
            continue;
        }
        else if (isspace(cs[i]))
        {
            // Convert all white space to SPACE
            io << " ";
            inLeadWhiteSpace = false;
        }
        else if (IsSpecialChar(cs[i]))
        {
            // Double up double quotes
            WriteSpecialChar(io, cs[i]);
            inLeadWhiteSpace = false;
        }
        else
        {
            io << cs[i];
            inLeadWhiteSpace = false;
        }
    }

    io << _stringDelimiter;

}


void DbOutput::_FormatDateData(ostream& io, const string& cs,
  const unsigned int maxWidth)
{

    if (CifString::IsEmptyValue(cs))
    {
        WriteEmptyDate(io);
        return;
    }

    io << _dateDelimiter;

    unsigned int len = cs.size();

    if (len > maxWidth)
        len = maxWidth;

    bool inLeadWhiteSpace = true;

    for (unsigned int i = 0; i < len; ++i)
    {
        if (isspace(cs[i]) && inLeadWhiteSpace)
        {
            continue;
        }
        else if (isspace(cs[i]))
        {
            io << " ";
            // VLAD - SHOULD WE PUT inLeadWhiteSpace = false???
        }
        else if (IsSpecialDateChar(cs[i]))
        {
            // Double up double quotes
            WriteSpecialDateChar(io, cs[i]);
            inLeadWhiteSpace = false;
        }
        else
        {
            io << cs[i];
            inLeadWhiteSpace = false;
        }
    }

    io << _dateDelimiter;

}


void DbOutput::_FormatTextData(ostream &io,  const string &cs)
{

    // A null string or the special CIF characters are treated as NULL . 
    if (CifString::IsEmptyValue(cs))
    {
        WriteEmptyString(io);
        return;
    }

    io << _stringDelimiter;

    unsigned int len = cs.size();

    for (unsigned int i = 0; i < len; ++i)
    {
        if ((i == 0) && (cs[i] == '\n') && IsFirstTextNewLineSpecial())
	    continue;
        else if (isspace(cs[i]) && cs[i] != '\n')
            // convert all white space to SPACE, except newlines
	    io << " ";
        else if (cs[i] == '\n')
        {
            if ((i != 0) && (cs[i - 1] == '\n'))
                WriteNewLine(io, true);
            else
                WriteNewLine(io);
        }
        else if (IsSpecialChar(cs[i]))
            WriteSpecialChar(io, cs[i]);
        else
	    io << cs[i];
    }

    io << _stringDelimiter;

}


bool DbOutput::IsSpecialChar(const char& character)
{

    vector<char>::const_iterator result = find(_specialChars.begin(),
      _specialChars.end(), character);

    if (result == _specialChars.end())
        return(false);
    else
        return(true);

}


bool DbOutput::IsSpecialDateChar(const char& character)
{

    vector<char>::const_iterator result = find(_specialDateChars.begin(),
      _specialDateChars.end(), character);

    if (result == _specialDateChars.end())
        return(false);
    else
        return(true);

}


void DbOutput::WriteHeader(ostream& io)
{

    io << "#!/bin/csh -f" << endl << endl;

    io << "set dbuser = $" << _db.GetEnvDbUser() << endl;
    io << "set dbpw = $" << _db.GetEnvDbPass() << endl;

}


BcpOutput::BcpOutput(Db& db) : DbOutput(db)
{

    _stringDelimiter = _db.GetBcpStringDelimiter();

    _specialChars.push_back('"');

    _specialDateChars.push_back('"');

    _itemSeparator = _db.GetFieldSeparator();

    _rowSeparator = _db.GetRowSeparator();

}


BcpOutput::~BcpOutput()
{

}


SqlOutput::SqlOutput(Db& db) : DbOutput(db)
{

    _SCHEMA_FILE = "DB_LOADER_SCHEMA.sql";

    _stringDelimiter = "'";

    _specialChars.push_back('\'');

    _dateDelimiter = "'";

    _itemSeparator = ",\n";

}


SqlOutput::~SqlOutput()
{

}


void DbLoader::Clear() {
   

  _verbose       = false;

  _blockName.clear();

}


void DbLoader::_OpenLog(const string& logName)
{
  if (_verbose && !logName.empty())
      _log.open(logName.c_str(),ios::out|ios::trunc);
}



DbLoader::DbLoader(SchemaMap& schemaMapping, DbOutput& dbOutput,
  bool verbose, const string& workDir) : _schemaMapping(schemaMapping),
  _dbOutput(dbOutput)
{

  Clear();

  _workDir = workDir;
#ifdef DB_HASH_ID
  _hashMode=1;
#endif
  _verbose = verbose;

  // Set default values  ...  These can be overridden if necessary .

  _blockName = "loadable";

  if (_verbose) {
    string logName = _workDir + _LOG_FILE;
    _OpenLog(logName);
  }

}


DbLoader::~DbLoader()
{
    if (_verbose)
        _log.close();
}


void DbLoader::SetWorkDir(const string& workDir)
{

    if (workDir.empty())
        return;

    _workDir = workDir;

}


void Db::SetUseOnlyPopulated(bool mode)
{
    _useOnlyPopulated = mode;
}


bool Db::GetUseOnlyPopulated()
{
    return(_useOnlyPopulated);
}


#ifdef DB_HASH_ID
void DbLoader::SetHashMode(int mode)
{
    _hashMode=mode;
}
#endif

void DbLoader::AsciiFileToDb(const string& inpFile, const eConvOpt convOpt) 
{

    _INPUT_FILE = inpFile;

    CifFile* fobjR = NULL;

    if (convOpt != eSCRIPTS_ONLY)
    {
        fobjR = new CifFile(_verbose, Char::eCASE_SENSITIVE,
          SchemaMap::_MAX_LINE_LENGTH);  

        if (_verbose)
            _log << "Reading input file  " << inpFile << endl;

        CifParser* cifParserP = NULL;
        cifParserP = new CifParser(fobjR, fobjR->GetVerbose());

        string diags;
        cifParserP->Parse(inpFile, diags);

        delete(cifParserP);

        if (!diags.empty())
        {
            if (_verbose) _log << " Diagnostics [" << diags.size() << "] " <<
              diags.c_str() << endl;
            diags.clear();
        }
    }

    FileObjToDb(*fobjR, convOpt);

    if (convOpt != eSCRIPTS_ONLY)
    {
        delete (fobjR);
    }

}


void DbLoader::SerFileToDb(const string& inpObjFile, const eConvOpt convOpt)
{

    _INPUT_FILE = inpObjFile;

    CifFile* fobjR = NULL;

    if (convOpt != eSCRIPTS_ONLY)
    {
        fobjR = new CifFile(READ_MODE, inpObjFile, _verbose,
          Char::eCASE_SENSITIVE, SchemaMap::_MAX_LINE_LENGTH);
    }

    FileObjToDb(*fobjR, convOpt);

    if (convOpt != eSCRIPTS_ONLY)
    {
        delete (fobjR);
    }

}


void DbLoader::FileObjToDb(CifFile& fobjR, const eConvOpt convOpt)
{
    if (convOpt != eSCRIPTS_ONLY)
    {
        CifFile* fobjW = new CifFile(_verbose, Char::eCASE_SENSITIVE,
          SchemaMap::_MAX_LINE_LENGTH);  

#ifdef VLAD_LATER_FIX
        CreateTables(*fobjW, fobjR);
#else
        fobjW->AddBlock(_blockName);
        _schemaMapping.CreateTables(*fobjW, _blockName);
#endif

        if (_verbose)
            _log << "Done with _CreateTable()" << endl;

        vector<string> blockNames;
        fobjR.GetBlockNames(blockNames);

        Block& wBlock = fobjW->GetBlock(_blockName);

        unsigned int numBlocks = blockNames.size();
        for (unsigned i = 0; i < numBlocks; ++i)
        {
            if (blockNames[i].empty())
            {
                cerr << "Skipping unnamed block " << endl;
                continue;
            }

            if (_verbose)
                _log << "Loading block " << i + 1 << " " << blockNames[i] <<
                  endl;
 
#ifdef DB_HASH_ID
            if (_hashMode == 1)
            {
                fobjR->GetAttributeValueIf(_HASH_ID, blockNames[i],
                  "database_2", "database_code", "database_id", "PDB");

                if (_HASH_ID.empty())
                {
                    fobjR->GetAttributeValueIf(_HASH_ID, blockNames[i],
                      "database_2", "database_code", "database_id", "NDB");
                }

                if (_HASH_ID.empty())
                {
                    cerr << "Skipping block with no hashable ID " <<
                      blockNames[i] <<endl;
                    continue;
	        }
            }
            else
            {
	        _HASH_ID = blockNames[i];
            }

            if (_verbose)
                _log << "Hash id for block " << i+1 << " " <<
                  blockNames[i] << " is " << _HASH_ID <<endl;
#endif

            Block& rBlock = fobjR.GetBlock(blockNames[i]);

            _LoadBlock(rBlock, wBlock);
        }

        if (_verbose)
            _log << "Done with _LoadBlock()" << endl;

#ifdef VLAD_DEBUG
        fobjW->Write("TempFile.cif");
#endif
        _dbOutput.WriteData(wBlock, _workDir);

        delete (fobjW);
    }

    if (convOpt != eDATA_ONLY)
    {
        _dbOutput.WriteDataLoadingScripts(_workDir);
    }

}


void DbLoader::_LoadBlock(Block& rBlock, Block& wBlock)
{

    // Create tables in the target schema ...

    // List of tables to be updated in the current schema ... 
    vector<string> tList;

    _schemaMapping.GetAllTablesNames(tList);

    for (unsigned int i = 0; i < tList.size(); ++i)
    {
        if (_verbose)
        {
            _log << " -------------------------------------------"\
              "--------------------" << endl;
            _log << "Loading table "  << i+1 << " " << tList[i] <<
              " of " << tList.size() << endl;
        }

        ISTable* t = wBlock.GetTablePtr(tList[i]);
        if (t == NULL)
        {
            if (_verbose) _log << "Missing output table " << tList[i] << endl;
            continue;
        }

        // For every item (i.e. attribute) of the category (i.e. table), get
        // the info.
        const vector<AttrInfo>& aI = _schemaMapping.GetTableAttributeInfo(
          t->GetName(), t->GetColumnNames(), t->GetColCaseSense());

        vector<string> cList;

        // Find all the schema defined attributes of the table and put
        // them in cList
        _schemaMapping.GetAttributeNames(cList, tList[i]);
        if (cList.empty())
        {
            continue;
        }

        unsigned int nCols = cList.size();

        if (_verbose)
            _log << "Schema defines " << nCols << " attributes " << endl;

        vector<vector<string> > mappedAttrInfo;
        _schemaMapping.GetMappedAttributesInfo(mappedAttrInfo, tList[i]);
        if (mappedAttrInfo.empty())
        {
            if (_verbose)
                _log << "No mapped attributes for " << tList[i] << endl;      
            continue; 
        }

        // Get mapped attribute names of this table
        const vector<string>& cNameMap = mappedAttrInfo[0];

        // Get source CIF item names of the mapped attribute names of this table
        const vector<string>& iNameMap = mappedAttrInfo[1];

        // Get condition id and function id of the mapped attribute names of
        //  this table
        const vector<string>& cIdMap = mappedAttrInfo[2];
        const vector<string>& fIdMap = mappedAttrInfo[3];

        // Number of mapped attributes of this table
        unsigned int nColsMap = cNameMap.size();


        //
        // get column indices of mapped attributes in the table.
        //
        vector<int> indMap;
        indMap.insert(indMap.begin(), nColsMap, 0);

        unsigned int ierr = 0;
        for (unsigned int j = 0; j < nColsMap; ++j)
        {
            if (!t->IsColumnPresent(cNameMap[j]))
            {
	        if (_verbose)
                    _log << "Data table missing mapped attribute " <<
                      cNameMap[j] << endl; 
                ierr += 1;
                indMap[j] = -1;
            }
            else
                indMap[j] = SchemaMap::GetTableColumnIndex(t->GetColumnNames(),
                  cNameMap[j], t->GetColCaseSense());
        }

        if (ierr)
        {
            continue;
        }

        if (_verbose)
        {
            _log << "Found "  << nColsMap << " attributes in schema map" <<
              endl;
            for (unsigned int j = 0; j < nColsMap; ++j)
            {
	        _log << "Map entry " << j << " is schema attribute " <<
                  indMap[j] << " " << cNameMap[j] <<
                  " mapped to "  << iNameMap[j] << " " <<
                  cIdMap[j] << " " << fIdMap[j] << endl;
            }
        }

        //
        // Temporary space for extracted data isomorphorous with the table t ...
        //   
        //
        vector<vector<string> > dMap;

        vector<string> tmpVector;
        for (unsigned int j = 0; j < nColsMap; ++j)
        {
            dMap.push_back(tmpVector);
        }

        bool iUpdate = false;

        // For every mapped attribute
        for (unsigned int j = 0; j < nColsMap; ++j)
        {
            if (_verbose)
                _log << endl << "*" << endl << "Mapped attribute " << j <<
                  " is " << iNameMap[j] << " condition " <<
                  cIdMap[j] << " function  " << fIdMap[j] <<
                  " schema attribute index " << indMap[j] << endl;

            string tableName;
            CifString::GetCategoryFromCifItem(tableName, iNameMap[j]);

            ISTable* t = rBlock.GetTablePtr(tableName);
#ifdef VLAD_DEBUG_ATOM_SITE
            if (t != NULL)
              cout << "From CIF file: Table \"" << t->GetName() << "\" has " <<
                (t->GetColumnNames()).size() << " columns." << endl;
#endif

            bool updated = _Search(dMap, j, t, rBlock.GetName(), cNameMap,
              iNameMap[j], cIdMap[j], fIdMap[j]);

            if (updated)
                iUpdate = true;

            // We cannot delete the table, since another source attribute
            // for a mapped attribute may be in that table
            // rBlock.DeleteTable(tableName);
        }

        if (!iUpdate || dMap[0].empty() || dMap[1].empty())
        {
            continue;
        }

        // Determine maximum length of non-empty mapped attribute columns
        unsigned int maxLen = dMap[0].size();

        for (unsigned int j = 0; j < nColsMap; ++j)
        {
            if (dMap[j].empty())
                continue;

            if (dMap[j].size() > maxLen)
                maxLen = dMap[j].size();
        }

        // Extend the size of non-empty mapped attribute columns, which are
        // shorter than the maximum size.
        for (unsigned int j = 0; j < nColsMap; ++j)
        {
            if (!aI[indMap[j]].iIndex)
                continue;  // hack... if we are not an index.

            if (!dMap[j].empty() && (dMap[j].size() < maxLen))
            {
                if (_verbose)
                    _log << " Extending  map column " << j <<
                      " with " << dMap[j][0]  << endl;

                for (unsigned int k = dMap[j].size(); k < maxLen; ++k)
                { 
                    dMap[j].push_back(dMap[j][0]);
                }
            }
        }


        // Check consistency. Minimum and maximum length must be the same.

        unsigned int minLen = dMap[0].size();
        maxLen = dMap[0].size();

        for (unsigned int j = 0; j < nColsMap; ++j)
        {
            if (dMap[j].empty())
                continue;

            if (dMap[j].size() < minLen)
                minLen = dMap[j].size();

            if (dMap[j].size() > maxLen)
                maxLen = dMap[j].size();
        }

        if (minLen != maxLen)
        {
            if (_verbose)
            {
                _log << "Conflict in length min = " << minLen <<
                  " max = " << maxLen << endl;
                _log << "Skipping table update update" << endl;
            }

            cerr << "In " << _INPUT_FILE << ": " <<
              "Skipping update for table " << tList[i] <<
              " with inconsistent column lengths." << endl;

            continue;
        }
      
        //
        //  Mapping OK, Update table ... 
        //
        if (_verbose)
            _log << " Table length = " << minLen << endl;

#ifndef DEBUG_POINT
    if (t->GetName() == "citation")
    {
        int a = 1;
        int b = a + 3;
        ++b;
    }
#endif

        // Here minLen and maxLen are the same and represent the number
        // of rows.
        for (unsigned int j = 0; j < minLen; ++j)
        {
            bool iskip = false;

            for (unsigned int k = 0; k < nColsMap; ++k)
            {
                if ((dMap[k].empty() || dMap[k][j].empty()) &&
                  (aI[indMap[k]].iIndex || aI[indMap[k]].iNull))
                {
                    iskip = true;

                    if (_verbose)
                       _log << "Skipping row with NULL value in key "\
                         "attribute column " << indMap[k] << endl;

                    cerr  << "In " << _INPUT_FILE << ": "  <<
                      "Skipping row in " << tList[i] <<
                      " with NULL value in key column " << indMap[k] <<
                      endl;

                    break;
                }
            }

            if (iskip)
                continue; 
	    
            if (_verbose)
            {
                unsigned int iRow = t->GetNumRows();
                _log << "Updating " << tList[i] << " row " <<
                  iRow << endl;
            }

            vector<string> row(nCols, string());

	    for (unsigned int k = 0; k < nColsMap; ++k)
            {
	        if (dMap[k].empty())
                    continue;

	        if (_verbose)
                    _log << "Updating attribute cell " << indMap[k] <<
                      " value " << dMap[k][j] << endl;

                // Cell must not be used if it is located in "ndb_id" column
                // and its value starts with "RCSB", because that indicates
                // a condition where RCSB id is stored in "ndb_id" column and
                // is to be ignored. In all other situations the cell must be
                // used.
                if ((cNameMap[k] != "ndb_id") || (dMap[k][j].compare(0, 4,
                  "RCSB", 0, 4) != 0))
                {
                    // Use the cell.
	            row[indMap[k]] = dMap[k][j];
                }
	    }

	    t->AddRow(row);
        }

#ifdef VLAD_CANNOT_BE_DONE
        What can happen is that one CIF file has the item specified and the
        other one does not have. Then the generated bcp files would not have
        the same number of data items and loading will fail. That is why,
        we need to treat the items that are in the schema maping file but
        not in the CIF file as unknown and put ",," where no space between
        value delimiters, i.e., commas, denotes missing value. That is why
        the logic of _LoadBlock is done to loop over tables/columns in the
        schema mapping file and not over columns defined in tables of CIF
        data file.

        if ((t->GetName() == "rcsb_tableinfo") ||
          (t->GetName() == "rcsb_columninfo"))
            continue;

        // Remove columns that are not in the read file.
        ISTable* readIsTableP = rBlock.GetTablePtr(t->GetName());
        if (readIsTableP == NULL)
            continue;

        const vector<string>& columnsNames = t->GetColumnNames();

        for (unsigned int colI = 0; colI < columnsNames.size(); ++colI)
        {
            if (columnsNames[colI] == "Structure_ID")
                continue;

            if (!readIsTableP->IsColumnPresent(columnsNames[colI]))
                t->DeleteColumn(columnsNames[colI]);
        }
#endif // VLAD_CANNOT_BE_DONE

        wBlock.WriteTable(t);
    }

}


void DbLoader::_DoFunc(vector<string>& s, const vector<string>& r,
  const string& sFnct)
{

    // VLAD - This method changes (if appropriate) every element of vector "r"
    // according to the function sFnct and stores each element in vector "s"

    s.clear();

    string cs;
    int i,iOK;

    if (sFnct.empty() || (sFnct == CifString::UnknownValue) ||
      (sFnct == "__missing()"))
    {
        // Empty string, unknown or internal missing attribute handler.
        if (r.empty())
        {
            s.push_back("");
        }
        else
        {
            s = r;
        }
    }
    else if (sFnct == "today()")
    {
        string dateAndTime;
	_dbOutput._db.GetDateAndTime(dateAndTime);

        if (r.empty())
        {
            s.push_back(dateAndTime);
        }
        else
        {
            for (unsigned int i = 0; i < r.size(); ++i)
            {
                s.push_back(dateAndTime);
            }
        }
    }
    else if (sFnct == "prefix(PDB)")
    {
        if (r.empty())
        {
            s.push_back("");
        }
        else
        {
            for (int i=0; i < (int) r.size(); i++)
            {
                cs = "PDB" + r[i];
                s.push_back(cs);
            }
        }
    }
    else if (sFnct == "prefixUp(PDB)")
    {
        if (r.empty())
        {
            s.push_back("");
        }
        else
        {
            for (int i=0; i < (int) r.size(); i++)
            {
                cs = "PDB" + r[i];
                _ToUpperString(cs);
                s.push_back(cs);
            }
        }
    }
    else if (sFnct == "date()")
    {
        if (r.empty())
        {
            s.push_back("");
        }
        else
        {
            string dbDate;
            for (unsigned int i = 0; i < r.size(); ++i)
            {
	        _dbOutput._db.ConvertDate(dbDate, r[i]);
	        s.push_back(dbDate);
            }
        }
    }
    else if (sFnct == "timestamp()")
    {
        if (r.empty())
        {
            s.push_back("");
        }
        else
        {
            string dbTimestamp;
            for (unsigned int i = 0; i < r.size(); ++i)
            {
	        _dbOutput._db.ConvertTimestamp(dbTimestamp, r[i]);
	        s.push_back(dbTimestamp);
            }
        }
    }
    else if (sFnct == "switchname(comma)")
    {
        if (r.empty())
        {
            s.push_back("");
        }
        else
        {
            int ilen = 0;
            for (unsigned int i = 0; i < r.size(); i++)
            {
                ilen += r[i].size() + 3;
            }

            char *buf = new char[ilen + 1];
            memset(buf, 0, ilen + 1);

            char* p = new char[ilen + 1];
            memset(p, 0, ilen + 1);

            for (unsigned int i = 0; i < r.size(); i++)
            {
                strcpy(p, r[i].c_str());
                name_conversion_first_last(p);
                strcat(buf, p);
                if (i < r.size() - 1)
                    strcat(buf, ", ");
            }
            string cs = buf;
            delete [] buf;
            delete [] p;
            s.push_back(cs);
        }
    }
    else if (sFnct == "tointegerplus()")
    {
        if (r.empty())
        {
            s.push_back("");
        }
        else
        {
            for (unsigned int i = 0; i < r.size(); i++)
            {
                if (String::IsCiEqual(r[i], "primary"))
                {
                    s.push_back(String::IntToString(1));
                }
                else if (String::IsNumber(r[i]))
                {
                    int number = String::StringToInt(r[i]);
                    s.push_back(String::IntToString(number + 1));
                }
                else
                {
                    // Default to empty string
                    s.push_back("");
                }
            }
        }
    }
#ifdef VLAD_DATE_OBSOLETE
    else if (sFnct == "datecnv(sybase)")
    {
        if (r.empty())
        {
            s.push_back("");
        }
        else
        {
            char sybdate[20];
            for (int i=0; i < (int) r.size(); i++)
            {
	        _dformat_3(r[i].c_str(), sybdate, 0);
	        s.push_back(sybdate);
            }
        }
    }
    else if (sFnct == "datecnv(sybase-short)")
    {
        if (r.empty())
        {
            s.push_back("");
        }
        else
        {
            char sybdate[20];
            for (int i=0; i < (int) r.size(); i++)
            {
	        _dformat_3(r[i].c_str(),sybdate, 1);
	        s.push_back(sybdate);
            }
        }
    }
    else if (sFnct == "datecnv(oracle-short)")
    {
        if (r.empty())
        {
            s.push_back("");
        }
        else
        {
            char sybdate[20];
            for (int i=0; i < (int) r.size(); i++)
            {
	        _dformat_5(r[i].c_str(),sybdate);
	        s.push_back(sybdate);
            }
        }
    }
#endif // VLAD_DATE_OBSOLETE
    else if (sFnct == "row()")
    {
        if (r.empty())
        {
            s.push_back("");
        }
        else
        {
            for (int i=1; i <= (int) r.size(); i++)
            {
	        cs = String::IntToString(i);
	        s.push_back(cs);
            }
        }
    }
    else if (sFnct == "toupper()")
    {
        if (r.empty())
        {
            s.push_back("");
        }
        else
        {
            for (int i=0; i < (int) r.size(); i++)
            {
	        cs = r[i];
	        _ToUpperString(cs);
	        s.push_back(cs);
            }
        }
    }
    else if ((sFnct == "strip(ws)") || (sFnct == "strip()"))
    {
        if (r.empty())
        {
            s.push_back("");
        }
        else
        {
            for (int i=0; i < (int) r.size(); i++)
            {
	        cs = r[i];
	        _StripString(cs,1);
	        s.push_back(cs);
            }
        }
    }
    else if (sFnct == "strip(nl)")
    {
        if (r.empty())
        {
            s.push_back("");
        }
        else
        {
            for (int i=0; i < (int) r.size(); i++)
            {
	        cs = r[i];
               _StripString(cs,2);
	       s.push_back(cs);
            }
        }
    }
    else if (sFnct == "collapse(comma)")
    {
        if (r.empty())
        {
            s.push_back("");
        }
        else
        {
            int ilen  = 0;
            for (i=0; i < (int) r.size(); i++)
            {
	        ilen += r[i].size() + 3; 
            }
            char *buf = (char *) calloc(ilen+1, sizeof(char));
            strcpy(buf,"");
            for (i=0; i < (int) r.size(); i++)
            {
	        strcat(buf, r[i].c_str());
	        if (i < (int) r.size()-1)
                    strcat(buf,", ");
            }
            cs.clear(); cs = buf;
            free(buf);
      
            s.push_back(cs);
        }
    }
    else if (sFnct == "collapse(comma,cnvname)")
    {
        if (r.empty())
        {
            s.push_back("");
        }
        else
        {
            int ilen  = 0;
            for (i=0; i < (int) r.size(); i++)
            {
	        ilen += r[i].size() + 2; 
            }
            char *buf = (char *) calloc(ilen+1, sizeof(char));
            strcpy(buf,"");
            for (i=0; i < (int) r.size(); i++)
            {
                string reorderedName;
        	_ReorderName(reorderedName, (char*)(r[i].c_str()),1);
        	strcat(buf, reorderedName.c_str());
        	if (i < (int) r.size()-1) strcat(buf,", ");
            }
            cs.clear(); cs = buf;
            free(buf);
      
            s.push_back(cs);
        }
    }
    else if (sFnct == "collapse(space)")
    {
        if (r.empty())
        {
            s.push_back("");
        }
        else
        {
            int ilen  = 0;
            for (i=0; i < (int) r.size(); i++)
            {
	        ilen += r[i].size() + 2; 
            }
            char *buf = (char *) calloc(ilen+1, sizeof(char));
           strcpy(buf,"");
           for (i=0; i < (int) r.size(); i++)
           {
	       strcat(buf, r[i].c_str());
	       strcat(buf," ");
           }
           cs.clear(); cs = buf;
           free(buf);      

           s.push_back(cs);
        }
    }
    else if (sFnct == "collapse()")
    {
        if (r.empty())
        {
            s.push_back("");
        }
        else
        {
            int ilen  = 0;
            for (i=0; i < (int) r.size(); i++)
            {
	        ilen += r[i].size(); 
            }
            char *buf = (char *) calloc(ilen+1, sizeof(char));
            strcpy(buf,"");
            for (i=0; i < (int) r.size(); i++)
            {
	        strcat(buf, r[i].c_str());
            }
            cs.clear(); cs = buf;
            free(buf);      

            s.push_back(cs);
        }
    }
    else if (sFnct == "count()")
    {
        if (r.empty())
        {
            s.push_back("0");
        }
        else
        {
            cs = String::IntToString((int)r.size());
            s.push_back(cs);
        }
    }
    else if (sFnct == "ifany()")
    {
        // one or more non-null values ...
        if (r.empty())
        {
            s.push_back("N");
        }
        else
        {
            iOK=0;
            for (i=0; i < (int) r.size(); i++)
            {
	        if (!CifString::IsEmptyValue(r[i]))
                    iOK++;
            }
            if (iOK) 
	        s.push_back("Y");
            else
                s.push_back("N");
        }
    }
    else if (sFnct == "tobool()")
    {
        // convert y/yes -> 1  anyother,NULL,missing -> 0
        if (r.empty())
        {
            s.push_back("0");
        }
        else
        {
            for (i=0; i < (int) r.size(); i++)
            {
	        if (!CifString::IsEmptyValue(r[i]))
                {
	            if (String::IsCiEqual(r[i], "Y") ||
                      String::IsCiEqual(r[i], "yes")) 
	                s.push_back("1");
	            else
	                s.push_back("0");
	        }
                else
                {
                    //  NULLS map to 0
                    s.push_back("0");
	        }
            }
        }
    }
    else if (sFnct.compare(0, 9, "ifexists(", 9) == 0)
    {
        // one or more non-null values ...
        if (r.empty())
        {
            s.push_back("N");
        }
        else
        {
            iOK=0;
            char* p = new char[strlen(sFnct.c_str()) + 1];
            strcpy(p,&sFnct.c_str()[9]);
            p[strlen(p)-1] = '\0';
            cs.clear(); cs = p;

            for (i=0; i < (int) r.size(); i++)
            {
	        if (!CifString::IsEmptyValue(r[i]))
                {
                    iOK++;
	            s.push_back(cs);
	        }
                else
                {
	            s.push_back("");	  
	        }
            }
            if (p) delete p;
        }
    }
    else if (sFnct == "first()")
    {
        // first() of possibly multiple values
        if (r.empty())
        {
            s.push_back("");
        }
        else
        {
            s.push_back(r[0]);
        }
    }
    else if (sFnct == "last()")
    {
        // last() of possibly multiple values
        if (r.empty())
        {
            s.push_back("");
        }
        else
        {
	  s.push_back(r[r.size()-1]);
        }
    }

    else
    {
        if (r.empty())
        {
            s.push_back("");
        }
        else
        {
            s = r;
        }
    }

}


int DbLoader::_GetMapColumnIndex(const vector<string>& cNameMap,
  const string& vOf) 
{
  char *p=NULL;
  string cs1;
  int j, jCol=-1;

  if ( !strncmp(vOf.c_str(),"valueof(",8) || !strncmp(vOf.c_str(),"unseqof(",8) ) {
    p = new char[strlen(vOf.c_str()) + 1];
    strcpy(p,&(vOf.c_str()[8]));
    p[strlen(p)-1] = '\0';
    cs1.clear(); cs1 = p;
    if (p) delete p;
    if (_verbose) _log << "Map join attribute is " << cs1 << endl;
    jCol = -1;
    for (j=0; j < (int) cNameMap.size(); j++) {
      if (String::IsCiEqual(cs1, cNameMap[j])) {
	jCol = j; 
	break;
      }
    }
    if (jCol < 0) {
      if (_verbose) _log << "Attribute " << cs1 << " is not in map" << endl;
    }
  }
  return(jCol);
}


void DbLoader::_GetMapColumnValue(string& p, vector<string>& dMapVec,
  unsigned int irow)
{
  p.clear();

  int nRows = 0, len = 0;

  if (irow >= dMapVec.size()) return;
  nRows= dMapVec.size();  

  len = dMapVec[irow].size();
  if (len > 0) {
    p = dMapVec[irow];
  } 

}


void DbLoader::CreateTables(CifFile& writeCifFile, CifFile& readCifFile)
{
    writeCifFile.AddBlock(_blockName);

    Block& outBlock = writeCifFile.GetBlock(_blockName);

    Block& inBlock = readCifFile.GetBlock(readCifFile.GetFirstBlockName());

    // Get table names in the read CIF file
    vector<string> tablesNames;
    inBlock.GetTableNames(tablesNames);

    for (unsigned int tableI = 0; tableI < tablesNames.size(); ++tableI)
    {
        // Create the table
        ISTable* isTableP = new ISTable(tablesNames[tableI]);

        // Get column names for the table
        ISTable* readIsTableP = inBlock.GetTablePtr(tablesNames[tableI]);

        const vector<string>& columnsNames = readIsTableP->GetColumnNames();

        for (unsigned int colI = 0; colI < columnsNames.size(); ++colI)
        {
            // Add columns
            isTableP->AddColumn(columnsNames[colI]);
        }

        outBlock.WriteTable(isTableP);
    }

    // Create special tables rcsb_tableinfo, rcsb_columninfo
    // Crate their attributes

    ISTable* isTableP = new ISTable("rcsb_tableinfo");
    vector<string> attribNames;
    _schemaMapping.GetAttributeNames(attribNames, "rcsb_tableinfo");
 
    for (unsigned int colI = 0; colI < attribNames.size(); ++colI)
    {
        // Add columns
        isTableP->AddColumn(attribNames[colI]);
    }

    outBlock.WriteTable(isTableP);

    isTableP = new ISTable("rcsb_columninfo");
    _schemaMapping.GetAttributeNames(attribNames, "rcsb_columninfo");
 
    for (unsigned int colI = 0; colI < attribNames.size(); ++colI)
    {
        // Add columns
        isTableP->AddColumn(attribNames[colI]);
    }

    outBlock.WriteTable(isTableP);
}


bool DbLoader::_Search(
  vector<vector<string> >& dMap,  // container for output
  const unsigned int iAttrib,     // index between map and schema
  ISTable* isTableP,              // source table
  const string& blockName,        // block name 
  const vector<string>& cNameMap, // mapped attributes 
  const string& sItem,            // source item
  const string& sCnd,
  const string& sFnct             // condition and function code
)
{

    if (blockName.empty())
        return(false);

    //
    // jdw catch the special cases of a constant non-schema mapping
    //

    vector<string> r;

    if (sFnct == "today()")
    {
        if (_verbose)
            _log << "Constant function today()" << endl;

        _DoFunc(dMap[iAttrib], r, sFnct);
        if (_verbose)
        {
            _log << "Returning result length " << dMap[iAttrib].size() << endl;
            for (unsigned int i = 0; i < dMap[iAttrib].size(); ++i)
            {
	        _log << "Returning value " << dMap[iAttrib][i] << endl;
            }
        }
        return(true);
    }
    else if (sFnct == "datablockid()")
    {
        if (_verbose)
            _log << "Constant function datablockid()" << endl;
        vector<string> s;
        s.push_back(blockName);
        dMap[iAttrib] = s;

        if (_verbose)
        {
            _log << "Returning result length " << dMap[iAttrib].size() << endl;
            for (unsigned int i = 0; i < dMap[iAttrib].size(); ++i)
            {
	        _log << "Returning value " << dMap[iAttrib][i] << endl;
            }
        }
        return(true);
    }
#ifdef DB_HASH_ID
    else if (sFnct == "unseq()")
    {
        // unseq() unique sequence using pdb hash code

        long long hashId;
        hashId = pdbIdHash(_HASH_ID);

        if (_verbose)
        {
            _log << "Function seqid()" << endl;
            _log << "Block id "<< blockName << endl;
            _log << "Hash id "<< (long)hashId  << endl;
        }

        vector<string> s;
        unsingned int maxLen = 0;
        for (i=0; i < iAttrib; i++)
        {
            if (dMap[i].empty())
                continue;
            j=dMap[i].size();
            if (j > maxLen)
                maxLen = j;
        }

        if (maxLen>0)
        {
            for (i=1; i <= maxLen; i++)
            {
                csTarget = String::IntToString((long long) hashId +
                  (long long) i);
	        if (_verbose)
                    _log << "hashid csTarget " << csTarget << endl;
                s.push_back(csTarget);
            }
        }
        else
        {
            s.push_back("");
        }
        dMap[iAttrib]=s;
        return(true);
    }
#endif
    else if (sFnct == "row()")
    {
        if (_verbose) _log << "Function row()" << endl;
        vector<string> s;
        unsigned int maxLen = 0;
        for (unsigned int i = 0; i < (unsigned int) iAttrib; ++i)
        {
            if (dMap[i].empty())
                continue;
            unsigned int j = dMap[i].size();
            if (j > maxLen)
                maxLen = j;
        }

        if (maxLen>0)
        {
            for (unsigned int i = 1; i <= maxLen; i++)
            {
                string csTarget = String::IntToString((int) i);
                s.push_back(csTarget);
            }
        }
        else
        {
            s.push_back("");
        }
        dMap[iAttrib]=s;
        return(true);
    }
#ifdef VLAD_HASH_ID_DEL
    else
    {
        return(false);
    }
#endif

    string columnName;
    string tableName;
    CifString::GetItemFromCifItem(columnName, sItem);
    CifString::GetCategoryFromCifItem(tableName, sItem);
    if (sItem.empty() || columnName.empty() || tableName.empty())
        return(false);


    if (_verbose)
        _log << "Searching block " << blockName << " table " <<
          tableName << " column " << columnName << endl;

    if (isTableP == NULL)
    {
        if (_verbose)
            _log << "Target table " << tableName << " not in " <<
              blockName << endl;

        return(false);
    }

#ifndef DEBUG_POINT
    if (isTableP->GetName() == "citation_author")
    {
        int a = 1;
        int b = a + 3;
        ++b;
    }
#endif

    if (!isTableP->IsColumnPresent(columnName))
    {
        if (_verbose)
            _log << "Target columnName " << columnName << " not in " <<
              tableName << endl; 
        vector<string> s;
        for (unsigned int i = 0; i < isTableP->GetNumRows(); ++i)
        {
            s.push_back(CifString::UnknownValue);
        }
        dMap[iAttrib] = s;
        if (_verbose)
        {
            _log << "Returning result length " << dMap[iAttrib].size() << endl;
            for (unsigned int i = 0; i < dMap[iAttrib].size(); ++i)
            {
	        _log << "Returning value " << dMap[iAttrib][i] << endl;
            }
        }
        return(true);
    }

    if (!sCnd.empty())
    {
        // fetch condition 

        if (_verbose)
            _log << "Using search condition " << sCnd << endl;

        vector<vector<string> > mappedConditions; 
        _schemaMapping.GetMappedConditions(mappedConditions, sCnd);

        if (!mappedConditions.empty())
        {
            // list of constraints for condition.

            vector<string> cndCol;
            cndCol = mappedConditions[0];

            vector<string> cndVal;
            cndVal = mappedConditions[1];

            unsigned int nEqCnd=0;
            for (unsigned int i = 0; i < mappedConditions[0].size(); ++i)
            {
                if ( !strcmp( cndVal[i].c_str(),"datablockid()"))
                {
	            cndVal[i].clear(); cndVal[i] = blockName;
	        }

	        if ( !strncmp( cndVal[i].c_str(),"valueof(",8) ||
                  !strncmp( cndVal[i].c_str(),"unseqof(",8) )
                {
	            nEqCnd++;
	        }

	        if (_verbose)
                    _log << "Condition column " << cndCol[i] <<
                      " value " << cndVal[i] << endl;
            }

            if (nEqCnd)
            {
                // number of join conditions.

                vector<int> indDMap(mappedConditions[0].size());
                vector<int> lenDMap(mappedConditions[0].size());
                int lenMin = -1;
                int lenMax = -1;

                for (unsigned int i = 0; i < indDMap.size(); ++i)
                {
	            indDMap[i] = -1;
	            lenDMap[i] =  0;
	            if (!strcmp(cndVal[i].c_str(), "datablockid()"))
                    {
                        cndVal[i] = blockName;
	            }

	            if (!strncmp(cndVal[i].c_str(), "valueof(", 8) ||
                      !strncmp(cndVal[i].c_str(), "unseqof(" ,8))
                    {
	                indDMap[i] = _GetMapColumnIndex(cNameMap, cndVal[i]);

                        if (indDMap[i] >= 0)
	                    lenDMap[i] = dMap[indDMap[i]].size();
                        else
                            lenDMap[i] = 0;

	                if (lenMin < 0 || lenDMap[i] < lenMin)
                            lenMin  = lenDMap[i];
	                if (lenDMap[i] > lenMax)
                            lenMax  = lenDMap[i];
	                if (_verbose)
                            _log << " ** value or unseqof() index " <<
                              indDMap[i] << " length " << lenDMap[i] << endl;
	            }
	        }
	        if (lenMin != lenMax)
                {
	            if (_verbose)
                        _log << " ** valueof() or unseqof() column length "\
                          "inconsistency  lenMin " <<  lenMin <<
                          " lenMax " << lenMax << endl;
	        }
	        if (_verbose)
                    _log << " ** valueof() or unseqof() column lenMin " <<
                      lenMin << " lenMax " << lenMax << endl;

                cndCol.clear();
                cndVal.clear();

                vector<string> tRes;

	        for (unsigned int j = 0; (int)j < lenMin; ++j)
                {
                    cndCol = mappedConditions[0];
                    cndVal = mappedConditions[1];
                    if (_verbose)
                        _log << " ** Starting row "<< j <<
                          " condition length " << cndCol.size() << 
                          endl;

                    unsigned int iFlagValueOf = 0;
	            for (unsigned int i = 0; i < cndCol.size(); ++i)
                    {
	                if (_verbose) _log << " ** condition value " << i <<
                          " is " << cndVal[i] << endl;
	    
	                if (!strcmp(cndVal[i].c_str(), "datablockid()"))
                        {
                            cndVal[i] = blockName;
	                }
	    
	                if ( !strncmp( cndVal[i].c_str(),"valueof(",8) ||
                          !strncmp( cndVal[i].c_str(),"unseqof(",8) )
                        {
                            iFlagValueOf = 1;
	                    if (!strncmp( cndVal[i].c_str(),"unseqof(",8))
                                iFlagValueOf = 2; 

                            string p;
                            if (indDMap[i] >= 0)
	                        _GetMapColumnValue(p, dMap[indDMap[i]], j);

	                    if (p.empty())
                            {
		                cndVal[i].clear(); 
		                cndVal[i] = "NULL";
		                if (_verbose)
                                    _log << " ** Map row " <<  j <<
                                      " has value NULL" << endl;
	                    }
                            else
                            {
		                cndVal[i].clear(); 
		                cndVal[i] = p;
		                if (_verbose)
                                    _log << " ** Map row " <<  j <<
                                      " has value " << p << endl;
	                    }
                        } 
	                if (_verbose)
                            _log << " ** Search column " <<
                              cndCol[i] << " for " <<
                              cndVal[i] << endl;
	            }

                    vector<unsigned int> is;
	            isTableP->Search(is, cndVal, cndCol);

	            if (!is.empty())
                    {
	                if (iFlagValueOf == 1)
                        {
                            // valueof()
	                    if (_verbose)
                                _log << " ** Search result length is " <<
                                  is.size() << " rows"<< endl;
	                    isTableP->GetColumn(r, columnName,is);
	                }
#ifdef DB_HASH_ID
                        else if (iFlagValueOf == 2)
                        {
                            // unseqof()
	                    if (_verbose)
                                _log << " ** Search result length is " <<
                                  is.size() << " rows"<< endl;
                            r.clear();
                            long long hashId;
	                    hashId = pdbIdHash(_HASH_ID);
	                    for (int jj = 0; jj < (int) is.size(); jj++)
                            {
                                cs = String::IntToString((long long) (is[jj] +
                                  1 + hashId));
                                r.push_back(cs);
                            }
	                }
#endif
	            }
                    else
                    {
	                if (_verbose)
                        {
	                    _log << " ** ** ** ** ** Warning " << endl;
	                   if (is.empty())
                               _log << " ** Search returns 0 length" << endl;
	                }
	                r.clear();
	            }

                    vector<string> r1;
	            _DoFunc(r1, r, sFnct);
	            tRes.push_back(r1[0]);
	            r1.clear();

                    cndVal.clear();
                    cndCol.clear();
                    r.clear();
	        } // end j loop	
                // copy expand the resulting column
	        _DoFunc(dMap[iAttrib], tRes, CifString::UnknownValue);
            }
            else
            {
                // If no join conditions nEqCnd ...
                vector<unsigned int> is;
                isTableP->Search(is, cndVal, cndCol);
                if (!is.empty())
                {
                    isTableP->GetColumn(r, columnName, is);
                }
	        _DoFunc(dMap[iAttrib], r, sFnct);
            }
        }
        else
        {  // search condition missing in category
            isTableP->GetColumn(r, columnName);
            _DoFunc(dMap[iAttrib], r, sFnct);
        }
    }
    else
    { // no condition ...
        if (_verbose)
            _log << "No search condition specified, selecting column " <<
                columnName << endl;
        isTableP->GetColumn(r, columnName);
        if (_verbose && r.empty())
        {
            _log << "Column "<< columnName << " returns NULL result." << endl;
        }
        else if (_verbose && !r.empty())
        {
            _log << "Column "<< columnName << " returns length " <<
              r.size() << endl;
        }

        _DoFunc(dMap[iAttrib], r, sFnct);
    }

    if (_verbose)
    {
        if (!dMap[iAttrib].empty())
        {
            _log << "Returning result length " << dMap[iAttrib].size() << endl;
            for (unsigned int i = 0; i < dMap[iAttrib].size(); ++i)
            {
	        _log << "Returning " << columnName << " value " <<
                  dMap[iAttrib][i] << endl;
            }
        }
        else
        {
            _log << "Returning result length 0 " << endl;
        }
    }

    r.clear();

    return (true);

}



void SqlOutput::WriteAuxTables(ostream& io, ISTable* infoP,
  const vector<string>& tableNames)
{
    string auxTableNameDb;
    _db._schemaMapping.GetTableNameAbbrev(auxTableNameDb, infoP->GetName());

    for (unsigned int i = 0; i < tableNames.size(); ++i)
    {
        string tableNameDb;

        _db._schemaMapping.GetTableNameAbbrev(tableNameDb, tableNames[i]);

        _db.WriteDeleteTable(io, auxTableNameDb, "tablename", tableNameDb);

        io << endl;
    }

    const vector<string>& columnNames = infoP->GetColumnNames();

    // Get all of the attributes for this table.
    const vector<AttrInfo>& aI =
      _db._schemaMapping.GetTableAttributeInfo(infoP->GetName(),
      columnNames, infoP->GetColCaseSense());

    vector<eTypeCode> typeCodes;
    vector<unsigned int> widths;

    // typeCode, width, maxWidth
    for (unsigned int j = 0; j < columnNames.size(); ++j)
    {
        typeCodes.push_back(aI[j].iTypeCode);
        widths.push_back(aI[j].iWidth);
    }

    _WriteTable(io, infoP, widths, false, typeCodes);
}


void SqlOutput::WriteSchema(const string& workDir)
{

    string oFile = workDir + _SCHEMA_LOADING_SCRIPT;
    ofstream ioc(oFile.c_str(), ios::out | ios::trunc);

    WriteSqlScriptSchemaInfo(ioc);

    ioc.close();

    oFile = workDir + _SCHEMA_FILE;
    ofstream io(oFile.c_str(),ios::out|ios::trunc);

    _db.WriteSchemaStart(io);
    io << endl << endl;

    oFile = workDir + _SCHEMA_DELETE_FILE;
    ofstream iod(oFile.c_str(), ios::out | ios::trunc);

    string start;
    _db.GetStart(start);
    iod << start << endl << endl;

    //
    // Generate SQL to create table schema...
    //
    vector<string> tableNames;
    _db._schemaMapping.GetAllTablesNames(tableNames);
    for (unsigned int i = 0; i < tableNames.size(); ++i)
    {
        if (tableNames[i].empty())
            continue;

        if (_db.GetUseOnlyPopulated() &&
          (!_db._schemaMapping.IsTablePopulated(tableNames[i])))
        {
            continue;
        } 

        string tableNameDb;
        _db._schemaMapping.GetTableNameAbbrev(tableNameDb, tableNames[i]);

        _db.DropTableSql(iod, tableNameDb);
        iod << endl;

        CreateTableSql(io, tableNames[i]);
        io << endl << endl;
    }

    iod.close();

    //
    // Create tableinfo and columninfo tables which contain data about schema.
    //
    ISTable* tinfo = _db._schemaMapping.CreateTableInfo();

    WriteAuxTables(io, tinfo, tableNames);

    io << endl;

    if (tinfo != NULL)
        delete tinfo;

    ISTable* cinfo = _db._schemaMapping.CreateColumnInfo();

    WriteAuxTables(io, cinfo, tableNames);

    if (cinfo != NULL)
        delete cinfo;

    io.close();

}


void DbOutput::SetInputFile(const string& inpFile)
{
    _INPUT_FILE = inpFile;
}



void DbOutput::GetMasterIndexAttribValue(string& masterIndexAttribValue,
  Block& block, const string& masterIndexAttribName,
  const vector<string>& tableNames)
{

    for (unsigned int i = 0; i < tableNames.size(); ++i)
    {
        if (tableNames[i].empty())
        {
            continue;
        }

        ISTable* t = block.GetTablePtr(tableNames[i]);
        if ((t != NULL) && (t->GetNumRows() > 0))
        {
            // const vector<string>& row = tIn->GetRow(0);

            // dbIndexValue = row[0];

            masterIndexAttribValue = (*t)(0, masterIndexAttribName);
            break;
        }
    }
}

void BcpOutput::WriteData(Block& block, const string& workDir)
{

    // BCP Update for ...

    string oFile = workDir + _DATA_DELETE_FILE;

    ofstream iosql;
    if (_db.GetAppendFlag())
    {
        iosql.open(oFile.c_str(), ios::out | ios::app);
    }
    else
    {
        iosql.open(oFile.c_str(), ios::out | ios::trunc);
    }

    string start;
    _db.GetStart(start);

    iosql << start << endl << endl;

    vector<string> tableNames;
    _db._schemaMapping.GetDataTablesNames(tableNames);

    string masterIndexAttribName;
    _db._schemaMapping.GetMasterIndexAttribName(masterIndexAttribName);

    string masterIndexAttribValue;
    GetMasterIndexAttribValue(masterIndexAttribValue, block,
      masterIndexAttribName, tableNames);

    for (unsigned int i = 0; i < tableNames.size(); ++i)
    {
        if (_db.GetUseOnlyPopulated() &&
          (!_db._schemaMapping.IsTablePopulated(tableNames[i])))
        {
            continue;
        }

        string tableNameDb;
        _db._schemaMapping.GetTableNameAbbrev(tableNameDb, tableNames[i]);

        _db.WriteDeleteTable(iosql, tableNameDb, masterIndexAttribName,
          masterIndexAttribValue);

        iosql << endl;

        ISTable* t = block.GetTablePtr(tableNames[i]);
        if ((t != NULL) && (t->GetNumRows() > 0))
        {
            string tName = workDir + tableNames[i] + ".bcp";

            struct stat statbuf;
            int istat = stat(tName.c_str(), &statbuf);

            while (istat == 0 && (statbuf.st_size > _MAXFILESIZE))
            {
                tName += "+";
                istat = stat(tName.c_str(), &statbuf);
                cerr << "File size for " << tName << " is " << statbuf.st_size 
                  << " istat " << istat << endl;
            }

            ofstream iobcp;
            if (_db.GetAppendFlag())
            {
                iobcp.open(tName.c_str(), ios::out | ios::app);
            }
            else
            {
                iobcp.open(tName.c_str(), ios::out | ios::trunc);
            } 

            const vector<string>& columnNames = t->GetColumnNames();

#ifdef VLAD_DEBUG_ATOM_SITE
            cout << "BcpOutput: Table \"" << t->GetName() << "\" has " <<
              columnNames.size() << " columns." << endl;
#endif

            // Get all of the attributes for this table.
            const vector<AttrInfo>& aI =
              _db._schemaMapping.GetTableAttributeInfo(t->GetName(),
              columnNames, t->GetColCaseSense());

            vector<eTypeCode> typeCodes;
            vector<unsigned int> widths;

            // typeCode, width, maxWidth
            for (unsigned int j = 0; j < columnNames.size(); ++j)
            {
                typeCodes.push_back(aI[j].iTypeCode);
                widths.push_back(aI[j].iWidth);
            }

            _WriteTable(iobcp, t, widths,
              _db._schemaMapping.GetReviseSchemaMode(), typeCodes);

#ifdef VLAD_LOG_SEPARATION
            if (_verbose)
            {
                for (unsigned int i = 0; i < columnNames.size(); ++i)
                {
                    _log  << tIn->GetName() << " " << columnNames[i] << " " <<
                      widths[i] << endl;
                }
            }
#endif

            if (_db._schemaMapping.GetReviseSchemaMode())
            {
                for (unsigned int i = 0; i < columnNames.size(); ++i)
                {
                    _db._schemaMapping.UpdateAttributeDef(t->GetName(),
                      columnNames[i], typeCodes[i], aI[i].iWidth,
                      widths[i]);
                }
            }

            iobcp.close();
        }
    }

    iosql.close();

}


void BcpOutput::WriteDataLoadingScripts(const string& workDir)
{

    WriteDataLoadingScript(workDir);

    WriteDataLoadingFile(workDir);

}


void SqlOutput::WriteDataLoadingScripts(const string& workDir)
{

    WriteDataLoadingScript(workDir);

}


void SqlOutput::WriteDataLoadingScript(const string& workDir)
{

    string oFile = workDir + _DATA_LOADING_SCRIPT;

    ofstream io(oFile.c_str(), ios::out | ios::trunc);

    WriteHeader(io);
    io << endl;

    WriteDbExec(io, _DATA_FILE);

    io << endl;

    io.close();
 
}


void SqlOutput::WriteData(Block& block, const string& workDir)
{

    string oFile = workDir + _DATA_FILE;

    ofstream iosql;
    if (_db.GetAppendFlag())
    {
        iosql.open(oFile.c_str(), ios::out | ios::app);
    }
    else
    {
        iosql.open(oFile.c_str(), ios::out | ios::trunc);
    }

    string start;
    _db.GetStart(start);

    iosql << start << endl << endl;

    vector<string> tableNames;
    _db._schemaMapping.GetDataTablesNames(tableNames);

    string masterIndexAttribName;
    _db._schemaMapping.GetMasterIndexAttribName(masterIndexAttribName);

    string masterIndexAttribValue;
    GetMasterIndexAttribValue(masterIndexAttribValue, block,
      masterIndexAttribName, tableNames);

    for (unsigned int i = 0; i < tableNames.size(); ++i)
    {
        if (_db.GetUseOnlyPopulated() &&
          (!_db._schemaMapping.IsTablePopulated(tableNames[i])))
        {
            continue;
        }

        string tableNameDb;
        _db._schemaMapping.GetTableNameAbbrev(tableNameDb, tableNames[i]);

        _db.WriteDeleteTable(iosql, tableNameDb, masterIndexAttribName,
          masterIndexAttribValue);
        iosql << endl;

        ISTable* t = block.GetTablePtr(tableNames[i]);
        if ((t != NULL) && (t->GetNumRows() > 0))
        {
            const vector<string>& columnNames = t->GetColumnNames();

            // Get all of the attributes for this table.
            const vector<AttrInfo>& aI =
              _db._schemaMapping.GetTableAttributeInfo(t->GetName(),
              columnNames, t->GetColCaseSense());

            vector<eTypeCode> typeCodes;
            vector<unsigned int> widths;

            // typeCode, width, maxWidth
            for (unsigned int j = 0; j < columnNames.size(); ++j)
            {
                typeCodes.push_back(aI[j].iTypeCode);
                widths.push_back(aI[j].iWidth);
            }

#ifdef VLAD_DEBUG_ATOM_SITE
            cout << "Sql Output: Table \"" << t->GetName() << "\" has " << 
              columnNames.size() << " columns." << endl;
#endif

            _WriteTable(iosql, t, widths,
              _db._schemaMapping.GetReviseSchemaMode(), typeCodes);

#ifdef VLAD_LOG_SEPARATION
            if (_verbose)
            {
                for (unsigned int i = 0; i < nCols; ++i)
                {
                    _log  << tIn->GetName() << " " << columnNames[i] << " " <<
                      widths[i] << endl;
                }
            }
#endif

            if (_db._schemaMapping.GetReviseSchemaMode())
            {
                for (unsigned int i = 0; i < columnNames.size(); ++i)
                {
                    _db._schemaMapping.UpdateAttributeDef(t->GetName(),
                      columnNames[i], typeCodes[i], aI[i].iWidth,
                      widths[i]);
                }
            }
        }
        iosql << endl;
    }

    iosql.close();

}


void Db::WriteLoad(ostream& io)
{

    io << "source " << GetDataLoadingFileName() << endl;

}


void DbOutput::_FormatStringDataSql(ostream& io, const string& cs,
  unsigned int maxWidth) 
{

    // A null string or the special CIF characters are treated as NULL. 
    if (CifString::IsEmptyValue(cs))
    {
        io << "NULL";
        return;
    }

    unsigned int len = cs.size();

    if (len > maxWidth)
        len = maxWidth;

    io << "'";

    bool inLeadWhiteSpace = true;

    for (unsigned int i = 0; i < len; ++i)
    {
        if (isspace(cs[i]) && inLeadWhiteSpace)
        {
            // Skip leading white space
            continue;
        }
        else if (isspace(cs[i]))
        {
            // Convert all white space to SPACE
            io << " ";
        }
        else if (cs[i] == '\'')
        {
            // Double-up single quotes
            io << "''";
            inLeadWhiteSpace = false;
        }
        else
        {
            io << cs[i];
            inLeadWhiteSpace = false;
        }
    }

    io << "'";

}


void BcpOutput::WriteDataLoadingScript(const string& workDir)
{

    string oFile = workDir + _DATA_LOADING_SCRIPT;

    ofstream io(oFile.c_str(), ios::out | ios::trunc);

    WriteHeader(io);
    io << endl;

    WriteDelete(io);
    io << endl;

    _db.WriteLoad(io);
    io << endl;

    io.close();

}


void BcpOutput::WriteDataLoadingFile(const string& workDir)
{

    ofstream ioctl;
    ioctl.open(_db.GetDataLoadingFileName().c_str(), ios::out | ios::trunc);

    _db.WriteLoadingStart(ioctl);
    ioctl << endl << endl;

    vector<string> tableNames;
    _db._schemaMapping.GetAllTablesNames(tableNames);

    for (unsigned int i = 0; i < tableNames.size(); ++i)
    {
        _db.WriteLoadingTable(ioctl, tableNames[i], workDir);
    }

    _db.WriteLoadingEnd(ioctl);

    ioctl.close();

}


void BcpOutput::WriteDelete(ostream& io)
{

    io << "if (\"x$1\" == \"xdelete\" && -e " << _DATA_DELETE_FILE <<
       " && ! -z " << _DATA_DELETE_FILE << ") then" << endl;

    WriteDbExecOnly(io, _DATA_DELETE_FILE);

    io << "endif" << endl;

}


#ifdef VLAD_DATE_OBSOLETE
void DbLoader::_dformat_1(const char *date, char *odate) {
  // reformat  dd-mmm-yy  to Mon dd, yyyy

  if (odate !=NULL) strcpy (odate,"");
  if (date == NULL ||  date[2] != '-' || date[6] != '-' || strlen(date) != 9) return;



  strncpy(odate,&date[3],3);
  odate[1] = tolower(odate[1]);
  odate[2] = tolower(odate[2]);

  strcat(odate," ");
  strncat(odate,&date[0],2);
  if (odate[4] == '0') odate[4] = ' ';

  strcat(odate," ");
  if (date[7] == '0' || date[7] == '1' || date[7] == '2' || date[7] == '3' || date[7] == '4' || date[7] == '5') {
    strcat(odate,"20");
    strncat(odate,&date[7],2);
  } else {
    strcat(odate,"19");
    strncat(odate,&date[7],2);
  }
}


void DbLoader::_dformat_2(const char *date, char *odate) {
  //
  // reformat  CIF data = yyyy-mm-dd[:00:00]  to Mon dd, yyyy
  //                      2000-01-27  to Jan 27, 2000  odate[13]
  //

  if (odate != NULL) strcpy (odate,"");
  if (date == NULL ||  date[4] != '-' || date[7] != '-' ) return;

  char month[5];
  memset(month, 0 ,5);
  strncpy(month,&date[5],2);




  if (!strcmp(month,"01"))
    strcpy(odate,"Jan ");
  else   if (!strcmp(month,"02"))
    strcpy(odate,"Feb ");
  else   if (!strcmp(month,"03"))
    strcpy(odate,"Mar ");
  else   if (!strcmp(month,"04"))
    strcpy(odate,"Apr ");
  else   if (!strcmp(month,"05"))
    strcpy(odate,"May ");
  else   if (!strcmp(month,"06"))
    strcpy(odate,"Jun ");
  else   if (!strcmp(month,"07"))
    strcpy(odate,"Jul ");
  else   if (!strcmp(month,"08"))
    strcpy(odate,"Aug ");
  else   if (!strcmp(month,"09"))
    strcpy(odate,"Sep ");
  else   if (!strcmp(month,"10"))
    strcpy(odate,"Oct ");
  else   if (!strcmp(month,"11"))
    strcpy(odate,"Nov ");
  else   if (!strcmp(month,"12"))
    strcpy(odate,"Dec ");


  strncat(odate,&date[8],2);
  if (odate[4] == '0') odate[4] = ' ';

  strcat(odate," ");
  strncat(odate,&date[0],4);

}
#endif // VLAD_DATE_OBSOLETE


void Db::GetDateAndTime(string& dateAndTime)
{
    time_t currTime;
    time(&currTime);

    struct tm locTime = *(localtime(&currTime));

    char* p = new char[128];
    strcpy(p, "");
    strftime(p, 128, "%Y-%m-%d %H:%M:%S", &locTime);

    dateAndTime = p;

    delete (p);
}


void DbSybase::GetDateAndTime(string& dateAndTime)
{
    time_t currTime;
    time(&currTime);

    struct tm locTime = *(localtime(&currTime));

    char* p = new char[128];
    strcpy(p, "");
    strftime(p, 128, "%Y-%b-%d %H:%M", &locTime);

    dateAndTime = p;

    delete (p);
}


void Db::ConvertDate(string& dbDate, const string& cifDate)
{
    //
    // Reformat CIF date = yyyy-mm-dd[:00:00]  to  Sybase date yyyy-Mon-dd hh:mm
    // 2000-01-27  to 2000-Jan-27 00:00  odate[18]
    //

    dbDate.clear();

    // CIF date must be in the format YYYY[-MM[-DD]]
    if ((cifDate.size() != 4) && (cifDate.size() != 7) &&
      (cifDate.size() != 10))
    {
#ifdef VLAD_LOG_SEPARATION
        if (_verbose)
            _log << "ConvertDate() date format error: " << cifDate << endl;
#endif

        return;
    }

    if (cifDate.size() == 10)
    {
        // The input data is YYYY-MM-DD
        if ((cifDate[4] != '-') && (cifDate[7] != '-'))
        {
#ifdef VLAD_LOG_SEPARATION
            if (_verbose)
                _log << "ConvertDate() date format error: " << cifDate << endl;
#endif
        }
        else
        {
            dbDate = cifDate;
        }
    }
    else if (cifDate.size() == 7)
    {
        // The input data is YYYY-MM
        if (cifDate[4] != '-')
        {
#ifdef VLAD_LOG_SEPARATION
            if (_verbose)
                _log << "ConvertDate() date format error: " << cifDate << endl;
#endif
        }
        else
        {
            dbDate = cifDate + "-01";
        }
    }
    else
    {
        // The input data is YYYY
        dbDate = cifDate + "-01" + "-01";
    }

}


void Db::ConvertTimestamp(string& dbTimestamp, const string& cifTimestamp)
{

    dbTimestamp.clear();

    if (CifString::IsEmptyValue(cifTimestamp))
    {
        return;
    }

    ConvertDate(dbTimestamp, cifTimestamp);

    string::size_type firstColonPos = cifTimestamp.find(':', 0);

    if (firstColonPos == string::npos)
        dbTimestamp += " 00:00";
    else
        dbTimestamp += cifTimestamp.substr(firstColonPos + 1);

}


#ifdef VLAD_DATE_OBSOLETE
void DbSybase::ConvertDate(string& dbDate, const string& cifDate)
{
    //
    // Reformat CIF date = yyyy-mm-dd[:00:00] to Sybase date yyyy-Mon-dd hh:mm
    // 2000-01-27  to 2000-Jan-27 00:00  odate[18]
    //
    // int i, iDay, maxDay, year;
    //char month[5], day[3];
    int maxDay;

    dbDate.clear();

    string baseDate;
    Db::ConvertDate(baseDate, cifDate);

    // Put YYYY and -
    dbDate = baseDate.substr(0, 5);

    //string substr( size_type index, size_type length = npos );

    // Convert MM to textual month and add it
    string::size_type firstDashPos = baseDate.find('-', 0);
    string::size_type secondDashPos = baseDate.find('-', firstDashPos + 1);
  
    string month = baseDate.substr(firstDashPos + 1, secondDashPos -
      (firstDashPos + 1));

    if (month == "01")
    {
        dbDate += "Jan";
        maxDay = 31;
    }
    else if (month == "02")
    {
        dbDate += "Feb";
        int year = atoi((baseDate.substr(0, 4)).c_str());
        if (year % 4 == 0 && year % 100 != 0 || year % 400 == 0)
             maxDay = 29;
        else
             maxDay = 28;
    }
    else if (month == "03")
    {
      dbDate += "Mar";
      maxDay = 31;
    }
    else if (month == "04")
    {
      dbDate += "Apr";
      maxDay = 30;
    }
    else if (month == "05")
    {
        dbDate += "May";
        maxDay = 31;
    }
    else if (month == "06")
    {
        dbDate += "Jun";
        maxDay = 30;
    }
    else if (month == "07")
    {
        dbDate += "Jul";
        maxDay = 31;
    }
    else if (month == "08")
    {
        dbDate += "Aug";
        maxDay = 31;
    }
    else if (month == "09")
    {
        dbDate += "Sep";
        maxDay = 30;
    }
    else if (month == "10")
    {
        dbDate += "Oct";
      maxDay = 31;
    }
    else if (month == "11")
    {
        dbDate += "Nov";
        maxDay = 30;
    }
    else if (month == "12")
    {
        dbDate += "Dec";
        maxDay = 31;
    }
    else
    {
#ifdef VLAD_LOG_SEPARATION
        if (_verbose)
            _log << "_dformat3() month format error: " << cifDate << endl;
#endif

        dbDate.clear();

        return;
    }

    // Put day

    dbDate += baseDate.substr(secondDashPos, 3);

}


void DbLoader::_dformat_3(const char *date, char *odate, int shortFlag) {
  //
  // reformat  CIF date = yyyy-mm-dd[:00:00]  to  Sybase date yyyy-Mon-dd hh:mm
  //                      2000-01-27  to 2000-Jan-27 00:00  odate[18]
  //
  int i, iDay, maxDay, year;
  char month[5], day[3];

  if (odate == NULL) return;
  strcpy(odate,"");

  if (date == NULL ||  date[4] != '-' || date[7] != '-' ) {
    if (_verbose) _log << "_dformat3() date format error: " << date << endl;
    return;
  }


  memset(month, 0,5);
  strncpy(month,&date[5],2);


  strcpy (odate,"");
  strncat(odate,&date[0],4);

  for (i=0; i < 4; i++) {
    if (!isdigit(odate[i])) {
      if (_verbose) _log << "_dformat3() year format error: " << date << endl;
      strcpy(odate,"");
      return;
    }
  }

  year = atoi(odate);
  strcat(odate,"-");

  if (!strcmp(month,"01")) {
    strcat(odate,"Jan");
    maxDay = 31;
  }  else   if (!strcmp(month,"02")) {
    strcat(odate,"Feb");
    if (year % 4 == 0 && year % 100 != 0 || year % 400 == 0)
         maxDay = 29;
    else maxDay = 28;
  } else   if (!strcmp(month,"03")) {
    strcat(odate,"Mar");
    maxDay = 31;
  } else   if (!strcmp(month,"04")) {
    strcat(odate,"Apr");
    maxDay = 30;
  } else   if (!strcmp(month,"05")) {
    strcat(odate,"May");
    maxDay = 31;
  } else   if (!strcmp(month,"06")) {
    strcat(odate,"Jun");
    maxDay = 30;
  } else   if (!strcmp(month,"07")) {
    strcat(odate,"Jul");
    maxDay = 31;
  } else   if (!strcmp(month,"08")) {
    strcat(odate,"Aug");
    maxDay = 31;
  } else   if (!strcmp(month,"09")) {
    strcat(odate,"Sep");
    maxDay = 30;
  } else   if (!strcmp(month,"10")) {
    strcat(odate,"Oct");
    maxDay = 31;
  } else   if (!strcmp(month,"11")) {
    strcat(odate,"Nov");
    maxDay = 30;
  } else   if (!strcmp(month,"12")) {
    strcat(odate,"Dec");
    maxDay = 31;
  } else {  //
    if (_verbose) _log << "_dformat3() month format error: " << date << endl;
    strcpy(odate,"");
    return;
  }




  strcat(odate,"-");
  strncat(odate,&date[8],2);

  if (!isdigit(odate[9]) || !isdigit(odate[10])) {
    if (_verbose) _log << "_dformat3() day format error: " << date << endl;
    strcpy(odate,"");
    return;
  }
  
  day[0] = odate[9];
  day[1] = odate[10];
  day[2] = '\0';
  iDay = atoi(day);
  if ((iDay <= 0) || (iDay > maxDay)) {
    if (_verbose) _log << "_dformat3() day format error: " << date << endl;
    strcpy(odate,"");
    return;
  }

  if (!shortFlag) strcat(odate," 00:00");
}


void DbLoader::_dformat_4(const char *date, char *odate) {
  //
  // Mon dd yyyy to CIF date yyyy-mm-dd  odate[11]
  //
  if (odate == NULL) return;
  strcpy(odate,"");

  if (date == NULL ||  date[3] != ' ' || date[6] != ' ' ) return;

  char month[5];
  memset(month, 0 ,5);
  strncpy(month,date,3);


  strcpy (odate,"");
  strncat(odate,&date[7],4);
  strcat(odate,"-");

  if (!strcmp(month,"Jan"))
    strcat(odate,"01");
  else   if (!strcmp(month,"Feb"))
    strcat(odate,"02");
  else   if (!strcmp(month,"Mar"))
    strcat(odate,"03");
  else   if (!strcmp(month,"Apr"))
    strcat(odate,"04");
  else   if (!strcmp(month,"May"))
    strcat(odate,"05");
  else   if (!strcmp(month,"Jun"))
    strcat(odate,"06");
  else   if (!strcmp(month,"Jul"))
    strcat(odate,"07");
  else   if (!strcmp(month,"Aug"))
    strcat(odate,"08");
  else   if (!strcmp(month,"Sep"))
    strcat(odate,"09");
  else   if (!strcmp(month,"Oct"))
    strcat(odate,"10");
  else   if (!strcmp(month,"Nov"))
    strcat(odate,"11");
  else   if (!strcmp(month,"Dec"))
    strcat(odate,"12");

  strcat(odate,"-");
  strncat(odate,&date[4],2);
  if (odate[8] ==  ' ') odate[8] = '0';

}




void DbLoader::_dformat_5(const char *date, char *odate) {
  //
  // reformat  CIF date = yyyy[-mm-dd:00:00]  to  Oracle/DB2 date yyyy-mm-dd
  //                      2000-01  to 2000-01-01   odate[10]
  //
  int i;

  if (odate == NULL) return;
  strcpy(odate,"");

  if (date == NULL || (strlen(date) < 4) ) {
    if (_verbose) _log << "_dformat_5() date format error" << endl;
    return;
  }

  // Year 
  strncat(odate,&date[0],4);
  for (i=0; i < 4; i++) {
    if (!isdigit(odate[i])) {
      if (_verbose) _log << "_dformat_5() year format error: " << date << endl;
      strcpy(odate,"");
      return;
    }
  }

  //month and day

  if (strlen(date) == 4) {
    strcat(odate,"-01-01");
    return;
  } else if ((strlen(date) == 7) && isdigit(date[5]) && isdigit(date[6])) {
    strcat(odate,"-");
    strncat(odate,&date[5],2);
    strcat(odate,"-01");
    return;
  } else if ((strlen(date) >= 10) && isdigit(date[8]) && isdigit(date[9])) {
    strcat(odate,"-");
    strncat(odate,&date[5],2);
    strcat(odate,"-");
    strncat(odate,&date[8],2);
    return;
  }
}
#endif // VLAD_DATE_OBSOLETE



void DbLoader::_ReorderName(string& res, char *aString, int mode)
{  
  //  mode = 1. Reformat name:  Lastname, I.J. -> I.J. Lastname
  //  mode = 2. Reformat name:  I.J. Lastname  -> Lastname, I.J. 
  
  res.clear();

  if (aString == NULL || aString[0] == '\0') return;
  
  int i, ilen = 0, c='\0';
  ilen = strlen(aString);
  if (ilen < 1) return;


  char *first = new char[ilen+1]; 
  char *last  = new char[ilen+1]; 
  
  if (mode == 1) {
    for (i = strlen(aString) - 1; i >= 0; i--)
      if (aString[i] == ',') break;
    
    if (i > 0) {
      strcpy(first, &aString[i+1]);
      aString[i] = '\0';
      strcpy(last, aString);
      string firstClean = first;
      string lastClean = last;
      CleanString(firstClean);
      CleanString(lastClean);
      if (firstClean[firstClean.size()-1] == '.')
	sprintf(aString, "%s%s%c", first, last,c);
      else 
	sprintf(aString, "%s %s%c", first, last, c);
    }
    delete first;
    delete last;
    res = aString;
    return;
    
  } else if (mode == 2) {
    for (i = strlen(aString) - 1; i >= 0; i--) {
      if (aString[i] == '.') break;
    }

    if (i > 0) {
      strcpy(last, &aString[i+1]);
      aString[i+1] = '\0';
      strcpy(first, aString);
      sprintf(aString, "%s, %s%c", last, first,c);
      res = aString;
      CleanString(res);
      delete first;
      delete last;
      return;
    }
    
    for (i = strlen(aString) - 1; i >= 0; i--)
      if (aString[i] == ' ') break;
    
    if (i > 0) {
      strcpy(last, &aString[i+1]);
      aString[i+1] = '\0';
      strcpy(first, aString);
      sprintf(aString, "%s, %s%c", last, first, c);
      res = aString;
      CleanString(res);
    }
    delete first;
    delete last;
    return;
  }
}  


void DbLoader::CleanString(string& aString)
{
/*
 *  CleanString() strip leading and trailing white space from a string.
 *  Also compress multiple internal white space characters.
 *
 */

  int i, stringlen;
  string res;

  if (aString.empty())
      return;

  stringlen = aString.size();

  for (i=0; i< stringlen; i++)
  { 
      if (aString[i] == ' ' || aString[i] == '\t' ||  aString[i] == '\n')
      {
          //strcpy(aString, &aString[i+1]);	
          //stringlen--;
          //i--;
      }
      else
          break;
  }

  int savedIndex = i;

  for (/*i=0*/; i< stringlen; i++)
  {
      if (i != savedIndex && 
	(aString[i-1] == ' ' || aString[i-1] == '\t' || aString[i-1] == '\n')  && 
	(aString[i]   == ' ' || aString[i]   == '\t' || aString[i]   == '\n' ))       {
          //strcpy(&aString[i-1], &aString[i]);
          //i--;
          //stringlen--;
      }
      else
      {
          if (aString[i] == '\t' || aString[i] == '\n' )
              res.push_back(' ');
          else
              res.push_back(aString[i]);
      }
  }

#ifdef VLAD_DELETED
  stringlen = strlen(aString);
  for (i=0; i< stringlen; i++)
  {
      if (aString[i] == '\t' || aString[i] == '\n' )
      {
          aString[i] = ' ';
      }
  }
#endif

  for (unsigned int i=0; i < res.size(); ++i)
  {
    if (res[res.size() - 1 - i] == ' ' || res[res.size() - 1 - i] == '\t' ||  res[res.size() - 1 - i] == '\n')
        res.erase(res.end() - 1);
    else
        break;
  }

  aString = res;

}


void DbLoader::_ToUpperString(string& aString)
{
/*
 *  _ToUpperString() set string to upper case.
 *
 */

  int i;
  if (_verbose) _log << "_ToUpperString: " << aString << endl;
  if (aString.empty()) return;
  for (i=0; i< (int) aString.size(); i++) { 
    aString[i] = toupper(aString[i]);
  }
  if (_verbose) _log << "_ToUpperString: " << aString << endl;
}

void DbLoader::_StripString(string& aString, int mode)
{
/*
 *  _StripString() remove white space characters.  
 *        mode = 1  - all white space
 *        mode = 2  - remove only \n and \r
 */

  int i, j, iLen;
  char *ostring=NULL;
  if (_verbose) _log << "_StripString: input " << aString << endl;

  if (aString.empty()) return;

  iLen = aString.size();
  ostring = new char[iLen + 1];
  memset(ostring,'\0',iLen+1);

  j=0;
  if (mode == 1) {
    for (i=0; i< iLen; i++) { 
      if ( isspace(aString[i]) ) continue;
      ostring[j] = aString[i]; j++;
    }
  } else if (mode == 2) {
    for (i=0; i< iLen; i++) { 
      if ( aString[i] == '\n' || aString[i] == '\r'  ) continue;
      ostring[j] = aString[i]; j++;
    }

  }
  aString.clear();
  aString = ostring;
  if (_verbose) _log << "_StripString: output " << aString << endl;
  if (ostring) delete[] ostring;

}


static void escapeString(string& outStr, const string& inStr)
{

  int i, iLen;

  outStr.clear();

  if (inStr.empty())
      return;
  
  iLen = inStr.size();

  for (i=0; i < iLen; i++)
  {
    if (inStr[i] == '\r')
    {
      outStr.push_back('\\');
      outStr.push_back('r');
    }
    else if (inStr[i] == '\n')
    {
      outStr.push_back('\\');
      outStr.push_back('n');
    }
    else if (inStr[i] == '\t')
    {
      outStr.push_back('\\');
      outStr.push_back('t');
    }
    else 
      outStr.push_back(inStr[i]);
  }
  
}


static void name_conversion_first_last(char* aString)
{
    int i, len;
    char first[500], last[500];

    if (aString == NULL || aString[0] == '\0')
        return;

    len = strlen(aString);
    for (i = len - 1; i >= 0; i--)
        if (aString[i] == ',')
            break;

    if (i > 0)
    {
        strcpy(first, &aString[i+1]);
        aString[i] = '\0';
        strcpy(last, aString);
        string fString(first);
        string lString(last);
        String::StripAndCompressWs(fString);
        String::StripAndCompressWs(lString);
        if (fString[strlen(fString.c_str())-1] == '.')
            sprintf(aString, "%s%s", fString.c_str(), lString.c_str());
        else
            sprintf(aString, "%s %s", fString.c_str(), lString.c_str());
    }
}


#ifdef DB_HASH_ID
long long DbLoader::pdbIdHash(const string& id) 
{

    long long ival, base, base1;

    int i, j;
    int id1[5];

    const char* list = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"; 

    ival = (long long) 0;
    base = (long long) strlen(list);

    if (id.empty() || (id.size() < 4))
        return ival;

    if (id.size() == 4)
    {
        base1 = (long long) 1;
        for (i = 3, j = 0; i >= 0; i--)
        {
            if (i != 3)
                base1 = base1*base;
      
            for (j = 0; j < base; j++)
            {
	        if (id[i] == list[j])
                {
                    ival = ival + base1 * j;
                    break;
                }
            }
        }
        ival = ival * 4000000;
    }
    else if (id.size() == 7)
    {
        i=0;
        for (j = 3; j < 7; j++)
        {
            id1[i] = id[j];
            i++;
        }
        id1[i] = '\0';

        base1 = (long long) 1;
        for (i = 3, j = 0; i >= 0; i--)
        {
            if (i != 3)
                base1 = base1*base;
      
            for (j = 0; j < base; j++)
            {
	        if (id1[i] == list[j])
                {
                    ival = ival + base1 * j;
                    break;
                }
            }
        }
        ival = ival * 4000000;
    }
    else if (id.size() == 10)
    {
        base = 10;
        base1 = (long long) 1;
        for (i = 9, j = 0; i >= 4; i--)
        {
            if (i != 9)
                base1 = base1*base;
 
            for (j = 0; j < base; j++)
            {
	        if (id[i] == list[j])
                {
                    ival = ival + base1 * j;
                    break;
                }
            }
        }
        ival = ival + 2000000;
        ival = ival * 4000000;
    }
    else if (id.size() == 6)
    {
        // NDB ids without pdb ids.
        base = (long long) strlen(list);
        base1 = (long long) 1;
        for (i = 5, j = 0; i >= 0; i--)
        {
            if (i != 5)
                base1 = base1*base;
      
            for (j = 0; j < base; j++)
            {
                if (id[i] == list[j])
                {
                    ival = ival + base1 * j;
                    break;
                }
            }
        }
        ival = ival + 2000000;
        ival = ival * 2000000;
    }

    return (ival);

}
#endif
