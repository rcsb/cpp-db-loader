/*$$FILE$$*/
/*$$VERSION$$*/
/*$$DATE$$*/
/*$$LICENSE$$*/


/*!
** \file CifSchemaMap.h
**
** \brief Header file for DbLoader related classes.
*/


#ifndef CIFSCHEMAMAP_H
#define CIFSCHEMAMAP_H


#include <string>
#include <ostream>
#include <fstream>

#include "CifFile.h"
#include "SchemaMap.h"
#include "Db.h"
#include "DbOutput.h"



/**
**  \class DbOracle
**
**  \brief Oracle database class.
**
**  This class represents an Oracle database.
*/
class DbOracle : public Db
{

  // Oracle 8.1.6 SQLLDR

  public:
    DbOracle(SchemaMap& schemaMapping,
      const string& dbName = DB_DEFAULT_NAME);
    ~DbOracle();

    void WriteSchemaStart(std::ostream& io);

    void WriteLoadingStart(std::ostream& io);
    void WriteLoadingTable(std::ostream& io, const string& tableName,
      const string& path);

    void GetDate(string& dType);
    void GetText(string& dType, const unsigned int width);

    void WriteNull(std::ostream& io, const int iNull,
      const unsigned int curr, const unsigned int attSize);
    void WriteTableIndex(std::ostream& io, const string& tableNameDb,
      const vector<string>& indexList);

    void WriteNewLine(std::ostream& io, bool special = false);
};


/**
**  \class DbDb2
**
**  \brief DB2 database class.
**
**  This class represents a DB2 database.
*/
class DbDb2 : public Db
{

  public:
    DbDb2(SchemaMap& schemaMapping,
      const string& dbName = DB_DEFAULT_NAME);
    ~DbDb2();


    void GetStart(string& start);

    void WriteSchemaStart(std::ostream& io);

    void WriteLoadingStart(std::ostream& io);
    void WriteLoadingEnd(std::ostream& io);
    void WriteLoadingTable(std::ostream& io, const string& tableName,
      const string& path);

    void GetFloat(string& dType);
    void GetDate(string& dType);
    void GetText(string& dType, const unsigned int width);
    void WriteNull(std::ostream& io, const int iNull,
      const unsigned int curr, const unsigned int attSize);

    void WriteTableIndex(std::ostream& io, const string& tableNameDb,
      const vector<string>& indexList);

    void WriteBcpDoubleQuotes(std::ostream& io);
};


/**
**  \class DbMySql
**
**  \brief MySQL database class.
**
**  This class represents a MySQL database.
*/
class DbMySql : public Db
{

  public:
    DbMySql(SchemaMap& schemaMapping,
      const std::string& dbName = DB_DEFAULT_NAME,
      const bool useMySqlDbHostOption = false,
      const bool useMySqlDbPortOption = false);
    ~DbMySql();

    void GetStart(string& start);

    void DropTableSql(std::ostream& io, const string& tableNameDb);

    void WriteLoad(std::ostream& io);
    void WriteLoadingStart(std::ostream& io);
    void WriteLoadingTable(std::ostream& io, const string& tableName,
      const string& path);

    void WriteTableIndex(std::ostream& io, const string& tableNameDb,
      const vector<string>& indexList);

    void WriteNull(std::ostream& io, const int iNull,
      const unsigned int curr, const unsigned int attSize);

    void WriteNewLine(std::ostream& io, bool special = false);

  private:
    bool _useMySqlDbHostOption;
    bool _useMySqlDbPortOption;

    static const string _SQL_LOADING_FILE;

};


/**
**  \class DbSybase
**
**  \brief Sybase database class.
**
**  This class represents a Sybase database.
*/
class DbSybase : public Db
{
  public:
    DbSybase(SchemaMap& schemaMapping,
      const string& dbName = DB_DEFAULT_NAME);
    ~DbSybase();

    void GetStart(string& start);

    void WriteLoadingStart(std::ostream& io);
    void WriteLoadingTable(std::ostream& io, const string& tableName,
      const string& path);

    void WritePrint(std::ostream& io, const string& tableNameDb);
    void WriteNull(std::ostream& io, const int iNull,
      const unsigned int curr, const unsigned int attSize);

    void WriteTableIndex(std::ostream& io, const string& tableNameDb,
      const vector<string>& indexList);

    void GetDateAndTime(string& dateAndTime);

#ifdef VLAD_DATE_OBSOLETE
    void ConvertDate(string& dbDate, const string& cifDate);
#endif
};


/**
**  \class BcpOutput
**
**  \brief BCP output class.
**
**  This class represents a BCP output.  It re-implements methods
**  for data and loading scripts generation.
*/
class BcpOutput : public DbOutput
{
  public:
    BcpOutput(Db& db);
    virtual ~BcpOutput();

    void WriteDataLoadingScripts(const string& path = std::string());
    void WriteData(Block& block, const string& path = std::string());

  private:
    static const string _DATA_DELETE_FILE;

    void WriteDataLoadingScript(const string& path);
    void WriteDataLoadingFile(const string& path = std::string());

    void WriteDelete(std::ostream& io);

    void WriteEmptyString(std::ostream& io);

    void WriteSpecialDateChar(std::ostream& io, const char& specDateChar);
};
 

/**
**  \class SqlOutput
**
**  \brief SQL output class.
**
**  This class represents an SQL output. It re-implements methods
**  for schema, data and loading scripts generation.
*/
class SqlOutput : public DbOutput
{

  public:
    SqlOutput(Db& db);
    virtual ~SqlOutput();

    void WriteSchema(const string& path = std::string());
    void WriteDataLoadingScripts(const string& path = std::string());
    void WriteData(Block& block, const string& path = std::string());

  protected:
    void WriteEmptyNumeric(std::ostream& io);
    bool IsFirstTextNewLineSpecial();
    void WriteNewLine(std::ostream& io, bool special = false);
    void GetTableStart(string& tableStart, const string& tableName);
    void GetTableEnd(string& tableEnd);

  private:
    static const unsigned int _MAX_SQL_NAME_LENGTH = 60;
    static const string _SCHEMA_LOADING_SCRIPT;
    static const string _SCHEMA_DELETE_FILE;
    static const string _DATA_FILE;

    void WriteSqlScriptSchemaInfo(std::ostream& io);
    void WriteDataLoadingScript(const string& path);

    void CreateTableSql(std::ostream& io, const string& tableName);

    void WriteAuxTables(std::ostream& io, ISTable* infoP,
      const vector<string>& tableNames);

    void WriteEmptyString(std::ostream& io);
    void WriteEmptyDate(std::ostream& io);
};


/**
**  \class DbLoader
**
**  \brief The controller of CIF to DB data conversion process.
**
**  This class controls the conversion process of CIF data to DB loadable data.
**  It provides methods for its construction/destruction, conversion of
**  ASCII CIF files, conversion of serialized (binary) CIF files, conversion of
**  in-memory CIF file objects, setting the working directory. Data conversion
**  methods can be instructed to generate: only the data loading files, only
**  the data loading shell scripts or both the data and scripts. DB server
**  data loading is accomplished by executing the generated loading shell
**  scripts, which utilize the data loading files, obtained during the
**  conversion process.
*/
class DbLoader
{
  public:
    enum eConvOpt
    {
        // Generate only data loading files
        eDATA_ONLY = 0,

        // Generate both data loading files and loading shell scripts
        eDATA_WITH_SCRIPTS,

        // Generate only loading shell scripts
        eSCRIPTS_ONLY
    };

    /**
    **  Constructs a CIF to DB controller object.
    **  
    **  \param[in] schemaMapping - reference to the schema mapping object
    **  \param[in] dbOutput - reference to the database output object
    **  \param[in] verbose - optional parameter that indicates whether
    **    logging should be turned on (if true) or off (if false).
    **    If \e verbose is not specified, logging is turned off.
    **  \param[in] workDir - optional parameter that indicates the working
    **    directory in which the files will be placed. If \e workDir is
    **    not specified, current process working directory is used.
    **
    **  \return Not applicable
    **
    **  \pre None
    **
    **  \post None
    **
    **  \exception: None
    */
    DbLoader(SchemaMap& schemaMapping, DbOutput& dbOutput,
      bool verbose = false, const string& workDir = std::string());

    /**
    **  Destructs a CIF to DB controller object.
    **
    **  \param: Not applicable
    **
    **  \return Not applicable
    **
    **  \pre None
    **
    **  \post None
    **
    **  \exception: None
    */
    virtual ~DbLoader();

    /**
    **  Sets the working directory in which the generated files will be placed.
    **
    **  \param[in] workDir - indicates the working directory in which the
    **    files will be placed.
    **
    **  \return None
    **
    **  \pre None
    **
    **  \post None
    **
    **  \exception: None
    */
    void SetWorkDir(const string& workDir);

    /**
    **  Converts an ASCII CIF file to DB loadable data with/without generating
    **  loading scripts. The specified file is first parsed and then converted.
    **
    **  \param[in] asciiFile - indicates the name of the ASCII CIF file,
    **    which is to be converted.
    **  \param[in] convOpt - indicates data conversion options: generate
    **    data only, generate data with loading scripts, generate loading
    **    scripts only.
    **  \param[in] skipCatList - optional parameter which indicates a list
    **    of categories not to be parsed from the file.
    **
    **  \return None
    **
    **  \pre None
    **
    **  \post None
    **
    **  \exception: None
    */
    void AsciiFileToDb(const string& asciiFile, const eConvOpt convOpt,
      const vector<string>& skipCatList = std::vector<std::string>());

    /**
    **  Converts a serialized (binary) CIF file to DB loadable data
    **  with/without generating loading scripts. The specified file is
    **  de-serialized (without the need to parse it) and converted.
    **  
    **  \param[in] serFile - indicates the name of the serialized (binary)
    **    CIF file, which is to be converted.
    **  \param[in] convOpt - indicates data conversion options: generate
    **    data only, generate data with loading scripts, generate loading
    **    scripts only.
    **
    **  \return None
    **
    **  \pre None
    **
    **  \post None
    **
    **  \exception: None
    */
    void SerFileToDb(const string& serFile, const eConvOpt convOpt);

    /**
    **  Converts an in-memory CIF file object to DB loadable data
    **  with/without generating loading scripts.
    **  
    **  \param[in] cifFile - a reference to the in-memory CIF file object,
    **    which is to be converted.
    **  \param[in] convOpt - indicates data conversion options: generate
    **    data only, generate data with loading scripts, generate loading
    **    scripts only.
    **
    **  \return None
    **
    **  \pre None
    **
    **  \post None
    **
    **  \exception: None
    */
    void FileObjToDb(CifFile& cifFile, const eConvOpt convOpt);

#ifdef DB_HASH_ID
    void SetHashMode(int mode);
#endif

  private:
    static const string _LOG_FILE;

    string _workDir; // Working directory for all generated files.
    string _INPUT_FILE;

#ifdef DB_HASH_ID
    string _HASH_ID;
    int _hashMode;
#endif

    // Block name of loadable data in mmCIF format
    string _blockName;

    bool _verbose;

    std::ofstream _log;

    SchemaMap& _schemaMapping;
    DbOutput& _dbOutput;

    void _LoadBlock(Block& rBlock, Block& wBlock);

    bool _Search(vector<vector<string> >& dMap, const unsigned int iAttr,
      ISTable* isTableP, const string& blockName,
      const vector<string>& cNameMap, const string& sItem,
      const string& sCnd, const string& sFnct);
 
    void _DoFunc(vector<string>& s, const vector<string>& r,
      const string& sFnct);

    void _OpenLog(const string& logName);

    int _GetMapColumnIndex(const vector<string>& cNameMap, const string& vOf);
    void _GetMapColumnValue(string& p, vector<string>& dMapVec,
      unsigned int irow);

    void CreateTables(CifFile& writeCifFile, CifFile& readCifFile);

#ifdef VLAD_DATE_OBSOLETE
    void _dformat_1(const char *date, char *odate);
    void _dformat_2(const char *date, char *odate);
    void _dformat_3(const char *date, char *odate, int shortFlag);
    void _dformat_4(const char *date, char *odate);
    void _dformat_5(const char *date, char *odate);
#endif

    void _ReorderName(string& res, char *string, int mode);
    void _ToUpperString(string& aString);
    void _StripString(string& aString, int mode);

#ifdef DB_HASH_ID
    long long pdbIdHash(const string& id);
#endif

    static void CleanString(string& aString);

    void Clear();
};


#endif
