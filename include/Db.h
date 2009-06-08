/*$$FILE$$*/
/*$$VERSION$$*/
/*$$DATE$$*/
/*$$LICENSE$$*/


/*!
** \file Db.h
**
** \brief Header file for Db class.
*/


#ifndef DB_H
#define DB_H


#include <string>
#include <vector>
#include <ostream>

#include "SchemaMap.h"


/**
**  \class Db
**
**  \brief Base class for all databases.
**
**  This class encapsulates general characteristics of each database. It
**  contains methods for generic database related operations and virtual
**  methods, that are to be specialized in concrete databases.
*/
class Db
{
  public:
    static const std::string DB_DEFAULT_NAME;

    SchemaMap& _schemaMapping;

    Db(SchemaMap& schemaMapping, const std::string& dbName = DB_DEFAULT_NAME);
    virtual ~Db();

    void SetUseOnlyPopulated(bool mode=true);
    bool GetUseOnlyPopulated();

    void SetAppendFlag(const bool appendFlag);
    bool GetAppendFlag();

    void SetFieldSeparator(const std::string& fieldSeparator);
    void SetRowSeparator(const std::string& rowSeparator);

    std::string GetCommandTerm();

    std::string GetFieldSeparator();
    std::string GetRowSeparator();

    virtual void GetStart(std::string& start);

    virtual void WriteSchemaStart(std::ostream& io);
    void WriteDeleteTable(std::ostream& io, const std::string& table,
      const std::string& where, const std::string& what);

    virtual void DropTableSql(std::ostream& io, const std::string& tableNameDb);

    virtual const std::string& GetExec();
    virtual const std::string& GetExecOption();
    virtual const std::string& GetUserOption();
    virtual const std::string& GetPassOption();

    virtual const std::string& GetEnvDbUser();
    virtual const std::string& GetEnvDbPass();

    virtual const std::string& GetConnect();
    virtual const std::string& GetTerminate();
    virtual const std::string& GetDbCommand();

    virtual void WriteLoad(std::ostream& io);

    const std::string& GetDataLoadingFileName();

    virtual void WriteLoadingStart(std::ostream& io);
    virtual void WriteLoadingEnd(std::ostream& io);
    virtual void WriteLoadingTable(std::ostream& io,
      const std::string& tableName, const std::string& path);

    virtual void WritePrint(std::ostream& io, const std::string& tableNameDb);

    virtual void GetChar(std::string& dType, const unsigned int width);
    virtual void GetFloat(std::string& dType);
    virtual void GetText(std::string& dType, const unsigned int width);
    virtual void GetDate(std::string& dType);
    virtual void WriteNull(std::ostream& io, const int iNull,
      const unsigned int curr, const unsigned int attSize);
    virtual void WriteTableIndex(std::ostream& io,
      const std::string& tableNameDb,
      const vector<std::string>& indexList);

    const std::string& GetBcpStringDelimiter();
    virtual void WriteBcpDoubleQuotes(std::ostream& io);

    virtual void WriteNewLine(std::ostream& io, bool special = false);

    bool IsFirstTextNewLineSpecial();

    virtual void ConvertDate(std::string& dbDate, const std::string& cifDate);
    virtual void ConvertTimestamp(std::string& dbTimestamp,
      const std::string& cifTimestamp);

    virtual void GetDateAndTime(std::string& dateAndTime);

  protected:
    bool _useOnlyPopulated;

    bool _appendFlag;

    // Field and row separators for compact output (eg. BCP)
    std::string _fieldSeparator; 
    std::string _rowSeparator;   

    std::string _cmdTerm;            // SQL command terminator.

    // Used in assigning permissions when exporting schema in SQL. 
    std::string _dbName; // Target database name 

    std::string _exec;
    std::string _execOption;
    std::string _userOption;
    std::string _passOption;

    std::string _connect;
    std::string _terminate;
    std::string _dbCommand;
    std::string _envDbUser;
    std::string _envDbPass;

    std::string _dataLoadingFileName;

    std::string _bcpStringDelimiter;

    bool _firstTextNewLineSpecial;

  private:
    static const std::string _SCRIPT_LOADING_FILE;

};

#endif
