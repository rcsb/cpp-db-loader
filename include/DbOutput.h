/*$$FILE$$*/
/*$$VERSION$$*/
/*$$DATE$$*/
/*$$LICENSE$$*/


/*!
** \file DbOutput.h
**
** \brief Header file for DbOutput class.
*/


#ifndef DBOUTPUT_H
#define DBOUTPUT_H


#include <string>
#include <vector>
#include <ostream>

#include "Db.h"


/**
**  \class DbOutput
**
**  \brief Base class for all output formats.
**
**  This class encapsulates general characteristics of each kind of output.
**  It contains virtual methods for generating the schema, data and loading
**  scripts.
*/
class DbOutput
{

  public:
    Db& _db;

    DbOutput(Db& db);
    virtual ~DbOutput();

    virtual void WriteSchema(const std::string& path = std::string());
    virtual void WriteDataLoadingScripts(const std::string& path =
      std::string());
    virtual void WriteData(Block& block, const std::string& path =
      std::string());

    void SetInputFile(const std::string& inpFile);

    const std::string& GetCommandScriptName();

  protected:
    static const std::string _DATA_LOADING_SCRIPT;

    std::string _SCHEMA_FILE;

    std::string _INPUT_FILE;

    std::string _stringDelimiter;
    std::vector<char> _specialChars;

    std::string _dateDelimiter;
    std::vector<char> _specialDateChars;

    std::string _itemSeparator; 
    std::string _rowSeparator; 

    void WriteDbExec(std::ostream& io, const std::string& fileName,
      const unsigned int indentLevel = 0);
    void WriteDbExecOnly(std::ostream& io, const std::string& fileName,
      const unsigned int indentLevel = 1);

    void WriteHeader(std::ostream& io);

    void _FormatNumericData(std::ostream &io, const std::string &cs);
    void _FormatStringData(std::ostream& io, const std::string& cs,
      const unsigned int maxWidth);
    void _FormatTextData(std::ostream &io, const std::string &cs);
    void _FormatDateData(std::ostream& io, const std::string& cs,
      const unsigned int maxWidth);

    void _FormatData(std::ostream& io, const std::string& cs,
      const unsigned int type, const unsigned int witdh);

    bool IsSpecialChar(const char& character);
    bool IsSpecialDateChar(const char& character);

    virtual void WriteEmptyNumeric(std::ostream& io);
    virtual void WriteEmptyString(std::ostream& io);
    virtual void WriteSpecialChar(std::ostream& io, const char& specChar);

    virtual void WriteEmptyDate(std::ostream& io);
    virtual void WriteSpecialDateChar(std::ostream& io,
      const char& specDateChar);

    virtual bool IsFirstTextNewLineSpecial();
    virtual void WriteNewLine(std::ostream& io, bool special = false);

    virtual void GetTableStart(std::string& tableStart,
      const std::string& tableName);
    virtual void GetTableEnd(std::string& tableEnd);
    const std::string& GetItemSeparator();
    const std::string& GetRowSeparator();

    void GetMasterIndexAttribValue(std::string& masterIndexAttribValue,
      Block& block, const std::string& masterIndexAttribName,
      const std::vector<std::string>& tableNames);

  protected:
    virtual void _WriteTable(std::ostream& io, ISTable* tIn,
      std::vector<unsigned int>& widths,
      const bool reCalcWidth = false,
      const std::vector<eTypeCode>& typeCodes =
        std::vector<eTypeCode> (0));

  private:
    static void _FormatStringDataSql(std::ostream &io, const std::string& cs,
      unsigned int maxWidth);
};

#endif
