/*$$FILE$$*/
/*$$VERSION$$*/
/*$$DATE$$*/
/*$$LICENSE$$*/


/*!
** \file XmlOutput.h
**
** \brief Header file for XmlOutput class.
*/


#ifndef XMLOUTPUT_H
#define XMLOUTPUT_H


#include <string>
#include <vector>
#include <ostream>

#include "DictObjFile.h"
#include "DictObjCont.h"
#include "SchemaDataInfo.h"
#include "SchemaParentChild.h"
#include "Db.h"
#include "DbOutput.h"


/**
**  \class XmlOutput
**
**  \brief XML output class.
**
**  This class represents an XML output. It re-implements methods
**  for schema and data writing.
*/
class XmlOutput : public DbOutput
{
  public:
    XmlOutput(Db& db, const std::string& dictObjFileName,
      const std::string& dictName,
      const std::string& ns = std::string());
    virtual ~XmlOutput();

    void WriteSchema(const std::string& path = std::string());
    void WriteData(Block& block, const std::string& path = std::string());

  protected:
    void _WriteTable(std::ostream& io, ISTable* tIn,
      std::vector<unsigned int>& widths,
      const bool reCalcWidth = false,
      const std::vector<eTypeCode>& typeCodes =
        std::vector<eTypeCode> (0));

  private:
    static const std::string _BASE_SCHEMA_FILE;

    std::string _ns;

    SchemaDataInfo* _schemaDataInfoP;
    DictParentChild* _dictParentChildP;

    DictObjFile* _dictObjFileP;
    DictObjCont* _dictObjContP;
};

#endif
