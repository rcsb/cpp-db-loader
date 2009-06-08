/*$$FILE$$*/
/*$$VERSION$$*/
/*$$DATE$$*/
/*$$LICENSE$$*/

 
#include <string>
#include <vector>
#include <ostream>
#include <iostream>
#include <fstream>

#include "DictObjFile.h"
#include "DictObjCont.h"
#include "PdbMlSchema.h"
#include "DictParentChild.h"
#include "PdbMlWriter.h"
#include "XmlOutput.h"


using std::string;
using std::ostream;
using std::ios;
using std::endl;
using std::cerr;
using std::ofstream;


// For controlling schema only related operations
const string XmlOutput::_BASE_SCHEMA_FILE = "DB_LOADER_SCHEMA_XML";


XmlOutput::XmlOutput(Db& db, const string& dictObjFileName,
  const string& dictName, const string& ns) : DbOutput(db), _ns(ns)
{
    _SCHEMA_FILE = _BASE_SCHEMA_FILE + ".xsd";

    _dictObjFileP = new DictObjFile(dictObjFileName);

    _dictObjFileP->Read();

    _dictObjContP = &(_dictObjFileP->GetDictObjCont(dictName));

    _schemaDataInfoP = new SchemaDataInfo(_db._schemaMapping, *_dictObjContP);

    _dictParentChildP = new DictParentChild(*_dictObjContP, *_schemaDataInfoP);
}


XmlOutput::~XmlOutput()
{
    delete (_dictParentChildP);
    delete (_schemaDataInfoP);
    delete (_dictObjFileP);
}


void XmlOutput::WriteSchema(const string& workDir)
{
    string oFile = workDir + _SCHEMA_FILE;
    ofstream io(oFile.c_str(), ios::out | ios::trunc);

    XsdWriter xsdWriter(io);

    PdbMlSchema pdbMlSchema(xsdWriter, *_dictParentChildP,
      *_schemaDataInfoP, _ns, _BASE_SCHEMA_FILE);

    pdbMlSchema.Convert();

    io.close();
}


void XmlOutput::WriteData(Block& block, const string& workDir)
{
    cerr << "Starting cif->xml" << endl;

    string oFile = workDir;
    if (!_INPUT_FILE.empty())
    {
        oFile += _INPUT_FILE;
        oFile += ".xml";
    }
    else
    {
        oFile += "data.xml";
    }
 
    cerr << "CIF->XML for file " << oFile << endl;

    ofstream ioxml;
    if (_db.GetAppendFlag())
    {
        ioxml.open(oFile.c_str(), ios::out | ios::app);
    }
    else
    {
        ioxml.open(oFile.c_str(), ios::out | ios::trunc);
    }

    PdbMlWriter pdbMlWriter(ioxml, _ns, *_schemaDataInfoP);

    pdbMlWriter.WriteDeclaration();
    pdbMlWriter.WriteNewLine();

    pdbMlWriter.WriteDatablockOpeningTag();
    pdbMlWriter.WriteNewLine();

    pdbMlWriter.IncrementIndent();

    pdbMlWriter.Indent();

    string fullSchemaFileName;
    PdbMlSchema::MakeFullSchemaFileName(fullSchemaFileName,
      _BASE_SCHEMA_FILE);

    pdbMlWriter.WriteNamespaceAttribute(pdbMlWriter.GetNamespace(),
      fullSchemaFileName, true);

    pdbMlWriter.WriteNewLine();

    pdbMlWriter.Indent();
    pdbMlWriter.WriteXsiNamespace();
    pdbMlWriter.WriteNewLine();

    pdbMlWriter.Indent();
    string schemaFileName;
    PdbMlSchema::MakeSchemaFileName(schemaFileName, _BASE_SCHEMA_FILE);

    pdbMlWriter.WriteSchemaLocationAttribute(fullSchemaFileName + " " +
     schemaFileName, true);

    pdbMlWriter.WriteClosingBracket();
    pdbMlWriter.WriteNewLine();

    pdbMlWriter.DecrementIndent();

    vector<string> tableNames;
    _db._schemaMapping.GetDataTablesNames(tableNames);

    for (unsigned int i = 0; i < tableNames.size(); ++i)
    {
        if (_db.GetUseOnlyPopulated() &&
          (!_db._schemaMapping.IsTablePopulated(tableNames[i])))
        {
            continue;
        }

        cerr << "  Table " << tableNames[i] << endl;

        ISTable* t = block.GetTablePtr(tableNames[i]);
        if (t != NULL && t->GetNumRows() > 0)
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

            pdbMlWriter.IncrementIndent();

#ifdef VLAD_DEBUG_ATOM_SITE
            cout << "Xml Output: Table \"" << t->GetName() << "\" has " << 
              columnNames.size() << " columns." << endl;
#endif

            pdbMlWriter.WriteTable(t, widths,
              _db._schemaMapping.GetReviseSchemaMode(), typeCodes);

            pdbMlWriter.DecrementIndent();

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
    }

    pdbMlWriter.WriteDatablockClosingTag();

    ioxml.close();
}


void XmlOutput::_WriteTable(ostream& io, ISTable* tIn,
  vector<unsigned int>& widths, const bool reCalcWidth,
  const vector<eTypeCode>& typeCodes)
{
    PdbMlWriter pdbMlWriter(io, "", *_schemaDataInfoP);

    pdbMlWriter.WriteTable(tIn, widths, reCalcWidth, typeCodes);
}


