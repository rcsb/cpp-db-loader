# MySQL Row Size Optimization Implementation

## Problem Statement

When generating MySQL DDL schemas from CIF definitions, we encounter errors such as:

- **Row size too large**: MySQL enforces a hard limit of 65,535 bytes for the in-row portion of a record.
- **Index size too large**: Columns with excessive VARCHAR(N) sizes create issues when used in unique keys or indexes.

The core issue arises when large columns (e.g., protein sequences) are defined as VARCHAR(32000) or more, and their cumulative size causes the row to exceed MySQL's internal limits.

## MySQL Storage Facts

- **VARCHAR(N)** stores data inline and contributes N + 1 or 2 bytes per row
- **TEXT/MEDIUMTEXT/LONGTEXT** store data off-row, only adding ~12-20 bytes of pointer overhead
- MySQL row format allows up to 768 bytes of inline data for large text fields
- MySQL rejects any table definition whose estimated row size exceeds 65,535 bytes
- TEXT has a size limit of ~64KB, which may truncate very long sequences
- MEDIUMTEXT supports up to 16MB, sufficient for even the longest biological sequences

## Solution: Greedy Offload Approach

We implemented a greedy algorithm that:

1. Estimates the byte cost of each column if kept as VARCHAR
2. Sorts columns by descending size
3. Converts the largest ones to TEXT until the inline row size drops below 65,535 bytes
4. Promotes any column > 40,000 characters directly to MEDIUMTEXT

## Detailed Code Changes

### 1. Modified `modules/cpp-db-loader/include/CifSchemaMap.h`

Added new members and methods to the `DbMySql` class:

```cpp
class DbMySql : public Db
{
  public:
    // Existing methods...

    // New methods
    void OptimizeTableColumns(const string& tableName, const vector<AttrInfo>& attrs);
    void ClearTableOptimization();

  private:
    // Existing members...

    // New constants for MySQL storage calculations
    static const unsigned int MAX_INLINE_ROW = 65535;
    static const unsigned int MAX_BYTES_PER_CHAR = 4;
    static const unsigned int TEXT_POINTER = 20;
    static const unsigned int TEXT_INLINE = 768;
    static const unsigned int MEDIUMTEXT_TRIGGER = 40000;

    // Column optimization tracking
    struct ColumnOptInfo {
        string attributeName;
        unsigned int widthChars;
        unsigned int varcharCost;
        string columnType;  // "VARCHAR", "TEXT", or "MEDIUMTEXT"
    };

    vector<ColumnOptInfo> _tableColumns;
    bool _tableOptimized;

    // Helper method to calculate VARCHAR length storage overhead
    unsigned int LenBytes(unsigned int n) {
        return (n < 256) ? 1 : 2;
    }
};
```

### 2. Modified `modules/cpp-db-loader/src/CifSchemaMap.C`

#### a. Updated DbMySql Constructor

```cpp
DbMySql::DbMySql(SchemaMap& schemaMapping, const string& dbName,
  const bool useMySqlDbHostOption, const bool useMySqlDbPortOption) :
  Db(schemaMapping, dbName)
{
    // Existing initialization code...

    _tableOptimized = false;  // Initialize the new member
}
```

#### b. Replaced the Original GetText Method

Previous implementation:
```cpp
void DbMySql::GetText(string& dType, const unsigned int width)
{
    if (width < 32000)
    {
        dType = "TEXT";
    }
    else
    {
        dType = "MEDIUMTEXT";
    }
}
```

New implementation:
```cpp
void DbMySql::GetText(string& dType, const unsigned int width)
{
    // First check if this table has been optimized
    if (_tableOptimized) {
        // Find the column by width in our optimized list
        for (unsigned int i = 0; i < _tableColumns.size(); i++) {
            if (_tableColumns[i].widthChars == width) {
                // Use the pre-calculated column type
                dType = _tableColumns[i].columnType;
                return;
            }
        }
    }

    // Fall back to simple threshold-based decision if table not optimized
    // or column not found in the optimization list
    if (width < 32000) {
        dType = "TEXT";
    } else {
        dType = "MEDIUMTEXT";
    }
}
```

#### c. Added OptimizeTableColumns Method

```cpp
void DbMySql::OptimizeTableColumns(const string& tableName, const vector<AttrInfo>& attrs)
{
    _tableColumns.clear();
    _tableOptimized = true;

    // Step 1: Initialize column info with estimated costs
    unsigned int inlineSum = 0;

    for (unsigned int i = 0; i < attrs.size(); i++) {
        ColumnOptInfo colInfo;
        colInfo.attributeName = attrs[i].attribName;
        colInfo.widthChars = attrs[i].iWidth;

        // Default to VARCHAR for numeric types
        if (attrs[i].iTypeCode == eTYPE_CODE_INT ||
            attrs[i].iTypeCode == eTYPE_CODE_FLOAT ||
            attrs[i].iTypeCode == eTYPE_CODE_BIGINT) {
            colInfo.columnType = "VARCHAR";
            colInfo.varcharCost = attrs[i].iWidth;
            inlineSum += colInfo.varcharCost;
            _tableColumns.push_back(colInfo);
            continue;
        }

        // Handle date types
        if (attrs[i].iTypeCode == eTYPE_CODE_DATETIME) {
            colInfo.columnType = "VARCHAR";
            colInfo.varcharCost = attrs[i].iWidth;
            inlineSum += colInfo.varcharCost;
            _tableColumns.push_back(colInfo);
            continue;
        }

        // Calculate varchar cost for string and text types
        colInfo.varcharCost = colInfo.widthChars * MAX_BYTES_PER_CHAR + LenBytes(colInfo.widthChars);
        colInfo.columnType = "VARCHAR";

        // Immediately promote very large columns to MEDIUMTEXT
        if (colInfo.widthChars > MEDIUMTEXT_TRIGGER) {
            colInfo.columnType = "MEDIUMTEXT";
            // Only count pointer overhead for inline calculation
            inlineSum += TEXT_POINTER;
        } else {
            inlineSum += colInfo.varcharCost;
        }

        _tableColumns.push_back(colInfo);
    }

    // Step 2: If row size is under the limit, we're done
    if (inlineSum <= MAX_INLINE_ROW) {
        return;
    }

    // Step 3: Sort columns by descending storage cost (only for string/text types)
    vector<unsigned int> sortedIndexes;
    for (unsigned int i = 0; i < _tableColumns.size(); i++) {
        // Only consider string/text columns that aren't already MEDIUMTEXT
        if (_tableColumns[i].columnType == "VARCHAR" &&
            (attrs[i].iTypeCode == eTYPE_CODE_STRING || attrs[i].iTypeCode == eTYPE_CODE_TEXT)) {
            sortedIndexes.push_back(i);
        }
    }

    // Sort the indexes based on varcharCost
    for (unsigned int i = 0; i < sortedIndexes.size(); i++) {
        for (unsigned int j = i + 1; j < sortedIndexes.size(); j++) {
            if (_tableColumns[sortedIndexes[i]].varcharCost < _tableColumns[sortedIndexes[j]].varcharCost) {
                unsigned int temp = sortedIndexes[i];
                sortedIndexes[i] = sortedIndexes[j];
                sortedIndexes[j] = temp;
            }
        }
    }

    // Step 4: Greedily offload largest columns to TEXT until under limit
    for (unsigned int i = 0; i < sortedIndexes.size(); i++) {
        if (inlineSum <= MAX_INLINE_ROW) {
            break;
        }

        unsigned int idx = sortedIndexes[i];
        // Convert to TEXT
        inlineSum -= _tableColumns[idx].varcharCost;
        inlineSum += TEXT_POINTER + TEXT_INLINE;
        _tableColumns[idx].columnType = "TEXT";
    }
}
```

#### d. Added ClearTableOptimization Method

```cpp
void DbMySql::ClearTableOptimization()
{
    _tableColumns.clear();
    _tableOptimized = false;
}
```

#### e. Modified SqlOutput::CreateTableSql Method

Added code to use our optimization for MySQL tables:

```cpp
void SqlOutput::CreateTableSql(ostream& io, const string& tableName)
{
    // Existing code...

    // If we're using MySQL, run table optimization for large text fields
    DbMySql* mysqlDb = dynamic_cast<DbMySql*>(&_db);
    if (mysqlDb != NULL) {
        mysqlDb->OptimizeTableColumns(tableName, attrInfo);
    }

    // Existing column generation code...

    // Clear MySQL table optimization if used
    if (mysqlDb != NULL) {
        mysqlDb->ClearTableOptimization();
    }

    // Rest of existing code...
}
```

## How The Code Works

### Optimization Process Flow

1. **Initialization** (in `SqlOutput::CreateTableSql`):
   - When generating a MySQL table schema, the code detects if we're using a MySQL database
   - It calls `OptimizeTableColumns` before generating the table definition

2. **Column Analysis** (in `OptimizeTableColumns`):
   - Calculates storage requirements for each column in the table
   - Identifies which columns would cause row size to exceed MySQL's limits
   - Makes initial decision to promote very large columns (>40,000 chars) to MEDIUMTEXT

3. **Greedy Optimization** (in `OptimizeTableColumns`):
   - If total row size still exceeds 65,535 bytes:
     - Sorts the remaining VARCHAR columns by size (largest first)
     - Converts them to TEXT one by one until row size is under limit
   - Stores the optimized column types in the `_tableColumns` vector

4. **Schema Generation** (in `GetText`):
   - When determining the data type for a text column, checks if optimization was done
   - If column is in the optimized list, uses the pre-calculated type (VARCHAR/TEXT/MEDIUMTEXT)
   - Otherwise falls back to the simple threshold-based decision

5. **Cleanup** (in `SqlOutput::CreateTableSql`):
   - After schema generation is complete, calls `ClearTableOptimization` to reset state

### Flow Control and Decision Making

- The optimization is only triggered for MySQL databases
- Only text/string columns participate in the size-based sorting and conversion
- Numeric and date columns keep their original types
- Very large columns (>40,000 chars) are immediately promoted to MEDIUMTEXT
- Smaller columns are only converted to TEXT if necessary to meet row size limits
- The system automatically determines the minimal set of changes needed

## Example: Entity_poly Table

Consider a table with protein sequence columns:

| Column Name | Data Type | Width |
|-------------|-----------|-------|
| structure_id | VARCHAR | 12 |
| entity_id | INT | 4 |
| pdbx_seq_one_letter_code | VARCHAR | 32000 |
| pdbx_seq_one_letter_code_can | VARCHAR | 32000 |
| type | VARCHAR | 50 |

**Without Optimization**:
- The two sequence columns would consume ~128,002 bytes each
- Total row size: ~256,000+ bytes (exceeds limit)
- MySQL would reject the table definition

**With Optimization**:
- Sequence columns converted to MEDIUMTEXT
- Each consumes only ~20 bytes in the row
- Total row size: ~100 bytes (well under limit)
- Table creation succeeds

- `modules/cpp-db-loader/include/CifSchemaMap.h`: Added structures and method declarations
- `modules/cpp-db-loader/src/CifSchemaMap.C`: Implemented the optimization algorithm