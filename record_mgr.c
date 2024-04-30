#include <stdlib.h>
#include <string.h>

#include "tables.h"

#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "record_mgr.h"

// Macros for defining primitive datatypes
#define INT_SIZE sizeof(int)
#define FLOAT_SIZE sizeof(float)
#define BOOL_SIZE sizeof(bool)
#define CHAR_SIZE sizeof(char)

// Data structure to handle Table related information and operations
typedef struct TD_info
{
    int recordSize;   // size of the record
    int totalRecords; // total number of tuples/records in a table
    int blkFctr;      // Block factor to identify if storage utilization is efficient

    RID freeSpace;             // location in the page where space is available
    RM_TableData *rm_tbl_data; // Management structure for a Record Manager to handle one relation
    BM_PageHandle pageHandle;  // pagehandle of buffermanager
    BM_BufferPool bufferPool;  // bufferpool of buffermanager
} TD_info;

// Data structure to handle Scan related information and operations
typedef struct RM_scanmgr
{
    Expr *theta; // select condition of the record
    int count;   // total records to be scanned

    RID rid;
    RM_TableData *rm_tbl_data;
    BM_PageHandle rm_pageHandle;
    BM_BufferPool rm_bufferPool;
} RM_scanmgr;

// Defining Global variables
SM_FileHandle sm_fileHandle;
SM_PageHandle sm_pageHandle;
TD_info td_info;
RM_scanmgr rm_scanmgr;

/*
====================================================================
====================================================================
==================== AUXILARY FUNCTION DECLARATION ===================
====================================================================
====================================================================
*/

void parsePageFileSchema(RM_TableData *, BM_PageHandle *);

char *parseSchemaName(char *);
int fetchTotalAttributes(char *);
char *parseAttributeMetadata(char *);
int parseTotalKeyAttributes(char *);
char *parseAttributeKeyData(char *);
char *locateFreePageSlotData(char *);
char **extractAttributeNames(char *, int);
int *extractAttributeDataType(char *, int);
int *extractAttributeSize(char *, int);
int *extractKeyData(char *data, int keyNum);
int *extractFirstFreePageSlot(char *);
int extractTotalRecords(char *);

char *extractSingleAttributeData(char *, int);
char *extractName(char *);
int extractTypeLength(char *data);
int *extractAttributeSize(char *schemaData, int numAtr);
int extractDataType(char *);
int getAttributeRecordOffset(Schema *, int);

/*
====================================================================
====================================================================
======================= AUXILARY FUNCTIONS  ========================
====================================================================
====================================================================
*/


RC writeStrToPage(char *name, int pageNum, char *str) {

	RC result = RC_OK;
	result = createPageFile(name);
	if (result != RC_OK) {
		return result;
	}
	result = openPageFile(name, &sm_fileHandle);
	if (result != RC_OK) {
		return result;
	}
	// Page 0 include schema and relative table message
	result = writeBlock(pageNum, &sm_fileHandle, str);
	if (result != RC_OK) {
		return result;
	}
	return closePageFile(&sm_fileHandle);
}

// parsePageFileSchema method parse the data calles to further method to read ans parse next record
void parsePageFileSchema(RM_TableData *rel, BM_PageHandle *h) // input parameters are schema details and pointer to page data
{
    // Allocate memory to load schema information
    char meta[PAGE_SIZE];
    strcpy(meta, h->data);

    // parse Relation name
    char *schema_name = parseSchemaName(meta);

    // fetch total number of attributes
    int totalAttr = fetchTotalAttributes(meta);

    // fetch all attributes
    char *atrMetadata = parseAttributeMetadata(meta);

    // fetch total number of key attributes
    int totalKeyAtr = parseTotalKeyAttributes(meta);

    // fetch key Attribute names
    char *atrKeydt = parseAttributeKeyData(meta);

    // Locate vacant slot in page (parse)
    char *freeVacSlot = locateFreePageSlotData(meta);

    // parse attribute name
    char **names = extractAttributeNames(atrMetadata, totalAttr);

    // parse attribute datatype
    DataType *dt = (DataType *)extractAttributeDataType(atrMetadata, totalAttr);

    // parse attribute datatype size
    int *sizes = extractAttributeSize(atrMetadata, totalAttr);

    // fetch indices of key attributes
    int *keys = extractKeyData(atrKeydt, totalKeyAtr);

    // fetch vacant page and slot location
    int *pageSlot = extractFirstFreePageSlot(freeVacSlot);

    // fetch total number of tuples in a table
    int totaltuples = extractTotalRecords(meta);

    int i = 0;
    char **cNames = (char **)malloc(sizeof(char *) * totalAttr);
    DataType *cDt = (DataType *)malloc(sizeof(DataType) * totalAttr);
    int *cSizes = (int *)calloc(INT_SIZE, totalAttr);
    int *cKeys = (int *)calloc(INT_SIZE, totalKeyAtr);
    char *cSchemaName = (char *)calloc(CHAR_SIZE, 20);

    for (; i < totalAttr;)
    {
        cNames[i] = (char *)calloc(CHAR_SIZE, 10);
        strcpy(cNames[i], names[i]);
        i++;
    }
    // copy data from variable which was read from page files to schema
    memcpy(cDt, dt, sizeof(DataType) * totalAttr);
    memcpy(cSizes, sizes, INT_SIZE * totalAttr);
    memcpy(cKeys, keys, INT_SIZE * totalKeyAtr);
    memcpy(cSchemaName, schema_name, strlen(schema_name));

    // deallocate memory
    free(names);
    free(dt);
    free(sizes);
    free(keys);
    free(schema_name);

    // creating schema from data which is read from files
    Schema *schema = createSchema(totalAttr, cNames, cDt, cSizes, totalKeyAtr, cKeys);
    rel->schema = schema;
    rel->name = cSchemaName;

    td_info.rm_tbl_data = rel;
    td_info.recordSize = getRecordSize(rel->schema) + 1; //
    td_info.blkFctr = (PAGE_SIZE / td_info.recordSize);
    td_info.freeSpace.page = pageSlot[0];
    td_info.freeSpace.slot = pageSlot[1];
    td_info.totalRecords = totaltuples;
}

// parse schema name from page file on disk
char *parseSchemaName(char *schemaData)
{
    int i = 0;
    char *relationName = (char *)calloc(CHAR_SIZE, 20);
    for (; schemaData[i] != '|';)
    {
        relationName[i] = schemaData[i];
        i++;
    }
    relationName[i] = '\0';
    return relationName;
}

// parse metadata of attribute from page file on disk
char *parseAttributeMetadata(char *schemaData)
{
    int i = 0, j = 0;
    char *attrData = (char *)calloc(CHAR_SIZE, 100);
    for (; schemaData[i] != '[';)
        i++;
    i++;
    for (; schemaData[i] != ']';)
    {
        attrData[j] = schemaData[i++];
        j++;
    }
    attrData[j] = '\0';

    return attrData;
}

// parse metadata of Key Attributes from page file on disk
char *parseAttributeKeyData(char *schemaData)
{
    int i = 0, j = 0;
    char *attrData = (char *)calloc(CHAR_SIZE, 50);
    for (; schemaData[i] != '{';)
        i++;
    i++;
    for (; schemaData[i] != '}';)
    {
        attrData[j] = schemaData[i++];
        j++;
    }
    attrData[j] = '\0';

    return attrData;
}

// locate free page slot from page file on disk
char *locateFreePageSlotData(char *schemaData)
{
    int i = 0, j = 0;
    char *attrData = (char *)calloc(CHAR_SIZE, 50);
    for (; schemaData[i] != '$';)
        i++;
    i++;
    for (; schemaData[i] != '$';)
    {
        attrData[j] = schemaData[i++];
        j++;
    }
    attrData[j] = '\0';

    return attrData; // return the data of free page slot which was read from page file
}

// extract names of attributes from page file on disk
char **extractAttributeNames(char *schemaData, int numAtr)
{
    int i = 0;
    char **attrNames = (char **)calloc(CHAR_SIZE, numAtr);
    for (; i < numAtr;)
    {
        char *atrDt = extractSingleAttributeData(schemaData, i);
        char *name = extractName(atrDt);
        attrNames[i] = calloc(CHAR_SIZE, strlen(name));
        strcpy(attrNames[i], name);
        free(name);
        free(atrDt);
        i++;
    }
    return attrNames;
}

// extract size of attribute from page file on disk
int *extractAttributeSize(char *schemaData, int numAtr)
{
    int i = 0;
    int *attrSize = (int *)calloc(INT_SIZE, numAtr);

    for (; i < numAtr;)
    {
        char *atrDt = extractSingleAttributeData(schemaData, i);
        attrSize[i] = extractTypeLength(atrDt);
        free(atrDt);
        i++;
    }

    return attrSize;
}

// extract metadata of Single Attribute from page file on disk
char *extractSingleAttributeData(char *schemaData, int atrNum)
{
    int i = 0, j = 0, count = 0;
    char *attrData = (char *)calloc(CHAR_SIZE, 30);
    for (; count <= atrNum;)
    {
        if (schemaData[i++] == '(')
            count++;
    }

    for (; schemaData[i] != ')';)
    {
        attrData[j] = schemaData[i++];
        j++;
    }
    attrData[j] = '\0';
    return attrData;
}

// extract Attribute/Key Attribute name
char *extractName(char *data)
{
    int i = 0;
    char *name = (char *)calloc(CHAR_SIZE, 10);
    for (; data[i] != ':';)
    {
        name[i] = data[i];
        i++;
    }
    name[i] = '\0';
    return name;
}

//  extract data type
int extractDataType(char *data)
{
    int i = 0, j = 0;
    char *dtType = (char *)calloc(INT_SIZE, 2);
    for (; data[i] != ':';)
        i++;
    i++;
    for (; data[i] != '~';)
        dtType[j++] = data[i++];

    dtType[j] = '\0';
    int dt = atoi(dtType);
    free(dtType);
    return dt;
}

// extract size of datatype
int extractTypeLength(char *data)
{
    int i = 0, j = 0;
    char *dtTypeLen = (char *)calloc(INT_SIZE, 2);
    for (; data[i] != '~';)
        i++;
    i++;
    for (; data[i] != '\0';)
        dtTypeLen[j++] = data[i++];
    dtTypeLen[j] = '\0';
    int dt = atoi(dtTypeLen);
    free(dtTypeLen);
    return dt;
}

// function to extract data of key attributes
int *extractKeyData(char *data, int keyNum)
{
    int i = 0, j = 0, k = 0;
    char *val = (char *)calloc(INT_SIZE, 2);
    int *values = (int *)calloc(INT_SIZE, keyNum);

    for (; data[k] != '\0';)
    {
        if (data[k] != ':')
            val[i++] = data[k++];
        else
        {
            values[j++] = atoi(val);
            memset(val, '\0', sizeof(int) * 2);
            i = 0;
            k++;
        }
    }

    values[keyNum - 1] = atoi(val);
    return values;
}

// extract first free page slot from page file on disk
int *extractFirstFreePageSlot(char *data)
{
    int i = 0, j = 0, k = 0;
    char *val = (char *)calloc(INT_SIZE, 2);
    int *values = (int *)calloc(INT_SIZE, 2);
    for (; data[k] != '\0';)
    {
        if (data[k] != ':')
            val[i++] = data[k++];
        else
        {
            values[j++] = atoi(val);
            memset(val, '\0', sizeof(int) * 2);
            i = 0;
            k++;
        }
    }
    values[1] = atoi(val);
    printf("\n Slot %d", values[1]);
    return values;
}

// calculate offset of particular attribute
int getAttributeRecordOffset(Schema *schema, int atrnum)
{
    int offset = 0, pos = 0, dt = (*schema).dataTypes[pos];
    for (; pos < atrnum; pos++)
    {
        if (dt == DT_INT)
        {
            offset = offset + INT_SIZE;
            continue;
        }
        if (dt == DT_STRING)
        {
            offset = offset + (CHAR_SIZE * schema->typeLength[pos]);
            continue;
        }
        if (dt == DT_FLOAT)
        {
            offset = offset + FLOAT_SIZE;
            continue;
        }
        if (dt == DT_BOOL)
        {
            offset = offset + BOOL_SIZE;
            continue;
        }
        pos++;
    }

    return offset;
}

/*
====================================================================
====================================================================
==================== RECORD MANAGER FUNCTIONS ======================
====================================================================
====================================================================
*/

// Initializing record manager
RC initRecordManager(void *mgmtData)
{
    initStorageManager();
    return RC_OK;
}

// Shutting down the record manager
RC shutdownRecordManager()
{
    if (!sm_pageHandle)
        return RC_OK;

    free(sm_pageHandle);
    return RC_OK;
}

// Creates a new Table
// name: Name of the table
// schema: holds crucial Information about the schema
RC createTable(char *name, Schema *schema)
{

    RC code = RC_OK;
    int i = 0;

    // Create an empty Page File on disk
    if (code = createPageFile(name) != RC_OK)
        return code;

    // Allocating memory to hold metadata information about the schema
    char *meta = (char *)calloc(PAGE_SIZE, 1);
    sm_pageHandle = (SM_PageHandle)malloc(PAGE_SIZE);

    // Storing name of relation
    sprintf(meta, "%s|", name);

    // Appending number of attributes of relation
    sprintf(meta + strlen(meta), "%d[", (*schema).numAttr);

    // Appending name, datatype and size of attributes of relation
    for (; i < (*schema).numAttr;)
    {
        sprintf(meta + strlen(meta), "(%s:%d~%d)", (*schema).attrNames[i], (*schema).dataTypes[i], (*schema).typeLength[i]);
        i++;
    }

    // Appending Key Attribute size
    sprintf(meta + strlen(meta), "]%d{", schema->keySize);
    for (i = 0; i < schema->keySize;)
    {
        sprintf(meta + strlen(meta), "%d", schema->keyAttrs[i]);
        if (i < (schema->keySize - 1))
            strcat(meta, ":");
        i++;
    }

    strcat(meta, "}");

    // Initializing free page-slot to 1 and 0 respectively
    td_info.freeSpace.page = 1;
    td_info.freeSpace.slot = 0;

    // Initializing total number of tuples in relation to 0
    td_info.totalRecords = 0;

    // Appending vacant page-slot location
    sprintf(meta + strlen(meta), "$%d:%d$", td_info.freeSpace.page, td_info.freeSpace.slot);

    // Appending total number of tuples in relation
    sprintf(meta + strlen(meta), "?%d?", td_info.totalRecords);

    memmove(sm_pageHandle, meta, PAGE_SIZE);

    // Writing schema information to the first pageFile on disk (page 0)
    if ((code = openPageFile(name, &sm_fileHandle) != RC_OK) || (code = writeBlock(0, &sm_fileHandle, sm_pageHandle) != RC_OK))
        return code;

    free(sm_pageHandle);
    return RC_OK;
}

// Open an existing table to perform operations such as scanning, inserting or deleting records
// rel: Table Data manager
// name: name of relation.table to open
RC openTable(RM_TableData *rel, char *name)
{
    RC code = RC_OK;
    sm_pageHandle = (SM_PageHandle)calloc(PAGE_SIZE, 1);

    BM_PageHandle *h = &td_info.pageHandle;
    BM_BufferPool *bm = &td_info.bufferPool;

    // we pin page 0 and read data from page 0 using buffer manager

    // Initializing bufferpool to load schema information from pagefile on disk
    if (code = initBufferPool(bm, name, 3, RS_FIFO, NULL) != RC_OK)
        return code;

    // Page 0 on pagefile has been reserved to store metadata of schema
    if (code = pinPage(bm, h, 0) != RC_OK)
        return code;

    // Parsing the metadata of schema stored in the 1st pagefile on disk
    parsePageFileSchema(rel, h);
    if (code = unpinPage(bm, h) != RC_OK)
        return code;
    return code;
}

// Closes an open table
// Any changes made to the table will be written to the pagefile on disk
RC closeTable(RM_TableData *rel)
{
    RC code = RC_OK;
    int i = 0, recordSize = 0;
    char *meta = (char *)calloc(PAGE_SIZE, 1);
    BM_PageHandle *page = &td_info.pageHandle;
    BM_BufferPool *bm = &td_info.bufferPool;
    char *pageData;

    sprintf(meta, "%s|", (*rel).name);
    recordSize = td_info.recordSize;
    sprintf(meta + strlen(meta), "%d[", rel->schema->numAttr);
    for (; i < rel->schema->numAttr;)
    {
        sprintf(meta + strlen(meta), "(%s:%d~%d)", rel->schema->attrNames[i], rel->schema->dataTypes[i], rel->schema->typeLength[i]);
        i++;
    }
    sprintf(meta + strlen(meta), "]%d{", rel->schema->keySize);
    i = 0;
    for (; i < rel->schema->keySize;)
    {
        sprintf(meta + strlen(meta), "%d", rel->schema->keyAttrs[i]);
        if ((rel->schema->keySize - 1) > i)
            strcat(meta, ":");
        i++;
    }

    strcat(meta, "}");
    sprintf(meta + strlen(meta), "$%d:%d$", td_info.freeSpace.page, td_info.freeSpace.slot);
    sprintf(meta + strlen(meta), "?%d?", td_info.totalRecords);
    if (code = pinPage(bm, page, 0) != RC_OK)
        return code;
    memmove(page->data, meta, PAGE_SIZE);
    if (code = markDirty(bm, page) != RC_OK)
        return code;

    if (code = unpinPage(bm, page) != RC_OK)
        return code;

    if (code = shutdownBufferPool(bm) != RC_OK)
        return code;

    return code;
}

// Delete an existing table
// name: name of the relation to be deleted
RC deleteTable(char *name)
{
    RC code = RC_OK;
    BM_BufferPool *bm = &td_info.bufferPool;

    if (name == ((char *)0))
        return RC_NULL_IP_PARAM;

    if (code = destroyPageFile(name) != RC_OK)
        return code;
    return code;
}

// returns the total numbers of records in table
int getNumTuples(RM_TableData *rel)
{
    return (int)td_info.totalRecords;
}

// inserts the record passed in input parameter at avialable page and slot
RC insertRecord(RM_TableData *rel, Record *record)
{
    RC code = RC_OK;
    BM_PageHandle *page = &td_info.pageHandle;
    BM_BufferPool *bm = &td_info.bufferPool;
    char *pageData;
    int recordSize, freePageNum, freeSlotNum, blockfactor;

    recordSize = td_info.recordSize;
    freePageNum = td_info.freeSpace.page; // record will be inserted at this page number
    freeSlotNum = td_info.freeSpace.slot; // record will be inserted at this slot
    blockfactor = td_info.blkFctr;
    bool verify = (freePageNum < 1 || freeSlotNum < 0);

    if (freePageNum < 1 || freeSlotNum < 0)
        return RC_INVALID_PAGE_SLOT_NUM;

    if (code = pinPage(bm, page, freePageNum) != RC_OK)
        return code;
    pageData = (*page).data;

    (*record).data[recordSize - 1] = '$';
    memcpy(pageData + freeSlotNum * recordSize, record->data, recordSize);

    if (code = markDirty(bm, page) != RC_OK)
        return code;

    if (code = unpinPage(bm, page) != RC_OK)
        return code;

    (*record).id.page = freePageNum; // storing page number for record
    (*record).id.slot = freeSlotNum; // storing slot number for record

    // updating total number of records in a table
    td_info.totalRecords = td_info.totalRecords + 1;

    // updating next available page and slot after record inserted into file
    if (freeSlotNum == (blockfactor - 1))
    {
        td_info.freeSpace.page = freePageNum + 1;
        td_info.freeSpace.slot = 0;
    }
    else
        td_info.freeSpace.slot = freeSlotNum + 1;
    return code;
}

// Delete a record from a relation
// id: record id to be deleted
RC deleteRecord(RM_TableData *rel, RID id)
{
    RC code = RC_OK;
    BM_PageHandle *page = &td_info.pageHandle;
    BM_BufferPool *bm = &td_info.bufferPool;
    int recordSize, recordPageNumber, recordSlotNumber, blockfactor;

    recordSize = td_info.recordSize;
    blockfactor = td_info.blkFctr;
    recordPageNumber = id.page; // record will be searched at this page number
    recordSlotNumber = id.slot; // record will be searched at this slot

    if (code = pinPage(bm, page, recordPageNumber) != RC_OK)
        return code;

    memset((*page).data + recordSlotNumber * recordSize, '\0', recordSize);

    td_info.totalRecords = td_info.totalRecords - 1; // updating total number of record by after deleting record

    if (code = markDirty(bm, page) != RC_OK)
        return code;

    if (code = unpinPage(bm, page) != RC_OK)
        return code;
    return code;
}

// updates a record in a relation
// record: record to be updated
RC updateRecord(RM_TableData *rel, Record *record)
{
    RC code = RC_OK;
    BM_PageHandle *page = &td_info.pageHandle;
    BM_BufferPool *bm = &td_info.bufferPool;
    int recordSize, recordPageNumber, recordSlotNumber, blockfactor, recordOffet;

    recordSize = td_info.recordSize;
    blockfactor = td_info.blkFctr;
    recordPageNumber = (*record).id.page; // record will be searched at this page number
    recordSlotNumber = (*record).id.slot; // record will be searched at this slot

    if (code = pinPage(bm, page, recordPageNumber) != RC_OK)
        return code;

    memcpy(page->data + recordSlotNumber * recordSize, record->data, recordSize - 1);
    if (code = markDirty(bm, page) != RC_OK)
        return code;
    if (code = unpinPage(bm, page) != RC_OK)
        return code;
    return code;
}

// Fetch record from a relation
// id: Record id to fetch
// record: pointer to record
RC getRecord(RM_TableData *rel, RID id, Record *record)
{
    RC code = RC_OK;
    BM_PageHandle *page = &td_info.pageHandle;
    BM_BufferPool *bm = &td_info.bufferPool;
    int recordSize, recordPageNumber, recordSlotNumber, blockfactor, recordOffet;

    recordSize = td_info.recordSize;
    blockfactor = td_info.blkFctr;
    recordPageNumber = id.page; // record will be searched at this page number
    recordSlotNumber = id.slot; // record will be searched at this slot

    if (code = pinPage(bm, page, recordPageNumber) != RC_OK)
        return code;

    recordOffet = recordSlotNumber * recordSize;                    // it gives starting point of record
    memcpy((*record).data, (*page).data + recordOffet, recordSize); // copy data from page file to record data. also checks boundry thetaition for reccord->data size

    (*record).data[recordSize - 1] = '\0';
    (*record).id.page = recordPageNumber;
    (*record).id.slot = recordSlotNumber;
    if (code = unpinPage(bm, page) != RC_OK)
        return code;

    return code;
}

// retrieve all tuples from a table that fulfill a certain condition
RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *theta) 
{
    BM_BufferPool *bm = &td_info.bufferPool;

    (*scan).rel = rel;
    rm_scanmgr.theta = theta;
    rm_scanmgr.rid.page = 1; // records starts from page 1
    rm_scanmgr.rid.slot = 0; // slot starts from 0
    rm_scanmgr.count = 0;

    (*scan).mgmtData = &rm_scanmgr;

    return RC_OK;
}

// next method should return the next tuple that fulfills the scan condition.
RC next(RM_ScanHandle *scan, Record *record)
{
    // check condition for no more tuple available in table
    RC code = RC_OK;

    if ((td_info.totalRecords < 1 || rm_scanmgr.count == td_info.totalRecords))
        return RC_RM_NO_MORE_TUPLES;

    BM_PageHandle *page = &td_info.pageHandle;
    BM_BufferPool *bm = &td_info.bufferPool;
    int blockfactor, totalTuple, curTotalRecScan, curPgScan, curSlotScan;
    blockfactor = td_info.blkFctr;
    totalTuple = td_info.totalRecords;

    curTotalRecScan = rm_scanmgr.count; // scanning start from current count to total no of records
    curPgScan = rm_scanmgr.rid.page;    // scanning will start from current page till record from last page encountered
    curSlotScan = rm_scanmgr.rid.slot;  // scanning will start from current slot till record encountered

    Value *queryExpResult = (Value *)malloc(sizeof(Value));
    rm_scanmgr.count = rm_scanmgr.count + 1;

    // Obtain next tuple from relation
    for (; curTotalRecScan < totalTuple;)
    {
        rm_scanmgr.rid.page = curPgScan;
        rm_scanmgr.rid.slot = curSlotScan;

        if (code = getRecord(scan->rel, rm_scanmgr.rid, record) != RC_OK)
            return code;
        curTotalRecScan++; // increment record scan counter by 1

        if (rm_scanmgr.theta == NULL)
            queryExpResult->v.boolV = TRUE; // if no condition is mentioned then it will return all records

        else
        {
            evalExpr(record, (scan->rel)->schema, rm_scanmgr.theta, &queryExpResult);
            if ((*queryExpResult).v.boolV == 1)
            {
                (*record).id.page = curPgScan;
                (*record).id.slot = curSlotScan;
                curSlotScan == (blockfactor - 1) ? (curSlotScan = 0) : (curSlotScan = curSlotScan + 1);
                if (curSlotScan == (blockfactor - 1))
                    curPgScan = curPgScan + 1;
                rm_scanmgr.rid.page = curPgScan;
                rm_scanmgr.rid.slot = curSlotScan;
                return code;
            }
        }

        if (curSlotScan == (blockfactor - 1))
            curPgScan = curPgScan + 1;
        curSlotScan == (blockfactor - 1) ? (curSlotScan = 0) : (curSlotScan = curSlotScan + 1);
    }

    (*queryExpResult).v.boolV = TRUE;
    rm_scanmgr.rid.page = 1; // records starts from page 1
    rm_scanmgr.rid.slot = 0; // slot starts from 0
    rm_scanmgr.count = 0;
    return RC_RM_NO_MORE_TUPLES;
}

// terminate scan and reset variables
RC closeScan(RM_ScanHandle *scan)
{
    rm_scanmgr.rid.page = 1; // records starts from page 1
    rm_scanmgr.rid.slot = 0; // slot starts from 0
    rm_scanmgr.count = 0;
    return RC_OK;
}

// this function will return the size of record based on length of each field and data type
int getRecordSize(Schema *schema)
{
    if (schema == ((Schema *)0))
        return RC_SCHEMA_NOT_INIT;

    int i = 0, recordSize = 0;
    for (; i < schema->numAttr;)
    {
        if ((*schema).dataTypes[i] == DT_FLOAT)
            recordSize = recordSize + sizeof(float);
        if ((*schema).dataTypes[i] == DT_BOOL)
            recordSize = recordSize + sizeof(bool);
        if ((*schema).dataTypes[i] == DT_INT)
            recordSize = recordSize + sizeof(int);
        if ((*schema).dataTypes[i] == DT_STRING)
            recordSize = recordSize + (sizeof(char) * schema->typeLength[i]);
        i++;
    }

    return recordSize;
}

// Create a new schema
Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{

    Schema *schema = (Schema *)malloc(sizeof(Schema)); // allocate memory to schema

    if (schema != NULL)

    {
        // set the parameter of new schema based on the input parameters
        (*schema).numAttr = numAttr;
        (*schema).attrNames = attrNames;
        (*schema).dataTypes = dataTypes;
        (*schema).typeLength = typeLength;
        (*schema).keySize = keySize;
        (*schema).keyAttrs = keys;
        td_info.recordSize = getRecordSize(schema);
        td_info.totalRecords = 0;

        return schema; // returns the schema
    }
}

// this function will free the memory which was allocated to schema
RC freeSchema(Schema *schema)
{
    free(schema);
    return RC_OK;
}


// createRecord function is used to create a new record with all the null values
RC createRecord(Record **record, Schema *schema) // input parameters are pointer to newly created record and all attribues of schema
{

    Record *newTuple = (Record *)calloc(sizeof(Record), 1); // allocating memory for new record
    if (newTuple == NULL)
        return RC_MELLOC_MEM_ALLOC_FAILED;
    (*newTuple).data = (char *)calloc(sizeof(char), td_info.recordSize);

    (*newTuple).id.page = -1; // set to -1 bcz it has not inserted into table/page/slot

    *record = newTuple; // assignning a new record
    return RC_OK;
}

// freeRecord function will free the memory related to record
RC freeRecord(Record *record)
{
    if (record == NULL)
        return RC_NULL_IP_PARAM;

    if ((*record).data != ((char *)0))
        free((*record).data); // free the record data
    free(record);             // free the space of record
    return RC_OK;
}

// getAttr function returns the value of attribute pointed by atttrnum
RC getAttr(Record *record, Schema *schema, int attrNum, Value **value)
{
    int attrInt, dt;
    float attrFloat;
    char *sub_string, *appliedOffset;

    dt = (*schema).dataTypes[attrNum];
    appliedOffset = (*record).data + getAttributeRecordOffset(schema, attrNum);

    if (dt == DT_FLOAT)
    {
        sub_string = calloc(FLOAT_SIZE + 1, 1);        // one extra byte to store '\0' char
        memcpy(sub_string, appliedOffset, FLOAT_SIZE); // copy data from record data to sub_string
        sub_string[FLOAT_SIZE] = '\0';                 // set last byet to '\0'
        attrFloat = atof(sub_string);                  // converts the string argument sub_string to a float
        MAKE_VALUE(*value, DT_FLOAT, attrFloat);
        free(sub_string);
        return RC_OK;
    }

    if (dt == DT_BOOL)
    {
        sub_string = calloc(BOOL_SIZE + 1, 1);        // one extra byte to store '\0' char
        memcpy(sub_string, appliedOffset, BOOL_SIZE); // copy data from record data to sub_string
        sub_string[BOOL_SIZE] = '\0';                 // set last byet to '\0'
        attrInt = atoi(sub_string);                   // converts the string argument sub_string to an integer
        MAKE_VALUE(*value, DT_BOOL, attrInt);
        free(sub_string);
        return RC_OK;
    }

    if (dt == DT_INT)
    {
        sub_string = calloc(INT_SIZE + 1, 1);        // one extra byte to store '\0' char
        memcpy(sub_string, appliedOffset, INT_SIZE); // copy data from record data to sub_string
        sub_string[INT_SIZE] = '\0';                 // set last byet to '\0'
        attrInt = atoi(sub_string);                  // converts the string argument sub_string to an integer
        MAKE_VALUE(*value, DT_INT, attrInt);
        free(sub_string);
        return RC_OK;
    }

    if (dt == DT_STRING)
    {
        sub_string = calloc(CHAR_SIZE * (*schema).typeLength[attrNum] + 1, 1);        // one extra byte to store '\0' char
        memcpy(sub_string, appliedOffset, CHAR_SIZE * (*schema).typeLength[attrNum]); // copy data from record data to sub_string
        sub_string[CHAR_SIZE * (*schema).typeLength[attrNum]] = '\0';                 // set last byet to '\0'
        MAKE_STRING_VALUE(*value, sub_string);
        free(sub_string);
        return RC_OK;
    }

    return RC_FAILED;
}

// Convert integer value 'val' to string and update it in char buffer 'intStr'
void strRepInt(int j, int val, char *intStr)
{
    int i = 0, q = val, last = j;
    while (q > 0 && j >= 0)
    {
        i = q % 10;
        q = q / 10;
        intStr[j] = intStr[j] + i;
        j--;
    }
    intStr[last + 1] = '\0';
}

// setAttr functions will set value of particular attribute given in attrNum
RC setAttr(Record *record, Schema *schema, int attrNum, Value *value) // input parameters are pointers to the record data,schema attributes, attribute number whose value needs to be changed and new value of attribute
{

    int remainder = 0, quotient = 0, k = 0, j, number, dt;

    bool q, r;
    char *intStr = (char *)calloc(INT_SIZE, 1), *intStrTemp = (char *)calloc(INT_SIZE, 1);
    memset(intStr, '0', INT_SIZE);
    char *hexValue = "0001", *appliedOffset = (*record).data + getAttributeRecordOffset(schema, attrNum);
    number = (int)strtol(hexValue, NULL, 16);
    dt = (*schema).dataTypes[attrNum];

    if (dt == DT_FLOAT)
    {
        sprintf(appliedOffset, "%f", (*value).v.floatV);
        return RC_OK;
    }
    if (dt == DT_BOOL)
    {
        strRepInt(1, (*value).v.boolV, intStr);
        sprintf(appliedOffset, "%s", intStr);
        return RC_OK;
    }
    if (dt == DT_INT)
    {
        strRepInt(3, (*value).v.intV, intStr);
        sprintf(appliedOffset, "%s", intStr);
        return RC_OK;
    }
    if (dt == DT_STRING)
    {
        sprintf(appliedOffset, "%s", (*value).v.stringV);
        return RC_OK;
    }

    return RC_FAILED;
}

// function to extract total number of records from page
int extractTotalRecords(char *schemaData)
{
    int i = 0, j = 0;
    char *attrData = (char *)calloc(CHAR_SIZE, 20);

    for (; schemaData[i] != '?';)
        i++;

    i++;

    for (; schemaData[i] != '?';)
        attrData[j++] = schemaData[i++];

    attrData[j] = '\0';
    return atoi(attrData); // returns the total number of record from page
}

int fetchTotalAttributes(char *schemaData)
{
    char *attr = (char *)calloc(INT_SIZE, 2);
    int i = 0, j = 0;
    for (; schemaData[i] != '|';)
        i++;
    i++;
    for (; schemaData[i] != '[';)
    {
        attr[j++] = schemaData[i++];
    }
    attr[j] = '\0';
    return atoi(attr); // return string to integer converter of total attributes
}

// function to read total number of key attributes from page file

int parseTotalKeyAttributes(char *schemaData)
{
    char *attr = (char *)calloc(INT_SIZE, 2);
    int i = 0, j = 0;
    for (; schemaData[i] != ']';)
        i++;
    i++;
    for (; schemaData[i] != '{';)
        attr[j++] = schemaData[i++];
    attr[j] = '\0';
    return atoi(attr); // return string to integer converter of total number of key attributes
}

// function to read data type of attribute from page file
int *extractAttributeDataType(char *schemaData, int numAtr)
{
    int i = 0, *dt = (int *)calloc(INT_SIZE, numAtr);

    for (; i < numAtr;)
    {
        char *atrDt = extractSingleAttributeData(schemaData, i);
        dt[i++] = extractDataType(atrDt);
        free(atrDt);
    }
    return dt; // return data type of attribute which was read from page file
}

/*
====================================================================
====================================================================
======================= OPTIONAL EXTENSION  ========================
====================================================================
====================================================================
*/


// updateScan function will update the record based on the scan condition
RC updateScan(RM_TableData *rel, Record *record, Record *updaterecord, RM_ScanHandle *scan) // input parameters are scheme details, record data to be returned, pointer to the updated record,other informatoin related to scan
{
    RC code;
    while (1)
    {
        code = next(scan, record);
        if (code != RC_OK)
            break;
        updaterecord->id.page = record->id.page;
        updaterecord->id.slot = record->id.slot;
        updateRecord(rel, updaterecord);
    }
    return RC_OK;
}

// function to print record
void printRecord(char *record, int recordLength)
{
    int i = 0;
    for (; i < recordLength;)
        printf("%c", record[i++]);
}

// function to print Record deatils
void printTableInfoDetails(TD_info *tab_info)
{
    printf(" \n table name: %s", tab_info->rm_tbl_data->name);
    printf(" \n Size of record: %d", tab_info->recordSize);
    printf(" \n total Records in page (blkftr): %d", tab_info->blkFctr);
    printf(" \n total Attributes in table: %d", tab_info->rm_tbl_data->schema->numAttr);
    printf(" \n total Records in table: %d", tab_info->totalRecords);
    printf(" \n next available page and slot: %d:%d", tab_info->freeSpace.page, tab_info->freeSpace.slot);
}

// function to print data of page
void printPageData(char *pageData)
{
    printf("\n Prining page Data ==>");
    int i = 0;
    for (; i < PAGE_SIZE;)
        printf("%c", pageData[i++]);
    printf("\n end of print... ");
}
