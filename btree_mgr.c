#include "dberror.h"
#include "tables.h"
#include "btree_mgr.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "record_mgr.h"
#include "string.h"

#include <string.h>
#include <stdlib.h>
#include <math.h>


typedef enum NodePos
{
    INNER = 1,
    LEAF = 0
} NodePos;

typedef struct TreeNode
{
    NodePos node_pos;
    Value **keys; // array[0...n]
    // leaf-node => RID
    // non-leaf-node => next level node
    void **ptrs;    // array[0...n]
    int keys_count; // the count of key
    RID *records;
    struct TreeNode *sibling; // sibling
    struct TreeNode *parent;  // parent
} TreeNode;

typedef struct TreeMtdt
{
    int n; // maximum keys in each block
    int minLeaf;
    int minNonLeaf;
    int nodes;   // the count of node
    int entries; // the count of entries
    DataType keyType;

    TreeNode *root;

    BM_PageHandle *ph;
    BM_BufferPool *bm;
} TreeMtdt;

typedef struct Tree_ScanMtdt
{
    int keyIndex;
    TreeNode *node;
} Tree_ScanMtdt;

TreeMtdt *tMgmt ;
// init and shutdown index manager
RC initIndexManager(void *mgmtData)
{
    tMgmt=NULL;
    initStorageManager();
    return RC_OK;
}
#define MAKE_TREE_HANDLE() \
    ((BTreeHandle *)malloc(sizeof(BTreeHandle)))

#define MAKE_TREE_MTDT() \
    ((TreeMtdt *)malloc(sizeof(TreeMtdt)))

#define MAKE_TREE_NODE() \
    ((TreeNode *)malloc(sizeof(TreeNode)))

#define MAKE_TREE_SCAN() \
    ((BT_ScanHandle *)malloc(sizeof(BT_ScanHandle)))


RC shutdownIndexManager()
{
    shutdownRecordManager();
    return RC_OK;
}

/**
 * @brief serialize Btree header
 * memory layout: n  |  keyType  | nodes | entries
 *
 * @param mgmtData
 * @return char*
 */
char *
mgmtToHeader(TreeMtdt *tMgmt)
{
    char *header_Data = (char *)malloc(1 * PAGE_SIZE);
    int i = 1;
    *(int *)header_Data = (*tMgmt).n;
    *(int *)(header_Data + sizeof(int) * i++) = (*tMgmt).keyType;
    *(int *)(header_Data + sizeof(int) * i++) = (*tMgmt).nodes;
    *(int *)(header_Data + sizeof(int) * i) = (*tMgmt).entries;

    return header_Data;
}

/**
 * @brief deserialize Btree header_Data from file to metadata
 *
 * @param header_Data
 * @return TreeMtdt*
 */
TreeMtdt *
headerToMgmt(char *header_Data)
{
    int i = 1;
    tMgmt = MAKE_TREE_MTDT();
    (*tMgmt).n = *(int *)(header_Data);
    (*tMgmt).keyType = *(int *)(header_Data + sizeof(int) * i++);
    (*tMgmt).nodes = *(int *)(header_Data + sizeof(int) * i++);
    (*tMgmt).entries = *(int *)(header_Data + sizeof(int) * i);

    return tMgmt;
}

/**
 * @brief Create a Btree index file
 *
 * @param idxId
 * @param keyType
 * @param n
 * @return RC
 */
RC createBtree(char *idxId, DataType keyType, int n)
{
    RC result;
    tMgmt = MAKE_TREE_MTDT();
    (*tMgmt).keyType = keyType;
    (*tMgmt).n = n;
    (*tMgmt).entries = 0;
    (*tMgmt).nodes = 0;
    char *header_Data = mgmtToHeader(tMgmt);
    result = writeStrToPage(idxId, 0, header_Data);
    free(tMgmt);
    free(header_Data);
    return result;
}

/**
 * @brief Open a Btree index file
 *
 * @param tree
 * @param idxId
 * @return RC
 */
RC openBtree(BTreeHandle **tree, char *idxId)
{
    int offset = 0;
    *tree = MAKE_TREE_HANDLE();
    BM_BufferPool *bm = MAKE_POOL();
    BM_PageHandle *ph = MAKE_PAGE_HANDLE();
    BM_PageHandle *phHeader = MAKE_PAGE_HANDLE();
    initBufferPool(bm, idxId, 10, RS_LRU, NULL);
    pinPage(bm, phHeader, 0);
    tMgmt = headerToMgmt(phHeader->data);

    if ((*tMgmt).nodes == 0)
    {
        (*tMgmt).root = NULL;
    }
    (*tMgmt).minLeaf = ((*tMgmt).n + 1) / 2;
    (*tMgmt).minNonLeaf = ((*tMgmt).n + 2) / 2 - 1;
    (*tMgmt).ph = ph;
    (*tMgmt).bm = bm;
    (*tree)->keyType = (*tMgmt).keyType;
    (*tree)->mgmtData = tMgmt;
    (*tree)->idxId = idxId;
    free(phHeader);
    return RC_OK;
}

/**
 * @brief Close a Btree index file
 *
 * @param tree
 * @return RC
 */
RC closeBtree(BTreeHandle *tree)
{
    tMgmt = (TreeMtdt *)(*tree).mgmtData;

    shutdownBufferPool((*tMgmt).bm);
    free((*tMgmt).ph);
    free(tMgmt);
    free(tree);
    return RC_OK;
}

/**
 * @brief Delete a Btree index file
 *
 * @param idxId
 * @return RC
 */
RC deleteBtree(char *idxId)
{
    return destroyPageFile(idxId);
}

/**
 * @brief Get the Num Nodes object
 *
 * @param tree
 * @param result
 * @return RC
 */
RC getNumNodes(BTreeHandle *tree, int *result)
{
    tMgmt = (TreeMtdt *)(*tree).mgmtData;
    *result = (*tMgmt).nodes;
    return RC_OK;
}

/**
 * @brief Get the Num Entries object
 *
 * @param tree
 * @param result
 * @return RC
 */
RC getNumEntries(BTreeHandle *tree, int *result)
{
    tMgmt = (TreeMtdt *)(*tree).mgmtData;
    *result = (*tMgmt).entries;
    return RC_OK;
}

/**
 * @brief Get the Key Type object
 *
 * @param tree
 * @param result
 * @return RC
 */
RC getKeyType(BTreeHandle *tree, DataType *result)
{
   tMgmt = (TreeMtdt *)(*tree).mgmtData;
    *result = (*tMgmt).keyType;
    return RC_OK;
}

/**
 * @brief   if key > sign: 1;
 *          if key < sign: -1;
 *          if key == sign: 0;
 *          if key != sign: -1; // bool
 *
 * @param key
 * @param realkey
 * @return int
 */
int cmpVal(Value *key, Value *sign)
{
    int result;

    if ((*key).dt == DT_INT)
    {
        if ((*key).v.intV == sign->v.intV)
        {
            result = 0;
        }
        else
        {
            result = ((*key).v.intV > sign->v.intV) ? 1 : -1;
        }
        return result;
    }
    if ((*key).dt == DT_FLOAT)
    {
        if ((*key).v.floatV == sign->v.floatV)
        {
            result = 0;
        }
        else
        {
            result = ((*key).v.floatV > sign->v.floatV) ? 1 : -1;
        }
        return result;
    }
    if ((*key).dt == DT_STRING)
    {
        result = strcmp((*key).v.stringV, sign->v.stringV);
        return result;
    }
    if ((*key).dt == DT_BOOL)
    {
        result = ((*key).v.boolV == sign->v.boolV) ? 0 : -1;
        return result;
    }
    return result;
}

/**
 * @brief find leaf node
 *
 * @param node
 * @param key
 * @return TreeNode*
 */
TreeNode *searchLeafNode(TreeNode *node, Value *key)
{
    int i = 0;
    if ((*node).node_pos == LEAF)
        return node;

    for (; i < (*node).keys_count;)
    {

        if (cmpVal(key, (*node).keys[i]) < 0)
            return searchLeafNode((TreeNode *)(*node).ptrs[i], key);
        i++;
    }
    return searchLeafNode((TreeNode *)(*node).ptrs[(*node).keys_count], key);
}

/**
 * @brief find entry in node
 *
 * @param node
 * @param key
 * @return RID*
 */
RID *searchEntryInNode(TreeNode *node, Value *key)
{
    int i = 0;
    for (; i < (*node).keys_count;)
    {
        if (cmpVal(key, (*node).keys[i]) == 0)
            return (RID *)(*node).ptrs[i];
        i++;
    }
    return NULL;
}

/**
 * @brief find key
 *
 * @param tree
 * @param key
 * @param result
 * @return RC
 */
RC findKey(BTreeHandle *tree, Value *key, RID *result)
{
    tMgmt = (TreeMtdt *)(*tree).mgmtData;
    // 1. Find correct leaf node L for k
    TreeNode *leaf = searchLeafNode((*tMgmt).root, key);
    if (!leaf)
        return RC_IM_KEY_NOT_FOUND;

    // 2. Find the position of entry in a node, binary search
    RID *rec_ptr = searchEntryInNode(leaf, key);
    if (!rec_ptr)
        return RC_IM_KEY_NOT_FOUND;

    (*result) = (*rec_ptr);
    return RC_OK;
}

/**
 * @brief Create a Node object block
 *
 * @param mgmtData
 * @return TreeNode*
 */
TreeNode *
createNewNode(TreeMtdt *mgmtData)
{
    (*mgmtData).nodes++;
    TreeNode *node = MAKE_TREE_NODE();
    // insert first and then split
    (*node).keys = malloc(((*mgmtData).n + 1) * sizeof(Value *));
    (*node).keys_count = 0;
    (*node).sibling = NULL;
    (*node).parent = NULL;
    return node;
}

/**
 * @brief Create a Leaf Node object Block
 *
 * @param mgmtData
 * @return TreeNode*
 */
TreeNode *
createNewLeafNode(TreeMtdt *mgmtData)
{
    TreeNode *node = createNewNode(mgmtData);
    (*node).node_pos = LEAF;
    (*node).ptrs = (void *)malloc(((*mgmtData).n + 1) * sizeof(void *));
    return node;
}

/**
 * @brief Create a Non Leaf Node object
 *
 * @param mgmtData
 * @return TreeNode*
 */
TreeNode *
createNewNonLeafNode(TreeMtdt *mgmtData)
{
    TreeNode *node = createNewNode(mgmtData);
    (*node).node_pos = INNER;
    (*node).ptrs = (void *)malloc(((*mgmtData).n + 2) * sizeof(void *));
    return node;
}

/**
 * @brief Get the Insert Pos object
 *
 * @param node
 * @param key
 * @return int
 */
int fetchInsertPos(TreeNode *node, Value *key)
{
    int insert_pos = (*node).keys_count, i = 0;
    for (; i < (*node).keys_count;)
    {
        if (cmpVal((*node).keys[i], key) >= 0)
        {
            insert_pos = i;
            break;
        }
        i++;
    }
    return insert_pos;
}

/**
 * @brief build rid
 *
 * @param rid
 * @return RID*
 */
RID *RIDbuilder(RID *rid)
{
    RID *rec_ptr = (RID *)malloc(sizeof(RID));
    (*rec_ptr).page = (*rid).page;
    (*rec_ptr).slot = (*rid).slot;
    return rec_ptr;
}

/**
 * @brief insert entry into leaf node
 *
 * @param node
 * @param key
 * @param rid
 * @param mgmtData
 */
void insLeafNode(TreeNode *node, Value *key, RID *rid, TreeMtdt *mgmtData)
{
    int insert_pos = fetchInsertPos(node, key);
    int i = (*node).keys_count;
    for (; i >= insert_pos;)
    {
        (*node).keys[i] = (*node).keys[i - 1];
        (*node).ptrs[i] = (*node).ptrs[i - 1];
        i--;
    }
    (*node).keys[insert_pos] = key;
    (*node).ptrs[insert_pos] = RIDbuilder(rid);
    (*node).keys_count++;
    (*mgmtData).entries++;
}

/**
 * @brief split leaf node
 *
 * @param node
 * @param mgmtData
 */
TreeNode *
splitLeaf(TreeNode *node, TreeMtdt *mgmtData)
{
    TreeNode *n_node = createNewLeafNode(mgmtData);
    // split right index
    int i = ((*node).keys_count + 1) * 0.5;
    for (; i < (*node).keys_count; i++)
    {
        int index = (*node).keys_count - i - 1;
        (*n_node).keys[index] = (*node).keys[i];
        (*n_node).ptrs[index] = (*node).ptrs[i];
        (*node).keys[i] = NULL;
        (*node).ptrs[i] = NULL;
        (*n_node).keys_count++;
        (*node).keys_count--;
    }
    (*n_node).sibling = (*node).sibling;
    (*node).sibling = n_node;
    (*n_node).parent = (*node).parent;
    return n_node;
}

/**
 * @brief split non leaf node
 *
 * @param node
 * @param mgmtData
 */
void splitNonLeaf(TreeNode *node, TreeMtdt *mgmtData)
{
    TreeNode *sibling = createNewNonLeafNode(mgmtData);
    // split right index
    int mid = (*node).keys_count * 0.5;
    // 5, [1.2] 3 [4.5]
    // 4, [1.2] 3 [4]
    Value *pushKey = replicateKey((*node).keys[mid]);

    int index = 0, i = mid + 1;
    for (; i < (*node).keys_count;)
    {
        (*sibling).keys[index] = (*node).keys[i];
        (*sibling).ptrs[index + 1] = (*node).ptrs[i + 1];
        (*node).keys[i] = NULL;
        (*node).ptrs[i + 1] = NULL;
        (*sibling).keys_count++;
        (*node).keys_count--;
        index++;
        i++;
    }
    (*sibling).ptrs[0] = (*node).ptrs[mid + 1];
    (*node).ptrs[mid + 1] = NULL;
    (*node).keys_count--; // pushKey
    (*node).keys[mid] = NULL;

    (*sibling).parent = (*node).parent;
    (*node).sibling = sibling;

    insertKeyIntoParent(node, pushKey, mgmtData);
}

/**
 * @brief insert into parent node
 *
 * @param lnode
 * @param key
 * @param mgmtData
 */
void insertKeyIntoParent(TreeNode *lnode, Value *key, TreeMtdt *mgmtData)
{
    TreeNode *rnode = (*lnode).sibling;
    TreeNode *parent = (*lnode).parent;
    if (!parent)
    {
        parent = createNewNonLeafNode(mgmtData);
        (*mgmtData).root = (*lnode).parent = rnode->parent = parent;
        rnode->parent->ptrs[0] = lnode;
    }
    int insert_pos = fetchInsertPos(parent, key), i = (*parent).keys_count;

    for (; i > insert_pos;)
    {
        (*parent).keys[i] = (*parent).keys[i - 1];
        (*parent).ptrs[i + 1] = (*parent).ptrs[i];
        i--;
    }
    (*parent).keys[insert_pos] = key;
    (*parent).ptrs[insert_pos + 1] = rnode;
    (*parent).keys_count++;

    if ((*parent).keys_count > (*mgmtData).n)
        splitNonLeaf(parent, mgmtData);
}

Value *
replicateKey(Value *key)
{
    Value *newKey = (Value *)malloc(sizeof(Value));
    memmove(newKey, key, sizeof(*newKey));
    return newKey;
}

/**
 * @brief insert key to B+Tree
 *        bottom-up strategy
 * @param tree
 * @param key
 * @param rid
 * @return RC
 */
RC insertKey(BTreeHandle *tree, Value *key, RID rid)
{
    tMgmt = (TreeMtdt *)(*tree).mgmtData;
    if (!(*tMgmt).root)
        (*tMgmt).root = createNewLeafNode(tMgmt);

    // 1. Find correct leaf node L for k
    TreeNode *leafNode = searchLeafNode((*tMgmt).root, key);
    // this key is already stored in the b-tree
    if (searchEntryInNode(leafNode, key))
        return RC_IM_KEY_ALREADY_EXISTS;

    // 2. Add new entry into L in sorted order
    insLeafNode(leafNode, key, &rid, tMgmt);
    // If L has enough space, the operation done
    if (leafNode->keys_count <= (*tMgmt).n)
        return RC_OK;
    // Otherwise
    // Split L into two nodes L and L2
    TreeNode *rnode = splitLeaf(leafNode, tMgmt);
    // Redistribute entries evenly and copy up middle key (new leaf's smallest key)
    Value *newKey = replicateKey(rnode->keys[0]);
    // Insert index entry pointing to L2 into parent of L
    // To split an inner node, redistrubute entries evenly, but push up the middle key Resursive function
    insertKeyIntoParent(leafNode, newKey, tMgmt);
    return RC_OK;
}

/**
 * @brief delete from leaf node
 *
 * @param node
 * @param key
 * @param mgmtData
 */
void delKeyInLeafNode(TreeNode *node, Value *key, TreeMtdt *mgmtData)
{
    int i = fetchInsertPos(node, key);
    for (; i < (*node).keys_count;)
    {
        (*node).keys[i] = (*node).keys[i + 1];
        (*node).ptrs[i] = (*node).ptrs[i + 1];
        i++;
    }
    (*node).keys_count--;
    (*mgmtData).entries--;
}

/**
 * @brief checks for enough space
 *
 * @param keys_count
 * @param node
 * @param mgmtData
 * @return true
 * @return false
 */
bool isAvailableSpace(int keys_count, TreeNode *node, TreeMtdt *mgmtData)
{
    int min = (*node).node_pos;

    switch (min)
    {
    case LEAF:
        (*mgmtData).minLeaf;
        break;
    default:
        (*mgmtData).minNonLeaf;
    }

    if (keys_count >= (*mgmtData).minLeaf)
        if (keys_count <= (*mgmtData).n)
            return true;

    return false;
}

/**
 * @brief Deletes the parent entry
 *
 * @param node
 */
void deleteKeyinParent(TreeNode *node)
{
    TreeNode *parent = (*node).parent;
    int i = 0;
    for (; i <= (*parent).keys_count;)
    {
        if ((*parent).ptrs[i] == node)
        {
            int keyIndex = i - 1;
            if (keyIndex < 0)
                keyIndex = 0;
            (*parent).keys[keyIndex] = NULL;
            (*parent).ptrs[i] = NULL;
            for (int j = keyIndex; j < (*parent).keys_count - 1; j++)
            {
                (*parent).keys[j] = (*parent).keys[j + 1];
            }
            for (int j = i; j < (*parent).keys_count; j++)
            {
                (*parent).ptrs[j] = (*parent).ptrs[j + 1];
            }
            (*parent).keys_count--;
            break;
        }
        i++;
    }
}
/**
 * @brief Updates parent entry
 *
 * @param node INNER
 * @param key
 * @param newkey
 */
void updateKeyinParent(TreeNode *node, Value *key, Value *newkey)
{
    // equal = no update
    if (cmpVal(key, newkey) == 0)
        return;

    bool isDelete = false;
    TreeNode *parent = (*node).parent;
    // 1. delete key
    for (int i = 0; i < (*parent).keys_count; i++)
    {
        if (cmpVal(key, (*parent).keys[i]) == 0)
        {
            for (int j = i; j < (*parent).keys_count - 1; j++)
           
                (*parent).keys[j] = (*parent).keys[j + 1];
            
            (*parent).keys[(*parent).keys_count - 1] = NULL;
            (*parent).keys_count -= 1;
            isDelete = true;
            break;
        }
    }
    if (isDelete == false)
        return;
    
    // 2. add new key
    int insert_pos = fetchInsertPos(parent, newkey);
    int i = (*parent).keys_count;
    for (; i >= insert_pos; )
    {
        (*parent).keys[i] = (*parent).keys[i - 1];
        i--;
    }
    (*parent).keys[insert_pos] = newkey;
    (*parent).keys_count++;
}

/**
 * @brief try to redistribute borrow key from sibling
 *
 * @param node
 * @param key
 * @param mgmtData
 * @return true
 * @return false
 */
bool redistributeKeys(TreeNode *node, Value *key, TreeMtdt *mgmtData)
{
    TreeNode *parent = (*node).parent;
    TreeNode *sibling = NULL;
    int index, i = 0;
    bool EnoughSpace;

    for (; i <= (*parent).keys_count; i++)
    {
        if ((*parent).ptrs[i] != node)
            continue;
        
        // perfer left sibling
        sibling = (TreeNode *)(*parent).ptrs[i - 1];
        EnoughSpace = isAvailableSpace((*sibling).keys_count - 1, sibling, mgmtData);
        if (sibling)
        {
            if (EnoughSpace == true)
            {
                index = (*sibling).keys_count - 1;
                break;
            }
        }
        sibling = (TreeNode *)(*parent).ptrs[i + 1];
        EnoughSpace = isAvailableSpace((*sibling).keys_count - 1, sibling, mgmtData);
        if (sibling && EnoughSpace == true)
        {
            index = 0;
            break;
        }
        sibling = NULL;
        break;
    }
    if (sibling)
    {
        insLeafNode(node, replicateKey((*sibling).keys[index]), (*sibling).ptrs[index], mgmtData);
        updateKeyinParent(node, key, replicateKey((*sibling).keys[index]));
        (*sibling).keys_count--;
        free((*sibling).keys[index]);
        free((*sibling).ptrs[index + 1]);
        (*sibling).keys[index] = NULL;
        (*sibling).ptrs[index + 1] = NULL;
        return true;
    }
    // redistribute fail
    return false;
}

/**
 * @brief Checks capacity of sibling
 *
 * @param node
 * @param mgmtData
 * @return TreeNode*
 */
TreeNode *
checkSiblingVolume(TreeNode *node, TreeMtdt *mgmtData)
{
    TreeNode *sibling = NULL;
    TreeNode *parent = (*node).parent;
    int i = 0;
    for (; i <= (*parent).keys_count; i++)
    {
        if ((*parent).ptrs[i] != node)
            continue;
        
        if (i - 1 >= 0)
        {
            // perfer left sibling
            sibling = (TreeNode *)(*parent).ptrs[i - 1];
            if (isAvailableSpace((*sibling).keys_count + (*node).keys_count, sibling, mgmtData) == true)
                break;
            
        }
        if (i + 1 <= (*parent).keys_count + 1)
        {
            sibling = (TreeNode *)(*parent).ptrs[i + 1];
            if (isAvailableSpace((*sibling).keys_count + (*node).keys_count, sibling, mgmtData) == true)
                break;
            
        }
        sibling = NULL;
        break;
    }
    return sibling;
}

/**
 * @brief Merges siblings
 *
 * @param node
 * @param sibling
 * @param mgmtData
 */
void mergeSibling(TreeNode *node, TreeNode *sibling, TreeMtdt *mgmtData)
{
    TreeNode *parent = (*node).parent;

    int key_count = (*sibling).keys_count + (*node).keys_count;
    if (key_count > (*sibling).keys_count)
    {
        int i = 0, j = 0;
        Value **new_keys = malloc((key_count) * sizeof(Value *));
        void **new_ptrs = malloc((key_count + 1) * sizeof(void *));
        for (int curr = 0; curr < key_count; curr++)
        {
            if (cmpVal((*sibling).keys[i], (*node).keys[j]) <= 0 || j >= (*node).keys_count)
            {
                new_keys[curr] = (*sibling).keys[i];
                new_ptrs[curr] = (*sibling).ptrs[i++];
            }
            else
            {
                new_keys[curr] = (*sibling).keys[j];
                new_ptrs[curr] = (*sibling).ptrs[j];
            }
        }
        free((*sibling).keys);
        free((*sibling).ptrs);
        (*sibling).keys = new_keys;
        (*sibling).ptrs = new_ptrs;
    }
    // update parent
    deleteKeyinParent(node);
    (*mgmtData).nodes--;
    free((*node).ptrs);
    free((*node).keys);
    free(node);
    // Todo: recurisve
    // if ((*sibling).keys_count < (*mgmtData).minNonLeaf) {}
}

/**
 * @brief delete key
 *
 * @param tree
 * @param key
 * @return RC
 */
RC deleteKey(BTreeHandle *tree, Value *key)
{
    TreeMtdt *mgmtData = (TreeMtdt *)(*tree).mgmtData;
    // 1. Find correct leaf node L for k
    TreeNode *leaf = searchLeafNode((*mgmtData).root, key);
    // check: key not in tree
    if (!searchEntryInNode(leaf, key))
        return RC_IM_KEY_NOT_FOUND;
    
    // 2.Remove the entry
    delKeyInLeafNode(leaf, key, mgmtData);
    // If L is at least half full, the operation is done
    if ((*leaf).keys_count >= (*mgmtData).minLeaf)
    {
        updateKeyinParent(leaf, key, replicateKey((*leaf).keys[0]));
        return RC_OK;
    }
    // Otherwise
    // try to redistribute, borrowing from sibling
    // if redistribution fails, merge L and sibling.
    // if merge occured, delete entry in parent pointing to L.

    if (!checkSiblingVolume(leaf, mgmtData))
        redistributeKeys(leaf, key, mgmtData);
    
    else
        mergeSibling(leaf, checkSiblingVolume(leaf, mgmtData), mgmtData);
    
    return RC_OK;
}

/**
 * @brief initialize the scan which is used to scan the entries in the B+ tree
 *
 * @param tree
 * @param handle
 * @return RC
 */

RC openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle)
{
    TreeMtdt *mgmtData = (TreeMtdt *)(*tree).mgmtData;
    TreeNode *node = (TreeNode *)(*mgmtData).root;
    if (!node)
        return RC_IM_KEY_NOT_FOUND;

    // find the first leaf node
    for (; (*node).node_pos == INNER;)
        node = (*node).ptrs[0];
    
    Tree_ScanMtdt *scanMeta = (Tree_ScanMtdt *)malloc(sizeof(Tree_ScanMtdt));
    (*handle) = malloc(sizeof(BT_ScanHandle));
    (*scanMeta).keyIndex = 0;
    (*scanMeta).node = node;
    (*handle)->mgmtData = scanMeta;
    return RC_OK;
}

/**
 * @brief sibling entry
 *
 * @param handle
 * @param result
 * @return RC
 */
RC nextEntry(BT_ScanHandle *handle, RID *result)
{
    Tree_ScanMtdt *scanMeta = (Tree_ScanMtdt *)handle->mgmtData;
    RID *rec_ptr;
    TreeNode *node = (*scanMeta).node;
    int keyIndex = (*scanMeta).keyIndex;

    if (keyIndex < (*node).keys_count)
    {
        rec_ptr = RIDbuilder((RID *)(*node).ptrs[keyIndex]);
        (*scanMeta).keyIndex++;
    }
    else
    {
        if (!(*node).sibling)
            return RC_IM_NO_MORE_ENTRIES;
        
        (*scanMeta).keyIndex = 0;
        (*scanMeta).node = (*node).sibling;
        return nextEntry(handle, result);
    }
    (*result) = (*rec_ptr);
    return RC_OK;
}

/**
 * @brief close tree scan
 *
 * @param handle
 * @return RC
 */
RC closeTreeScan(BT_ScanHandle *handle)
{
    free((*handle).mgmtData);
    free(handle);
    return RC_OK;
}

/**
 * @brief print tree (debug and test functions)
 *        depth-first pre-order sequence
 * @param tree
 * @return char*
 */
char *printTree(BTreeHandle *tree)
{
    tMgmt = (TreeMtdt *)(*tree).mgmtData;
    TreeNode *root = (*tMgmt).root;
    if (!root)
        return NULL;
    
    TreeNode **queue = malloc(((*tMgmt).nodes) * sizeof(TreeNode *));
    int level = 0, count = 1, curr = 0, i = 0;

    queue[0] = root;
    for (; curr < (*tMgmt).nodes;)
    {
        TreeNode *node = queue[curr];
        printf("(%i)[", level);

        for (; i < (*node).keys_count;)
        {
            if ((*node).node_pos == LEAF)
            {
                RID *rec_ptr = (RID *)(*node).ptrs[i];
                printf("%i.%i,", (*rec_ptr).page, (*rec_ptr).slot);
                if (i == (*node).keys_count - 1)
                {
                    printf("%s", serializeValue((*node).keys[i]));
                }
                else
                {
                    printf("%s,", serializeValue((*node).keys[i]));
                }
            }
            printf("%i,", count);
            queue[count++] = (TreeNode *)(*node).ptrs[i];
            printf("%s,", serializeValue((*node).keys[i]));

            i++;
        }
        if ((*node).node_pos == INNER)
        {
            printf("%i", count);
            queue[count++] = (TreeNode *)(*node).ptrs[(*node).keys_count];
        }
        printf("]\n");
        level++;
        curr++;
    }
    return RC_OK;
}
