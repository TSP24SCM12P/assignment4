
<br>Shadaan Arzeen, A20528043</br>

<br>Vidheesha Patil, A20517203</br>

<br>Hyder Reza Telegraphy, A20527634</br>

======= Solution ======= <br>
    MAIN FUNCTIONS

1)init and shutdown index manager <br>
initIndexManager : Initializes the index manager <br>
shutdownIndexManager : Shutsdowns a index manager <br>
<br>
2)create, destroy, open, and close an btree index <br>
createBtree : Creates B Tree <br>
openBtree : Opens provided B Tree <br>
closeBtree : Closes provided B Tree <br>
deleteBtree : Deletes provided B Tree <br>
<br>
3)access information about a b-tree <br>
getNumNodes : Gets the number of nodes in the tree <br>
getNumEntries : Gets the number of enteries in the tree <br>
getKeyType : Gets the type of key in the tree <br>
<br>
4)index access <br>
findKey : Finds a provided key <br>
insertKey : Inserts a provided key <br>
deleteKey : Deletes a provided key <br>
openTreeScan : Initializes a new scan <br>
nextEntry : Gets the next entry <br>
closeTreeScan : Closes the initialized scan <br>
<br>
5)debug and test functions <br>
printTree :Prints BTree <br>
<br>
    ADDITIONAL FUNCTIONS

<br>
1. mgmtToHeader(): serializes Btree header <br>
2. headerToMgmt(): deserializes Btree header <br>
3. cmpVal(): compares key with sign <br>
4. searchLeafNode(): finds leaf node <br>
5. searchEntryInNode(): finds entry in node <br>
6. createNewNode(): creates a node object block <br>
7. createNewLeafNode(): creates a leaf node object block <br>
8. createNewNonLeafNode(): creats a non leaf node object <br>
9. fetchInsertPos(): Gets the insert position object <br>
10. insLeafNode(): Inserts entry into leaf node <br>
11. splitLeaf(): Splits leaf node <br>
12. splitNonLeaf(): Splits non lead node <br>
13. replicateKey(): Copies a provided key <br>
14. delKeyInLeafNode(): Deletes key from leaf node <br>
15. isAvailableSpace(): checks for enough space <br>
16. deleteKeyinParent(): Deletes the parent entry <br>
17  updateKeyinParent(): Updates parent entry <br>
18. redistributeKeys(): Tries to redistribute borrow key from sibling <br>
19. checkSiblingVolume(): Checks capacity of sibling <br>
20. mergeNodes(): Merges siblings <br>
21. insertKeyIntoParent(): Inserts given key into the parent node <br>
22. buildRID(): Builds RID <br>
<br>

=======Additional Structures Defined======= <br>

TreeNode { <br>
  NodePos node_pos; <br>
  Value **keys; // array[0...n] <br>
  // leaf-node => RID <br>
  // non-leaf-node => next level node <br>
  void **ptrs; // array[0...n] <br>
  int keys_count; // the count of key <br>
  RID *records; <br>
  struct TreeNode *sibling; // sibling <br>
  struct TreeNode *parent; // parent <br>
} <br>
<br>
<br>
TreeMtdt { <br>
  int n; // maximum keys in each block <br>
  int minLeaf; <br>
  int minNonLeaf; <br>
  int nodes; // the count of node <br>
  int entries; // the count of entries <br>
  DataType keyType; <br>
  <br>
  TreeNode *root; <br>
  <br>
  BM_PageHandle *ph; <br>
  BM_BufferPool *bm; <br>
} <br>

Tree_ScanMtdt {
  int keyIndex;
  TreeNode *node;
} Tree_ScanMtdt;


    

