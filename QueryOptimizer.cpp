#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <map>
#include <vector>
#include <ios>
#include <algorithm>
#include <cctype>

using namespace std;

// For storing reduction factors
struct RF {
    string colName;
    double rfVal = -1;
};

// For storing indexes
struct index {
    string name;
    int nkeys;
    double npages; 
    int height = -1;
    int min;
    int max;
};

// For storing foreign key relationships
struct fk_relation {
    string col;
    string ref_table;
    string ref_col;
};

// For storing a table
class Table {
    public:
        string name;
        vector<string> pks;
        vector<fk_relation> fks;
        vector<index> idxs;
        vector<RF> rfs;
        vector<string> columns;
        int ntuples;
        double npages;
        double tuplesPerPage;
        bool isOpTable = false;
        
        void setName(string newName) {
            name = newName;
        }
        void addPK(string pk) {
            pks.push_back(pk);

        }
        void addFK(fk_relation fk) {
            fks.push_back(fk);
        }
};

// For storing an operation
class Operation {
    public:
        string name;
        string opType = "";
        string query;
        string tbl1 = "";
        string tbl2 = "";
        string sel_col;
        string sel_type;
        int sel_val;
        string proj_cols;
        string join_col1;
        string join_col2;
        int ntuples;
        double npages;
        double cost;
        vector<string> inherit_tbls;
};

// To store the node of a query tree
class Node {
    public:
    Operation* op;
    Node* parent;
    Node* left;
    Node* right;
    Node(Operation* data) {
        this->op = data;
        this->parent = NULL;
        this->left = NULL;
        this->right = NULL;
    }
};

// Query tree for a given query
class QueryTree {
    public:
        Node* root;
        QueryTree(Node* root) {
            this->root = root;
        }
};

vector<Table> tables;
vector<Operation> operations;
vector<Operation> baseOperations;
vector<Node> treeNodes;
Node* treeRoot;

// Finds and returns the node corresponding to an operation
Node* findNode(string opName) {
    for (unsigned int i = 0; i < treeNodes.size(); i++) {
        if (treeNodes[i].op->name == opName) {
            return &(treeNodes[i]);
        }
    }
    return nullptr;
}

// Check if a given operation exists
bool opExists(string opName) {
    for (unsigned int i = 0; i < operations.size(); i++) {
        if (operations[i].name == opName) {
            return true;
        }
    }
    return false;
}

// Finds and returns an operation
Operation* findOperation(string opName) {
    for (unsigned int i = 0; i < operations.size(); i++) {
        if (operations[i].name == opName) {
            return &(operations[i]);
        }
    }
    return NULL;
}

// Finds and returns an RF
RF* findRF(Table* tbl, string colName) {
    for (unsigned int i = 0; i < tbl->rfs.size(); i++) {
        if (tbl->rfs[i].colName == colName) {
            return &(tbl->rfs[i]);
        }
    }
    return nullptr;
}

// Finds and returns an index
index* findIndex(Table* tbl, string idxName) {
    for (unsigned int i = 0; i < tbl->idxs.size(); i++) {
        if (tbl->idxs[i].name == idxName) {
            return &(tbl->idxs[i]);
        }
    }
    return nullptr;
}

// Finds and returns a table
Table* findTable(string tableName) {
    for (unsigned int i = 0; i < tables.size(); i++) {
        if (tables[i].name == tableName) {
            return &(tables[i]);
        }
    }
    return nullptr;
}

// Prints the child nodes of a node in the tree
void printChildren(Node* root, string link) {
    if (root == NULL) {
        return;
    }

    bool leftExists = (root->left != NULL);
    bool rightExists = (root->right != NULL);

    // Has no children
    if (!leftExists && !rightExists) {
        return;
    }

    cout << link;
    cout << ((leftExists && rightExists) ? "├── " : "");
    cout << ((!leftExists && rightExists) ? "└── " : "");

    if (rightExists) {
        bool grandChildExists = (leftExists && rightExists && (root->right->right != NULL || root->right->left != NULL));
        string nextLink = link + (grandChildExists ? "|   " : "    ");
        cout << root->right->op->name << endl;
        printChildren(root->right, nextLink);
    }

    if (leftExists) {
        cout << (rightExists ? link : "") << "└── " << root->left->op->name << endl;
        printChildren(root->left, link + "    ");
    }
}

// Function for printing the query tree
void printTree(Node* root) {
    // Base case
    if (root == NULL) {
        return;
    }
    cout << root->op->name << endl;
    printChildren(root, "");
    cout << endl;
}

// Finds out whether a node is the left/right child of a join operation in the tree
string whichChild(Node* parent, Node* child) {
    if (parent->left->op->name == child->op->name) {
        return "left";
    } else {
        return "right";
    }
}

// Pushes selections down the tree
void pushDownSelections(Node* selNode) {
    // Find the node that corresponds to the base table of the selection column
    string baseTblName = "";
    for (unsigned int i = 0; i < tables.size(); i++) {
        for (unsigned int j = 0; j < tables[i].columns.size(); j++) {
            if (tables[i].columns[j] == selNode->op->sel_col) {
                baseTblName = tables[i].name;
                break;
            }
        }
        if (baseTblName != "") {
            break;
        }
    }
    Node* baseTblNode = findNode(baseTblName);

    //Re-link the tree at the location of selection node
    Node* parentNode = selNode->parent;
    if (parentNode->op->opType == "SELECTION" || parentNode->op->opType == "PROJECTION") {
        parentNode->left = selNode->left;
        selNode->left->parent = parentNode;    
    } else if (parentNode->op->opType == "JOIN") {
        // Figure out whether current node is the left/right child
        if (whichChild(parentNode, selNode) == "left") {
            parentNode->left = selNode->left; 
        } else {
            parentNode->right = selNode->left;  
        }
        selNode->left->parent = parentNode;
    }
    
    //Insert selection node before base table
    if (baseTblNode->parent->op->opType == "JOIN") {
        if (whichChild(baseTblNode->parent, baseTblNode) == "left") {
            baseTblNode->parent->left = selNode;
        } else {
            baseTblNode->parent->right = selNode;  
        }
        selNode->parent = baseTblNode->parent;
        selNode->left = baseTblNode;
        baseTblNode->parent = selNode;
    } else {
        baseTblNode->parent->left = selNode;
        selNode->parent = baseTblNode->parent;
        selNode->left = baseTblNode;
        baseTblNode->parent = selNode;
    }
}

// Moves selections/projections up the query tree to help build pipelined approach
void updateUnary(Node* node) {
    // Make sure we're not at the root
    Node* grandParentNode = node->parent->parent;
    if (grandParentNode != NULL) {
        if (grandParentNode->op->opType == "JOIN") {
            if (whichChild(grandParentNode, node->parent) == "left") {
                grandParentNode->left = node;
                if (whichChild(node->parent, node) == "left") {
                    node->parent->left = node->left;
                } else {
                    node->parent->right = node->left;
                }
                node->left->parent = node->parent;
                node->parent->parent = node;
                node->parent = grandParentNode;
            } else {
                grandParentNode->right = node;
                if (whichChild(node->parent, node) == "left") {
                    node->parent->left = node->left;
                } else {
                    node->parent->right = node->left;
                }
                node->left->parent = node->parent;
                node->parent->parent = node;
                node->parent = grandParentNode;
            }
        } else {
            Node * tmp = node->parent;
            grandParentNode->left = node;
            if (whichChild(node->parent, node) == "left") {
                node->parent->left = node->left;
            } else {
                node->parent->right = node->left;
            }
            node->left->parent = node->parent;
            node->parent->parent = node;
            node->parent = grandParentNode;
            node->left = tmp;
        }
    } else {
        node->parent->parent = node;
        if (whichChild(node->parent, node) == "left") {
            node->parent->left = node->left;
        } else {
            node->parent->right = node->left;
        }
        node->left->parent = node->parent;
        node->left = node->parent;
        node->parent = NULL;
        treeRoot = node;        
    }
}

// Checks if a column exists in a given table
bool colExists(Table* tbl, string column) {
    for (unsigned int i = 0; i < tbl->columns.size(); i++) {
        if (tbl->columns[i] == column) {
            return true;
        }
    }
    return false;
}

// Re-structures the tree to make it left-deep
void updateJoin(Node* node) {
    // Base case is when the right side is a base table
    if (node->right->op->opType == "") {
        return;
    }
    string joinCol = node->op->join_col2;
    Table* rightTable = findTable(node->right->op->name);
    if (colExists(rightTable, joinCol)) {
        Node* tmp = node->parent;
        node->parent = node->right;
        node->right = node->right->right;
        node->right->parent = node;
        node->parent->right = node->parent->left;
        node->parent->left = node;
        if (tmp != NULL) {
            tmp->left = node->parent;
        } else {
            treeRoot = node->parent; 
        }
        node->parent->parent = tmp;
    } else {
        Node* tmp = node->parent;
        node->parent = node->right;
        node->right = node->right->left;
        node->right->parent = node;
        node->parent->left = node;
        if (tmp != NULL) {
            tmp->left = node->parent;       
        } else {
            treeRoot = node->parent; 
        }
        node->parent->parent = tmp;
    }
    if (node->right->op->opType != "") {
        updateJoin(node);
    }
}

// Wrapper function to restructure the original tree
void recurseTree(Node* node) {
    if (node->op->opType == "") {
        return;
    }
    if (node->op->opType == "JOIN") {
        recurseTree(node->left);
        recurseTree(node->right);
        if (node->right->op->opType == "SELECTION" || node->right->op->opType == "PROJECTION") {
            updateUnary(node->right);
        } else if (node->left->op->opType == "SELECTION" || node->left->op->opType == "PROJECTION") {
            updateUnary(node->left);
        }
        updateJoin(node);
    } else {
        recurseTree(node->left);
    }
}

// Returns the cost of the original query
double regularCost() {
    double total = 0;
    for (unsigned int i = 0; i < operations.size(); i++) {
        if (operations[i].opType == "SELECTION" || operations[i].opType == "PROJECTION" || operations[i].opType == "JOIN") {
            total += operations[i].cost;
        }
    }
    return total;
}

// Returns the cost of the optimized query
double optimizedCost() {
    double total = 0;
    for (unsigned int i = 0; i < operations.size(); i++) {
        if (operations[i].opType == "JOIN") {
            Node* opNode = findNode(operations[i].name);
            Table* leftTbl = findTable(opNode->left->op->name);
            Table* rightTbl = findTable(opNode->right->op->name);
            if (opNode->left->op->opType == "" && opNode->right->op->opType == "") {
                bool innerIdxExists = false;
                // Check for index on inner table
                for (unsigned int i = 0; i < rightTbl->idxs.size(); i++) {
                    if (rightTbl->idxs[i].name == operations[i].join_col2) {
                        innerIdxExists = true;
                    }
                }
                if (innerIdxExists) {
                    total += leftTbl->npages + (leftTbl->ntuples*1.2);
                } else {
                    total += leftTbl->npages + (leftTbl->ntuples*rightTbl->npages);
                }
            } else if (opNode->left->op->opType != "" && opNode->right->op->opType == "") {
                total += rightTbl->npages;
            }
        } else if (operations[i].opType == "SELECTION" || operations[i].opType == "PROJECTION") {
            Node* opNode = findNode(operations[i].name);
            Table* leftTbl = findTable(opNode->left->op->name);
            if (opNode->left->op->opType == "") {
                total += leftTbl->npages;
            }
        }
    }
    return total;
}

// Constructs the tree for the original query
void constructTree(Node* node) {
    if (node->op->opType == "JOIN") {
        //Check if left, right tables of join operation are base tables
        Node* leftNode = findNode(node->op->tbl1);
        Node* rightNode = findNode(node->op->tbl2);
        node->left = leftNode;
        node->right = rightNode;
        leftNode->parent = node;
        rightNode->parent = node;
    } else if (node->op->opType == "SELECTION" || node->op->opType == "PROJECTION") {
        Node* leftNode = findNode(node->op->tbl1);
        node->left = leftNode;
        leftNode->parent = node;
    }
}

// Adds the nodes for base tables
void pushBaseNode(string tblName) {
    Operation* baseTblOp = new Operation();
    baseTblOp->name = tblName;
    baseOperations.push_back(*baseTblOp);
    Node baseTblNode(baseTblOp);
    treeNodes.push_back(baseTblNode);
}

// Adds the nodes for operations
void pushOpNode(string opName) {
    Operation* currOp = findOperation(opName);
    Node opNode(currOp);
    treeNodes.push_back(opNode);
}

// Creates the nodes of the tree
void createBaseTblNodes() {
    for (unsigned int i = 0; i < tables.size(); i++) {
        if (findOperation(tables[i].name) == NULL && tables[i].name != "RESULT") {
            pushBaseNode(tables[i].name);
        }
    }
    for (unsigned int i = 0; i < operations.size(); i++) {
        pushOpNode(operations[i].name);
    } 
}

// Creates the query tree
QueryTree* createQueryTree() {
    createBaseTblNodes();
    for (unsigned int i = 0; i < treeNodes.size(); i++) {
        constructTree(&(treeNodes[i]));
    }
    Node* resultNode = findNode("RESULT");
    QueryTree qt(resultNode);
    return &qt;
}

// Sets the name of a table
void setTableName(string statement, Table* tbl) {
    int nameStart = statement.find("TABLE") + 6;
    int nameEnd = statement.find("(");
    string tableName = statement.substr(nameStart, nameEnd-nameStart);
    tbl->setName(tableName);
}

// Sets the primary keys of a table
void getPrimaryKeys(string statement, Table* tbl) {
    int pkStart = statement.find("PRIMARY KEY") + 12;
    int pkEnd = statement.find(")");
    string pkUnparsed = statement.substr(pkStart, pkEnd-pkStart);
    stringstream ss(pkUnparsed);
    string currPK;
    while (getline(ss, currPK, ' ')) {
        string finPK = currPK;
        if (currPK.back() == ',') {
            finPK = currPK.substr(0, currPK.size()-1);
        }
        tbl->addPK(finPK);
    }
}

// Sets the columns of a table
void getColumns (string statement, Table* tbl) {
    int colStart = statement.find_first_of('(');
    int colEnd = statement.find("PRIMARY KEY");
    string colsOnly = statement.substr(colStart+1, colEnd-colStart-2);
    stringstream ss (colsOnly);
        string currCol;
        while (getline(ss, currCol, ',')) {
            tbl->columns.push_back(currCol);
        }
}

// Copies indexes from one table to another
void copyTableIdxs(Table* newTbl, Table* existingTbl) {
    for (unsigned int i = 0; i < existingTbl->idxs.size(); i++) {
        newTbl->idxs.push_back(existingTbl->idxs[i]);
    }
}

// Copies RFs from one table to another
void copyTableRfs(Table* newTbl, Table* existingTbl) {
    for (unsigned int i = 0; i < existingTbl->rfs.size(); i++) {
        newTbl->rfs.push_back(existingTbl->rfs[i]);
    }
}

// Copies PKs from one table to another
void copyTablePks(Table* newTbl, Table* existingTbl) {
    for (unsigned int i = 0; i < existingTbl->pks.size(); i++) {
        newTbl->pks.push_back(existingTbl->pks[i]);
    }
}

// Copies FKs from one table to another
void copyTableFks(Table* newTbl, Table* existingTbl) {
    for (unsigned int i = 0; i < existingTbl->fks.size(); i++) {
        newTbl->fks.push_back(existingTbl->fks[i]);
    }
}   

// Sets the tuples per page for each table
void updateRegTbls() {
    for (unsigned int i = 0; i < tables.size(); i++) {
        if (tables[i].isOpTable == false) {
            tables[i].tuplesPerPage = tables[i].ntuples/tables[i].npages;
        }
    }
}

// Update the operation tables to preserve RFs, PKs, and FKs
void updateOpTbls() {
    for (unsigned int i = 0; i < operations.size(); i++) {
        Table* opTable = findTable(operations[i].name);
        for (unsigned int j = 0; j < operations[i].inherit_tbls.size(); j++) {
            Table* toInherit = findTable(operations[i].inherit_tbls[j]);
           // copyTableIdxs(opTable, toInherit);
            copyTableRfs(opTable, toInherit);
            copyTablePks(opTable, toInherit);
            copyTableFks(opTable, toInherit);
        }
    }
}

// Function for calculating cost of operations
void calcOpCosts() {
    for (unsigned int i = 0; i < operations.size(); i++) {
       // cout << "Currently on operation " << operations[i].name << endl;
        Operation* op = &(operations[i]);
        Table* opTable = findTable(op->name);
        if (op->opType == "JOIN") {
            // We assume that tbl1 is the outer table
            Table* tbl1 = findTable(op->tbl1);
            Table* tbl2 = findTable(op->tbl2);
            // Look for an index on the inner table (tbl2)
            bool innerIdxExists = false;
                for (unsigned int i = 0; i < tbl2->idxs.size(); i++) {
                    if (tbl2->idxs[i].name == op->join_col2) {
                        innerIdxExists = true;
                    }
                }
            if (innerIdxExists == true) {
                double costToMatch = 1.2;
                op->cost = tbl1->npages + (tbl1->ntuples * costToMatch);
            } else {
                op->cost = tbl1->npages + (tbl1->ntuples * tbl2->npages);
            }
            // Get the RF of this join condition
            RF* tbl1_RF = findRF(tbl1, op->join_col1);
            RF* tbl2_RF = findRF(tbl2, op->join_col2);
            double joinRF = tbl1_RF->rfVal*tbl2_RF->rfVal;
            index* tbl1_idx = findIndex(tbl1, op->join_col1);
            index* tbl2_idx = findIndex(tbl1, op->join_col1);
            if (tbl1_idx != nullptr && tbl2_idx != nullptr) {
                joinRF = 1/(max(tbl1_idx->nkeys, tbl2_idx->nkeys));
            }
            opTable->ntuples = (tbl1->ntuples*tbl2->ntuples)*(joinRF);
            opTable->npages = op->cost;
        } else {
            Table* tbl1 = findTable(op->tbl1);
            if (op->opType == "SELECTION") {
                int readFileCost = tbl1->npages;
                RF* selColRF = findRF(opTable, op->sel_col);
                opTable->ntuples = tbl1->ntuples*selColRF->rfVal;
                opTable->tuplesPerPage = tbl1->tuplesPerPage;
                // Check if index exists on selection column
                // If it does, cost changes (as we dont have to write to temp)
                for (unsigned int i = 0; i < tbl1->idxs.size(); i++) {
                    if (tbl1->idxs[i].name == op->sel_col) {
                        if (op->sel_type == "=") {
                            readFileCost = (tbl1->ntuples/tbl1->idxs[i].nkeys)/tbl1->tuplesPerPage;
                            opTable->ntuples = tbl1->ntuples/(tbl1->idxs[i].nkeys);
                        } else if (op->sel_type == ">") {
                            double idxRF = (tbl1->idxs[i].max-(op->sel_val))/(tbl1->idxs[i].max - tbl1->idxs[i].min);
                            readFileCost = (tbl1->ntuples*(idxRF))/tbl1->tuplesPerPage;
                        }
                    }
                }
                op->cost = readFileCost;
                opTable->npages = op->cost;
                // If we are selecting from an existing operation, use on-the-fly
                if (tbl1->isOpTable == true) {
                    op->cost = 0;
                }
            } else if (op->opType == "PROJECTION") {
                // Perform projections on-the-fly, cost is zero as the input is pipelined.
                // However we must file scan if the projection is happening on a base table.
                Table* tbl1 = findTable(op->tbl1);
                op->cost = 0;
                if (tbl1->isOpTable == false) {
                    op->cost = tbl1->npages; 
                }
                opTable->ntuples = tbl1->ntuples;

                //Calculate the number of pages produced by this projection operation
                stringstream ss (op->proj_cols);
                string currCol;
                double totalRF = 1;
                while (getline(ss, currCol, ',')) {
                    RF* currRF = findRF(tbl1, currCol);
                    totalRF *= currRF->rfVal;
                }

                opTable->npages = (tbl1->npages)*totalRF;
            }
        }
    }
}


// Processes table statement and stores details
void processTable(string statement) {
    Table newTbl;
    setTableName(statement, &newTbl);
    getPrimaryKeys(statement, &newTbl);
    getColumns(statement, &newTbl);
    newTbl.isOpTable = false;
    tables.push_back(newTbl);
}

// Processes foreign key statement and stores details
void processForeign(string statement) {
    // Obtain only what's inside the outermost brackets
    int stmtStart = statement.find("(");
    int stmtEnd = statement.size();
    string cutStmt = statement.substr(stmtStart+1, stmtEnd-stmtStart-3);

    // Getting table name and fk column
    int tblEnd = cutStmt.find("(");
    int colEnd = cutStmt.find(")");
    string tableName = cutStmt.substr(0, tblEnd);
    string colName = cutStmt.substr(tblEnd+1, colEnd-tblEnd-1);

    // Getting referenced table and column
    int refStart = cutStmt.find("REFERENCES");
    string refStmt = cutStmt.substr(refStart);
    int refTblStart = refStmt.find(" ") + 1;
    int refTblEnd = refStmt.find("(");
    int refEnd = refStmt.find(")");
    string refTable = refStmt.substr(refTblStart, refTblEnd-refTblStart);
    string refCol = refStmt.substr(refTblEnd+1, refEnd-refTblEnd-1);

    // Storing results
    for (unsigned int i = 0; i < tables.size(); i++) {
        if (tables[i].name == tableName) {
            fk_relation fk;
            fk.col = colName;
            fk.ref_col = refCol;
            fk.ref_table = refTable;
            tables[i].addFK(fk);
        }
    }
}

// Function to process OPERATION statement
void processOP (string operationStr) {
    // Finding the type of operation
    Operation newOp;
    string parseOp;
    int equalLoc = operationStr.find('=');
    string tableName = operationStr.substr(0, equalLoc-1);
    (&newOp)->name = tableName;
    string rightSide = operationStr.substr(equalLoc + 1);
    istringstream iss(rightSide);
    iss >> parseOp; //Table1
    (&newOp)->tbl1 = parseOp;
    iss >> parseOp; //Operation Type
    (&newOp)->opType = parseOp;

    // Parse various types of operations
    if (newOp.opType == "SELECTION") {
        iss >> parseOp;
        string selCol;
        string selType;
        int selVal;
        if (parseOp.find('=') != string::npos) {
            int eqLoc = parseOp.find('=');
            selCol = parseOp.substr(0, eqLoc);
            selType = '=';
            selVal = stoi(parseOp.substr(eqLoc+1));
        } else if (parseOp.find('>') != string::npos) {
            int gtLoc = parseOp.find('>');
            selCol = parseOp.substr(0, gtLoc);
            selType = '>';
            selVal = stoi(parseOp.substr(gtLoc+1));
        }
        (&newOp)->sel_col = selCol;
        (&newOp)->sel_type = selType;
        (&newOp)->sel_val = selVal;
        (&newOp)->inherit_tbls.push_back((&newOp)->tbl1);
    } else if (newOp.opType == "PROJECTION") {
        iss >> parseOp;
        (&newOp)->proj_cols = parseOp;
        (&newOp)->inherit_tbls.push_back((&newOp)->tbl1);
    } else if (newOp.opType == "JOIN") {
        iss >> parseOp;
        (&newOp)->tbl2 = parseOp;
        iss >> parseOp;
        iss >> parseOp;
        int eqLoc = parseOp.find('=');
        string col1 = parseOp.substr(0,eqLoc);
        string col2 = parseOp.substr(eqLoc+1);
        (&newOp)->join_col1 = col1;
        (&newOp)->join_col2 = col2;
        (&newOp)->inherit_tbls.push_back((&newOp)->tbl1);
        (&newOp)->inherit_tbls.push_back((&newOp)->tbl2);
    }
    Table newTbl;
    newTbl.name = newOp.name;
    newTbl.isOpTable = true;
    tables.push_back(newTbl);
    operations.push_back(newOp);
}

// Function to process CARDINALITY statement
void processCard(string statement) {
    int start = statement.find_first_of('(');
    int end = statement.find_last_of(')');
    string inBracket = statement.substr(start+1, end-start-1);
    int eqLoc = statement.find('=');
    string rhs = statement.substr(eqLoc+1);
    string cardValStr;
    istringstream iss(rhs);
    iss >> cardValStr;
    int cardVal = stoi(cardValStr);

    // Check if Cardinality of TABLE or Index Column
    if (inBracket.find(" IN ") == string::npos) {
        Table* tbl = findTable(inBracket);
        tbl->ntuples = cardVal;
    } else {
        // Check for multi-attribute index
        string parseCard;
        if (inBracket.find('(') == string::npos) {  
            string idx_col;
            istringstream iss2(inBracket);
            iss2 >> parseCard;
            idx_col = parseCard;
            iss2 >> parseCard;
            iss2 >> parseCard;
            Table* tbl = findTable(parseCard);
            index idx;
            idx.name = idx_col;
            idx.nkeys = cardVal;
            tbl->idxs.push_back(idx);
        } else {
            int inStart = inBracket.find('(');
            int inEnd = inBracket.find(')');
            string idx_col = inBracket.substr(inStart+1, inEnd-inStart-1);
            string inTable = inBracket.substr(inEnd+1);
            istringstream iss2(inTable);
            iss2 >> parseCard;
            iss2 >> parseCard;
            Table* tbl = findTable(parseCard);
            index idx;
            idx.name = idx_col;
            idx.nkeys = cardVal;
            tbl->idxs.push_back(idx);
        }
    }
}

// Function to process SIZE statement
void processSize(string statement) {
    int start = statement.find_first_of('(');
    int end = statement.find_last_of(')');
    string inBracket = statement.substr(start+1, end-start-1);
    int eqLoc = statement.find('=');
    string rhs = statement.substr(eqLoc+1);
    string sizeValStr;
    istringstream iss(rhs);
    iss >> sizeValStr;
    double sizeVal = stod(sizeValStr);

    // Check if Size of TABLE or Index Column
    if (inBracket.find(" IN ") == string::npos) {
        Table* tbl = findTable(inBracket);
        tbl->npages = sizeVal;
    } else {
        // Check for multi-attribute index
        string parseSize;
        if (inBracket.find('(') == string::npos) { 
            string idx_col;
            istringstream iss2(inBracket);
            iss2 >> parseSize; // Col
            idx_col = parseSize;
            iss2 >> parseSize; // in
            iss2 >> parseSize; // Table
            Table* tbl = findTable(parseSize);
            index* idx = findIndex(tbl, idx_col);
            idx->npages = sizeVal;
        } else {
            int inStart = inBracket.find('(');
            int inEnd = inBracket.find(')');
            string idx_col = inBracket.substr(inStart+1, inEnd-inStart-1);
            string inTable = inBracket.substr(inEnd+1);
            istringstream iss2(inTable);
            iss2 >> parseSize;
            iss2 >> parseSize;
            Table* tbl = findTable(parseSize);
            index* idx = findIndex(tbl, idx_col);
            idx->npages = sizeVal;
        }
    }
}

// Function to process SIZE statement
void processRF(string statement) {
    int start = statement.find_first_of('(');
    int end = statement.find_last_of(')');
    string inBracket = statement.substr(start+1, end-start-1);
    int eqLoc = statement.find('=');
    string rhs = statement.substr(eqLoc+1);
    string rfValStr;
    istringstream iss(rhs);
    iss >> rfValStr;
    double rf_Val = stod(rfValStr);

    string parseRF;
    string col;
    istringstream iss2(inBracket);
    iss2 >> parseRF;
    col = parseRF;   // Col
    iss2 >> parseRF; // in
    iss2 >> parseRF; // Table
    Table* tbl = findTable(parseRF);
    RF currRF;
    currRF.colName = col;
    currRF.rfVal = rf_Val;
    tbl->rfs.push_back(currRF);
}

// Function to process HEIGHT statement
void processHeight(string statement) {
    int start = statement.find_first_of('(');
    int end = statement.find_last_of(')');
    string inBracket = statement.substr(start+1, end-start-1);
    int eqLoc = statement.find('=');
    string rhs = statement.substr(eqLoc+1);
    string heightValStr;
    istringstream iss(rhs);
    iss >> heightValStr;
    int heightVal = stoi(heightValStr);

    string parseHeight;
        if (inBracket.find('(') == string::npos) {  
            string idx_col;
            istringstream iss2(inBracket);
            iss2 >> parseHeight;
            idx_col = parseHeight;
            iss2 >> parseHeight;
            iss2 >> parseHeight;
            Table* tbl = findTable(parseHeight);
            index* idx = findIndex(tbl, idx_col);
            idx->height = heightVal;
        } else {
            int inStart = inBracket.find('(');
            int inEnd = inBracket.find(')');
            string idx_col = inBracket.substr(inStart+1, inEnd-inStart-1);
            string inTable = inBracket.substr(inEnd+1);
            istringstream iss2(inTable);
            iss2 >> parseHeight;
            iss2 >> parseHeight;
            Table* tbl = findTable(parseHeight);
            index* idx = findIndex(tbl, idx_col);
            idx->height = heightVal;
        }    
}

// Function to process RANGE statement
void processRange(string statement) {
    int start = statement.find_first_of('(');
    int end = statement.find_last_of(')');
    string inBracket = statement.substr(start+1, end-start-1);
    int eqLoc = statement.find('=');
    string rhs = statement.substr(eqLoc+1);
    string rangeStr;
    istringstream iss(rhs);
    iss >> rangeStr;
    stringstream ss(rangeStr);
    string currVal;
    vector<int> minMax;
    while (getline(ss >> ws, currVal, ',')) {
        minMax.push_back(stoi(currVal));
    }
    int minVal = minMax[0];
    int maxVal = minMax[1];
    
    string parseRange;
    string idx_col;
    istringstream iss2(inBracket);
    iss2 >> parseRange;
    idx_col = parseRange;
    iss2 >> parseRange;
    iss2 >> parseRange;
    Table* tbl = findTable(parseRange);
    index* idx = findIndex(tbl, idx_col);
    idx->min = minVal;
    idx->max = maxVal;
}

// Function to process all statement
void processStatement(string statement) {
    if (statement.substr(0, 2) == "OP" || statement.substr(0, 6) == "RESULT") {
        processOP(statement);
    } else if (statement.substr(0, 5) == "TABLE") {
        processTable(statement);
    } else if (statement.substr(0, 7) == "FOREIGN") {
        processForeign(statement);
    } else if (statement.substr(0, 11) == "CARDINALITY") {
        processCard(statement);
    } else if (statement.substr(0, 4) == "SIZE") {
        processSize(statement);
    } else if (statement.substr(0, 2) == "RF") {
        processRF(statement);
    } else if (statement.substr(0, 6) == "Height") {
        processHeight(statement);
    } else if (statement.substr(0, 5) == "RANGE" || statement.substr(0, 5) == "Range") {
        processRange(statement);
    }
}

int main (int argc, char** argv) {
    if (argc != 2) {
        cerr << "Please pass 1 input file to the program" << endl;
    }
    string line;
    string whitespace = " ";
    ifstream inputFile(argv[1]);
   
    while (getline(inputFile, line)) {
        if (line != "") {
            // Getting rid of any whitespace at the beginning of a line
            size_t starting = line.find_first_not_of(whitespace);
            string statement = line.substr(starting);
            transform(statement.begin(), statement.end(), statement.begin(), ::toupper);
            processStatement(statement);
        }
    }
    updateRegTbls();
    updateOpTbls();
    calcOpCosts();
    QueryTree* qt = createQueryTree();
    Node* currRoot = findNode("RESULT");
    treeRoot = currRoot;
    // Before changes
    cout << "--------------" << endl;
    cout << "| Query Tree |" << endl;
    cout << "--------------" << endl;
    cout << endl;
    printTree(treeRoot);
    cout << "Cost: " << (long)regularCost() << " I/Os" << endl;
    cout << endl;
    cout << "------------------------" << endl;
    cout << "| Optimized Query Tree |" << endl;
    cout << "------------------------" << endl;
    cout << endl;
    recurseTree(treeRoot);
    printTree(treeRoot);
    cout << "Cost: " << (long)optimizedCost() << " I/Os" << endl;
    cout << endl;
}