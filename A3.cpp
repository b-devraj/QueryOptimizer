#include <iostream>
#include <string>
#include <fstream>
#include <sstream>
#include <vector>
#include <set>
#include <unordered_map>
#include <map>
#include <algorithm>
using namespace std;

class Operation {
    public:
        int timeOffset;
        int transID;
        string opType = "";
        int objID = -1;
        string deadlockMsg = "";
};

class Transaction {
    public:
        int transID;
        bool hasCommitted = false;
        vector<Operation> operations;
        vector<int> dependentTransactions; // list of transactions that depend on this transaction
        vector<int> dependsOn; // list of transactions that depend on this transaction
        string deadlockMsg = "";
};

struct Object {
    int objID;
    vector<Transaction> refTrans;
    bool isClean = true;
};

vector<Operation> operations;
vector<Transaction> transactions;
vector<Operation> stallOps;
vector<int> committedTrans;
unordered_map<int, Object> objRel;
set<int> abortedTrans;
map<int, int> deadlockTrans;
map<int, int> dependencies;
vector<int> cycleTrans;

// Recoverable Schedule
vector<Operation> recOperations;

// Cascadeless Schedule
vector<Operation> casOperations;

void clearAll () {
    for (unsigned int i = 0; i < transactions.size(); i++) {
        Transaction currTrans = transactions[i];
        currTrans.operations.clear();
        currTrans.dependentTransactions.clear();
        currTrans.dependsOn.clear();
    }
    transactions.clear();
    stallOps.clear();
    committedTrans.clear();
    for (auto rel: objRel) {
        rel.second.refTrans.clear();
    }
    objRel.clear();
    abortedTrans.clear();
    deadlockTrans.clear();
    dependencies.clear();
}

Transaction* getTrans(int id) {
    for (unsigned int i = 0; i < transactions.size(); i++) {
        if (id == transactions[i].transID) {
            return &(transactions[i]);
        }
    }
    return NULL;
}

bool objExists(int objID) {
    if (objRel.find(objID) == objRel.end()) {
        return false;
    }
    return true;
}

bool foundTrans (vector<int> trans, int searchID) {
    for (unsigned int i = 0; i < trans.size(); i++) {
        if (trans[i] == searchID) {
            return true;
        }
    }
    return false;
}

bool compOperations (Operation op1, Operation op2) {
    return op1.timeOffset < op2.timeOffset;
}

bool inCycle (int transID) {
    for (unsigned int i = 0; i < cycleTrans.size(); i++) {
        if (cycleTrans[i] == transID) {
            return true;
        }
    }
    return false;
}

void remDep (Transaction* trans) {
    Transaction* depTrans;
    for (unsigned i = 0; i < trans->dependentTransactions.size(); i++) {
        depTrans = getTrans(trans->dependentTransactions[i]);
        auto it = find(depTrans->dependsOn.begin(), depTrans->dependsOn.end(), trans->transID);
        if (it != depTrans->dependsOn.end()) {
            depTrans->dependsOn.erase(it);
        }
    }
}

void cascadeAbort (Transaction* trans, bool isDeadlock, string scheduleType) {
    if (abortedTrans.find(trans->transID) == abortedTrans.end()) {
            Operation abortOp;
            abortOp.transID = trans->transID;
            abortOp.opType = "A";
            abortOp.timeOffset = -1;
            if (isDeadlock) {
                abortOp.deadlockMsg = "(because of deadlock)";
            }
            trans->operations.push_back(abortOp);
            if (scheduleType == "REC") {
                recOperations.push_back(abortOp);
            } else {
                casOperations.push_back(abortOp);
            }
            abortedTrans.insert(trans->transID);
    }
    for (unsigned int i = 0; i < trans->dependentTransactions.size(); i++) {
        if (abortedTrans.find(trans->dependentTransactions[i]) == abortedTrans.end()) {
            cascadeAbort(getTrans(trans->dependentTransactions[i]), isDeadlock, scheduleType);
        }
    }
}

void processStalledOp (Operation* currOp, string scheduleType) {
    Transaction* currTrans = getTrans(currOp->transID);
    if (currOp->opType == "R") {
        if (objRel[currOp->objID].refTrans.size() > 0) {
            Transaction* indepTrans = getTrans(objRel[currOp->objID].refTrans.back().transID);
                if (currTrans->transID != indepTrans->transID) {
                    indepTrans->dependentTransactions.push_back(currTrans->transID);
                    currTrans->dependsOn.push_back(indepTrans->transID);
                    dependencies[currTrans->transID] = indepTrans->transID;
                }
                // Detect deadlock and store it. Abort when one of them tries to commit.
                if (foundTrans(currTrans->dependentTransactions, indepTrans->transID)) {
                    deadlockTrans[currTrans->transID] = indepTrans->transID; 
                    deadlockTrans[indepTrans->transID] = currTrans->transID;
                } else if (!(deadlockTrans.find(indepTrans->transID) == deadlockTrans.end())) {
                    // If the trans we are reading from is already in a deadlock cycle, the curr
                    // trans is also apart of this deadlock
                    deadlockTrans[currTrans->transID] = indepTrans->transID;
                }
            }
    } else if (currOp->opType == "W") {
        if (!(objExists(currOp->objID))) {
            // Create the obj, attach the transaction to it
            Object currObj;
            currObj.objID = currOp->objID;
            currObj.refTrans.push_back(*currTrans);
            objRel[currOp->objID] = currObj;
            objRel[currOp->objID].refTrans.push_back(*currTrans);
        } else {
            objRel[currOp->objID].refTrans.push_back(*currTrans);
        }
    } else if (currOp->opType == "A") {
        currTrans->operations.push_back(*currOp);
        abortedTrans.insert(currOp->transID);   
        cascadeAbort(currTrans, false, "CAS");
        remDep(currTrans);
    } else if (currOp->opType == "C") {
        currTrans->operations.push_back(*currOp);
        committedTrans.push_back(currOp->transID);
        remDep(currTrans);
    }
}

bool mustWait (Transaction* trans, string scheduleType, Operation currOp) {
    // Must wait for transactions it is dependent on to commit
    for (unsigned int i = 0; i < trans->dependsOn.size(); i++) {
        if (find(committedTrans.begin(), committedTrans.end(), trans->dependsOn[i]) == committedTrans.end()) {
            return true;
        }
    }
    if (scheduleType != "REC") {
    // Cannot jump ahead of operations which come before it
        for (unsigned int i = 0; i < stallOps.size(); i++) {
            if (stallOps[i].transID == trans->transID && stallOps[i].timeOffset < currOp.timeOffset && stallOps[i].timeOffset != currOp.timeOffset) {
                return true;
            }
        } 
    }
    return false;
}



void updateStall (string scheduleType) {
    for (unsigned int i = 0; i < stallOps.size(); i++) {
        Operation currOp = stallOps[i];
        if (mustWait(getTrans(currOp.transID), scheduleType, currOp) == false) {
            if (scheduleType == "REC") {
                recOperations.push_back(currOp);
            } else {
                casOperations.push_back(currOp);
            }
            stallOps.erase(stallOps.begin() + i);
            processStalledOp(&currOp, "CAS");
            updateStall(scheduleType);
        }
    }
}

string makeDeadMsg (Operation currOp) {
    string deadMsg = "Deadlock detected at (" + to_string(currOp.timeOffset) + " T";
    if (currOp.transID <= 9) {
        deadMsg += "0" + to_string(currOp.transID) + " ";
    } else {
        deadMsg += to_string(currOp.transID) + " ";
    }
    deadMsg += currOp.opType;
    if (currOp.opType == "R" || currOp.opType == "W") {
        deadMsg += " O";
            if (currOp.objID <= 9) {
                deadMsg += "0" + to_string(currOp.objID) + " ";
            } else {
                deadMsg += to_string(currOp.objID);
            }
    }
    deadMsg += ").\n";
    deadMsg += "Deadlocked transactions:";
    set<int> deadTrans;
    for (unsigned int i = 0; i < cycleTrans.size(); i++) {
        deadTrans.insert(cycleTrans[i]);
    }
    for (auto transID : deadTrans) {
        deadMsg += " T";
        if (transID <= 9) {
            deadMsg += "0" + to_string(transID);
        } else {
            deadMsg += to_string(transID);
        }
    }
    deadMsg += ".";
    return deadMsg;
}

void printOps (vector<Operation> ops) {
    int opCount = 1;
    for (unsigned int i = 0; i < ops.size(); i++) {
        Operation currOp = ops[i];
        if (currOp.opType == "D") {
            cout << currOp.deadlockMsg << endl;
        } else {
            cout << opCount << "   ";
            if (currOp.timeOffset == -1) {
                cout << "-   T";
            } else {
                cout << currOp.timeOffset << "   T";
            }
            if (currOp.transID <= 9) {
                cout << "0" << currOp.transID << "   ";
            } else {
                cout << currOp.transID << "   ";
            }
            cout << currOp.opType;
            if (currOp.opType == "R" || currOp.opType == "W") {
                cout << "   O";
                if (currOp.objID <= 9) {
                    cout << "0" << currOp.objID << " ";
                } else {
                    cout << currOp.objID;
                }
            }
            if (currOp.deadlockMsg != "") {
                cout << "   " << currOp.deadlockMsg;
            }
            cout << endl;
            opCount += 1; 
        }
    }
}

// Check if adjacent trasactions have cycles
bool hasCycle(int node, map<int, vector<int>> dependencies, set<int> visited, set<int> path, vector<int>* cycle) {
    visited.insert(node);
    path.insert(node);
    for (const int neighbor : dependencies[node]) {
        if (visited.count(neighbor) == 0) {
            if (hasCycle(neighbor, dependencies, visited, path, cycle)) {
                cycle->push_back(node);
                for (const int cycleNode : path) {
                    if (cycleNode != node) {
                        cycle->push_back(cycleNode);
                    } else {
                        break;
                    }
                }
                return true;
            }
        } else if (path.count(neighbor) == 1) {
            cycle->push_back(node);
            for (const int cycleNode : path) {
                if (cycleNode != node) {
                    cycle->push_back(cycleNode);
                } else {
                    break;
                }
            }
            return true;
        }
    }
    path.erase(node);
    return false;
}

// Using adjacency list for each transaction, check for cycles
bool detectCycle(map<int, int> readsFrom, vector<int>* cycle) {
    map<int, vector<int>> dependencies;
    for (auto pair : readsFrom) {
        if (pair.first != pair.second) { // Self-loops are allowed
            dependencies[pair.second].push_back(pair.first);
        }
    }
    set<int> visited;
    set<int> path;
    for (auto pair : dependencies) {
        if (visited.count(pair.first) == 0) {
            if (hasCycle(pair.first, dependencies, visited, path, cycle)) {
                return true;
            }
        }
    }
    return false;
}

/*
RECOVERABLE SCHEDULE
*/

void getRecSchedule(vector<Operation> ops) {
    Operation currOp;

    for (unsigned int i = 0; i < ops.size(); i++) {
        currOp = ops[i];
        // Ignore operations of already aborted transactions
        if (!(abortedTrans.find(currOp.transID) == abortedTrans.end())) {
            continue;
        }
        // Start, initialize a transaction object
        if (currOp.opType == "S") {
            Transaction newTran;
            newTran.transID = currOp.transID;
            newTran.operations.push_back(currOp);
            transactions.push_back(newTran);
            recOperations.push_back(currOp);
        } else if (currOp.opType == "W") {
            Transaction* currTrans = getTrans(currOp.transID);
            if (!(objExists(currOp.objID))) {
                // Create the obj, attach the transaction to it
                Object currObj;
                currObj.objID = currOp.objID;
                currObj.refTrans.push_back(*currTrans);
                objRel[currOp.objID] = currObj;
                objRel[currOp.objID].refTrans.push_back(*currTrans);
            } else {
                objRel[currOp.objID].refTrans.push_back(*currTrans);
            }
            currTrans->operations.push_back(currOp);
            recOperations.push_back(currOp);
        } else if (currOp.opType == "R") {
            Transaction* currTrans = getTrans(currOp.transID);
            if (!(objExists(currOp.objID))) {
                // Create the obj, attach the transaction to it
                Object currObj;
                currObj.objID = currOp.objID;
                objRel[currOp.objID] = currObj;
            } else {
                // Check if this object has been written to before
                if (objRel[currOp.objID].refTrans.size() > 0) {
                    Transaction* indepTrans = getTrans(objRel[currOp.objID].refTrans.back().transID);
                    if (currTrans->transID != indepTrans->transID) {
                        indepTrans->dependentTransactions.push_back(currTrans->transID);
                        currTrans->dependsOn.push_back(indepTrans->transID);
                        dependencies[currTrans->transID] = indepTrans->transID;
                    }
                    // Detect deadlock and store it. Abort when one of them tries to commit.
                    if (foundTrans(currTrans->dependentTransactions, indepTrans->transID)) {
                        deadlockTrans[currTrans->transID] = indepTrans->transID; 
                        deadlockTrans[indepTrans->transID] = currTrans->transID;
                        string deadlockMessage = "(Deadlock cycle between " + to_string(currTrans->transID) 
                                                + " and " + to_string(indepTrans->transID) + ")";
                        currTrans->deadlockMsg = deadlockMessage;
                        indepTrans->deadlockMsg = deadlockMessage;
                    } else if (!(deadlockTrans.find(indepTrans->transID) == deadlockTrans.end())) {
                        // If the trans we are reading from is already in a deadlock cycle, the curr
                        // trans is also apart of this deadlock
                        deadlockTrans[currTrans->transID] = indepTrans->transID;
                    }
                }
            }
            currTrans->operations.push_back(currOp);
            recOperations.push_back(currOp);
        } else if (currOp.opType == "A") {
            Transaction* currTrans = getTrans(currOp.transID);
            currTrans->operations.push_back(currOp);
            recOperations.push_back(currOp);
            abortedTrans.insert(currOp.transID);   
            cascadeAbort(currTrans, false, "REC");
        } else if (currOp.opType == "C") {
            Transaction* currTrans = getTrans(currOp.transID);
            if (detectCycle(dependencies, &cycleTrans) && inCycle(currTrans->transID)) {
                string deadlockMsg = makeDeadMsg(currOp);
                Operation deadOp;
                deadOp.opType = "D";
                deadOp.deadlockMsg = deadlockMsg;
                recOperations.push_back(deadOp);
                cascadeAbort(getTrans(currOp.transID), true, "REC");
            } else {
                if (mustWait(currTrans, "REC", currOp)) {
                    stallOps.push_back(currOp);
                } else {
                    currTrans->operations.push_back(currOp);
                    recOperations.push_back(currOp);
                    committedTrans.push_back(currOp.transID);
                    remDep(currTrans);
                    updateStall("REC");
                }
            }
            cycleTrans.clear();
        }
    }
    cout << "Recoverable" << endl;
    printOps(recOperations);
    clearAll();
}

/*
CASCADELESS RECOVERABLE SCHEDULE
*/

void getCascSchedule(vector<Operation> ops) {
    Operation currOp;

    for (unsigned int i = 0; i < ops.size(); i++) {
        currOp = ops[i];
        // Ignore operations of already aborted transactions
        if (!(abortedTrans.find(currOp.transID) == abortedTrans.end())) {
            continue;
        }
        // Start, initialize a transaction object
        if (currOp.opType == "S") {
            Transaction newTran;
            newTran.transID = currOp.transID;
            newTran.operations.push_back(currOp);
            transactions.push_back(newTran);
            casOperations.push_back(currOp);
        } else if (currOp.opType == "W") {
            Transaction* currTrans = getTrans(currOp.transID);
            if (!(objExists(currOp.objID))) {
                // Create the obj, attach the transaction to it
                if (mustWait(currTrans, "CAS", currOp)) {
                    stallOps.push_back(currOp);
                } else {
                    Object currObj;
                    currObj.objID = currOp.objID;
                    currObj.refTrans.push_back(*currTrans);
                    objRel[currOp.objID] = currObj;
                    objRel[currOp.objID].refTrans.push_back(*currTrans);
                    currTrans->operations.push_back(currOp);
                    casOperations.push_back(currOp);
                }
            } else {
                if (mustWait(currTrans, "CAS", currOp)) {
                    stallOps.push_back(currOp);
                } else {
                    objRel[currOp.objID].refTrans.push_back(*currTrans);
                    currTrans->operations.push_back(currOp);
                    casOperations.push_back(currOp);
                }
            }
        } else if (currOp.opType == "R") {
            Transaction* currTrans = getTrans(currOp.transID);
            if (!(objExists(currOp.objID))) {
                // Create the obj, attach the transaction to it
                if (mustWait(currTrans, "CAS", currOp)) {
                    stallOps.push_back(currOp);
                } else {
                    Object currObj;
                    currObj.objID = currOp.objID;
                    objRel[currOp.objID] = currObj;
                    currTrans->operations.push_back(currOp);
                    casOperations.push_back(currOp);
                }
            } else {
                // Check if this object has been written to before
                if (objRel[currOp.objID].refTrans.size() > 0) {
                    Transaction* indepTrans = getTrans(objRel[currOp.objID].refTrans.back().transID);
                    if (currTrans->transID != indepTrans->transID) {
                        indepTrans->dependentTransactions.push_back(currTrans->transID);
                        currTrans->dependsOn.push_back(indepTrans->transID);
                        dependencies[currTrans->transID] = indepTrans->transID;
                    }
                    // Detect deadlock and store it. Abort when one of them tries to commit.
                    if (detectCycle(dependencies, &cycleTrans) && inCycle(currTrans->transID)) {
                        string deadlockMsg = makeDeadMsg(currOp);
                        Operation deadOp;
                        deadOp.opType = "D";
                        deadOp.deadlockMsg = deadlockMsg;
                        casOperations.push_back(deadOp);
                        cascadeAbort(getTrans(currOp.transID), true, "CAS");
                    } else {
                        // If there are no deadlock issues, simply stall reads until after commits
                        if (mustWait(currTrans, "CAS", currOp)) {
                            stallOps.push_back(currOp);
                        } else {
                            currTrans->operations.push_back(currOp);
                            casOperations.push_back(currOp);
                        }
                    }
                    cycleTrans.clear();
                } else {
                    if (mustWait(currTrans, "CAS", currOp)) {
                            stallOps.push_back(currOp);
                    } else {
                        currTrans->operations.push_back(currOp);
                        casOperations.push_back(currOp);
                    }
                }
            }
        } else if (currOp.opType == "A") {
            Transaction* currTrans = getTrans(currOp.transID);
            if (mustWait(currTrans, "CAS", currOp)) {
                stallOps.push_back(currOp);
            } else {
                currTrans->operations.push_back(currOp);
                casOperations.push_back(currOp);
                abortedTrans.insert(currOp.transID);   
                cascadeAbort(currTrans, false, "CAS");
                remDep(currTrans);
                updateStall("CAS");
            }
        } else if (currOp.opType == "C") {
            Transaction* currTrans = getTrans(currOp.transID);
            if (mustWait(currTrans, "CAS", currOp)) {
                stallOps.push_back(currOp);
            } else {
                currTrans->operations.push_back(currOp);
                casOperations.push_back(currOp);
                committedTrans.push_back(currOp.transID);
                remDep(currTrans);
                updateStall("CAS");
            }
        }
    }
    cout << endl;
    cout << "Cascadeless Recoverable" << endl;
    printOps(casOperations);
    clearAll();
}

int main (int argc, char** argv) {
    if (argc != 2) {
        cerr << "Please pass 1 input file to the program" << endl;
    }

    string line;
    ifstream inputFile(argv[1]);
    string token;

    while (getline(inputFile, line)) {
        int currProperty = 1;
        Operation currOp;
        if (line != "") {
            istringstream iss(line);
            while (iss >> token) {
                if (currProperty == 1) {
                    // TimeOffset
                    currOp.timeOffset = stoi(token);
                } else if (currProperty == 2) {
                    // TransactionID
                    int transNum = stoi(token.substr(1));
                    currOp.transID = transNum;
                } else if (currProperty == 3) {
                    // OperationType
                    currOp.opType = token;
                } else {
                    // ObjectID
                    currOp.objID = stoi(token.substr(1));
                }
                currProperty += 1;
            }
            operations.push_back(currOp);
        }
    }

    string inputFileName = argv[1];
    string outputFileName = inputFileName.substr(0, inputFileName.find_last_of('.')) + "_output.txt";
    ofstream out(outputFileName);
    streambuf* cbuf = cout.rdbuf();
    cout.rdbuf(out.rdbuf());

    // Sort the operations based on time
    sort(operations.begin(), operations.end(), compOperations);
    getRecSchedule(operations);
    getCascSchedule(operations);
    
    cout.rdbuf(cbuf);
}