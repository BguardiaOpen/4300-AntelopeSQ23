#include "Transactions.h"
using namespace std;
using namespace hsql;

void TransactionManager::begin_transaction(){
    highestTransactionID++;
    activeTransactions.push_back(highestTransactionID); // add new ID to active transactions
    TransactionLogRecord record = {highestTransactionID, TransactionStatement(TransactionStatement::BEGIN)};
    transactionLog.push_back(record); // add begin to log

    // add a new transaction to the stack
    transactionStack.push(highestTransactionID);
    cout << "Opened transaction level " << transactionStack.size() << endl;
    cout << "ID of the transaction that just begun: " << highestTransactionID << endl;
}

// commits the current transaction (the one at the top of the stack)
void TransactionManager::commit_transaction(){
    if(transactionStack.empty())
        throw TransactionManagerError("Attempted to commit a transaction when there are no transactions pending");

    int id = transactionStack.top(); // id of the transaction to commit

    vector<int>::iterator it = find(activeTransactions.begin(), activeTransactions.end(), id);
    if(it == activeTransactions.end())
        throw TransactionManagerError("Transaction not found in transaction log");

    activeTransactions.erase(it);

    // add commit to log
    TransactionLogRecord record = {id, TransactionStatement(TransactionStatement::COMMIT)};
    transactionLog.push_back(record); 

    // update stack
    int oldNumTransactions = transactionStack.size(); // number of transactions before popping stack
    transactionStack.pop();

    cout << "Transaction level " << oldNumTransactions << " committed, " <<
                            (transactionStack.empty() ? "no" : to_string(transactionStack.size()))
                            << " transactions pending" << endl;
    cout << "ID of the transaction that was just committed: " << id << endl;
}

// rolls back the current transaction (the one at the top of the stack)
void TransactionManager::rollback_transaction(){
    if(transactionStack.empty())
        throw TransactionManagerError("Attempted to roll back a transaction when there are no transactions pending");
    
    int id = transactionStack.top(); // id of the transaction to roll back

    vector<int>::iterator it = find(activeTransactions.begin(), activeTransactions.end(), id);
    if(it == activeTransactions.end()){
        throw TransactionManagerError("Transaction not found in transaction log");
    }
    
    // reverse data changes
    // erase transaction log
    activeTransactions.erase(it); // remove from activeTransactions

    // add rollback to log
    TransactionLogRecord record = {id, TransactionStatement(TransactionStatement::ROLLBACK)};
    transactionLog.push_back(record); 

    // update stack
    int oldNumTransactions = transactionStack.size(); // number of transactions before popping stack
    transactionStack.pop();
    

    cout << "Transaction level " << oldNumTransactions << " rolled back, " <<
                            (transactionStack.empty() ? "no" : to_string(transactionStack.size()))
                            << " transactions pending" << endl;
    cout << "ID of the transaction that was just rolled back: " << id << endl;
}

// returns the file descriptor for a database table file
// @param tableName: the table for which to get the file descriptor
int TransactionManager::getFD(Identifier tableName){
    // TODO: get the name of the data directory from the driver
    string filename = "data/";
    filename += tableName;
    filename += ".db";
    int fd = open(filename.c_str(), O_RDONLY); // get the file descriptor

    if(fd == -1)
        throw TransactionManagerError("Error: failed to get a file descriptor for table "+tableName);
    

    return fd;
}

// tries to get a lock
// @param transactionID: ID of the transaction that's requesting the lock
// @param fd: table to lock

void TransactionManager::tryToGetLock(int transactionID, SQLStatement statement, int fd){
    // determine whether to get a shared or exclusive lock based on the type of statement
    bool shared = (statement.type() == kStmtSelect || statement.type() == kStmtShow ? true : false);

    // try to lock the database file
    int errorCode = flock(fd, (shared ? LOCK_SH : LOCK_EX) | LOCK_NB);
    
    if(errorCode == -1){ // if not successful
        // EWOULDBLOCK means that the file is locked, so this transaction needs to wait
        if(errno == EWOULDBLOCK){
            cout << "Waiting..." << endl;
        }else{
            throw TransactionManagerError("Error: " + errno);
        }
    }else{
        cout << "Successfully got the lock on the file with FD" << fd << endl;
    }
}

// @param transactionID: 
// @param fileDescriptor: the file descriptor of the file that's locked by this transaction
void TransactionManager::releaseLock(int transactionID, int fileDescriptor){
    if(fileDescriptor < 0)
        throw TransactionManagerError("Error: passed a file descriptor that's less than 0");

    // call flock() with LOCK_UN to remove the lock on the file
    int errorCode = flock(fileDescriptor, LOCK_UN | LOCK_NB);
    if(errorCode == -1) 
        throw TransactionManagerError("Error with releasing lock: " + errno);
    else cout << "Successfully released the lock" << endl;

    errorCode = close(fileDescriptor);
    if(errorCode == -1)
        throw TransactionManagerError("Error with closing the FD: " + errno);
    else cout << "Successfully closed the file" << endl;
}

// returns ID of the transaction that's currently executing, which is the one
// most recently pushed to the stack. Returns -1 if there are no transactions executing
int TransactionManager::getCurrentTransactionID(){
    return (transactionStack.empty() ? -1 : transactionStack.top());
}
void TransactionManager::checkpoint(int transactionID, DbRelation& table)
{
    vector<int> transArr;
    transArr.push_back(transactionID);
    
    vector<DbRelation table> tableArr;
    tableArr.push_back(tableArr);
}

int TransactionManager::retrieveTransaction(int transactionID)
{
    int id = 0;
    for(int i = 0; i < transArr.size(); i++)
    {
        if(transArr[i] == transactionID)
        {
            id = transactionID;
        }
    }
    return id;
}
