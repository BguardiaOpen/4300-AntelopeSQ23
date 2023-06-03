#include "Transactions.h"
using namespace std;
using namespace hsql;

void TransactionManager::begin_transaction(){
    highestTransactionID++;
    activeTransactions.push_back(highestTransactionID);

    // add begin to log
}

void TransactionManager::commit_transaction(int transactionID){
    // check if transactionID exists, then remove it
    vector<int>::iterator it = find(activeTransactions.begin(), activeTransactions.end(), transactionID);

    if(it != activeTransactions.end()){
        activeTransactions.erase(it);
    }

    // add commit to log
}

void TransactionManager::rollback_transaction(int transactionID){
    vector<int>::iterator it = find(activeTransactions.begin(), activeTransactions.end(), transactionID);
    if(it == activeTransactions.end()){
        return;
    }
    
    // reverse data changes
    // erase transaction log
    // remove from activeTransactions
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
        throw TransactionManagerError("Error in getFD(): "+errno);
    

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
        cout << "Successfully got the lock" << endl;
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