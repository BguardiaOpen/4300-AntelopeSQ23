#ifndef TRANSACTIONS_H
#define TRANSACTIONS_H

#include <vector>
#include "../sql-parser/src/SQLParser.h"
#include "TransactionStatement.h"
#include "SQLExec.h"
#include <algorithm>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <fcntl.h>

// struct TransactionStatement;

using namespace std;
    class TransactionManager{

        // A record in the transaction log
        struct TransactionLogRecord{
            int transactionID;      // id of the transaction that executed the statement
            hsql::SQLStatement* statement; // the SQL statement that was executed, if any
            hsql::TransactionStatement* transactionStatement; // the transaction statement that was executed, if any
            vector<DbRelation*>* checkpointTables; // list of DbRelations at a checkpoint, if any
            vector<Identifier>* tblNames; // list of the names of all the tables at a checkpoint
        };

        private:
            std::vector<TransactionLogRecord> transactionLog; // the vector at index i is the transaction log for the transaction with id i
            std::vector<int> activeTransactions; // process IDs, starting from 0, of the transactions that are currently running
            int highestTransactionID ; // largest transaction ID that has been created out of all the transactions
            stack<int> transactionStack;
            vector<DbRelation*> currentTables; // list of DbRelations representing all the tables currently in the database
            vector<Identifier>* tableNames;
        public: 
            TransactionManager(){ highestTransactionID = -1; }
            void begin_transaction();
            void commit_transaction();
            void rollback_transaction();
            int getFD(Identifier tableName);
            void tryToGetLock(int transactionID, hsql::SQLStatement statement, int fd);
            void releaseLock(int transactionID, int fileDescriptor);
            int getCurrentTransactionID();

            // returns a list of the transactions that are currently active in the database system
            vector<int> getActiveTransactions(){ return activeTransactions; } 

            // For every SQLExec method that's called within a transaction, that method MUST call this to 
            // update the list of tables and table names stored in TransactionManager, but only AFTER all locks are released.
            // (This is done in SQLExec::releaseLock())
            void updateTablesAndNames(); 

    };

    // This is from the instructor code in SQLExec.h
    class TransactionManagerError : public std::runtime_error {
    public:
        explicit TransactionManagerError(std::string s) : runtime_error(s) {}
    };

#endif
