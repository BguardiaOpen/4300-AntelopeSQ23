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

using namespace std;
    class TransactionManager{

        // A record in the transaction log
        struct TransactionLogRecord{
            int transactionID;      // id of the transaction that executed the statement
            hsql::SQLStatement statement; // the statement that was executed
        };

        private:
            std::vector<TransactionLogRecord> transactionLog; // the vector at index i is the transaction log for the transaction with id i
            std::vector<int> activeTransactions; // process IDs, starting from 0, of the transactions that are currently running
            int highestTransactionID ; // largest transaction ID that has been created out of all the transactions
            stack<int> transactionStack;

        public: 
            TransactionManager(){ highestTransactionID = -1; }
            void begin_transaction();
            void commit_transaction();
            void rollback_transaction();
            int getFD(Identifier tableName);
            void tryToGetLock(int transactionID, hsql::SQLStatement statement, int fd);
            void releaseLock(int transactionID, int fileDescriptor);
            int getCurrentTransactionID();
            // returns a list of the transactions that are currently active in the databaset system
            vector<int> getActiveTransactions(){ return activeTransactions; } 
    };

    // This is from the instructor code in SQLExec.h
    class TransactionManagerError : public std::runtime_error {
    public:
        explicit TransactionManagerError(std::string s) : runtime_error(s) {}
    };

#endif
