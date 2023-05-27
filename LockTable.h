#include "HeapTable.h"
#include "storage_engine.h"
#include <string>
#include <vector>
using namespace std;

class LockTable{
    private:
        HeapTable* table;
        const ColumnNames COLUMN_NAMES = {"transaction_id", "lock_type", "table_locked"};
        const ColumnAttributes COL_ATTRS = {ColumnAttribute::TEXT, ColumnAttribute::TEXT, ColumnAttribute::TEXT};
        const Identifier TABLE_NAME = "lock_table";

    public:
        // which type of lock a transaction currently holds: shared, exclusive, or none
        enum LockType{
            SHARED,
            EXCLUSIVE,
            NO_LOCK
        };

        // A record in the lock table
        struct LockTableRecord{
            int transactionID;
            LockType lockType;
            string tableLocked; // the table locked by the transaction, if any
        };

        // const string NO_TABLE_LOCKED = ""; // when initializing a LockTableRecord, use this if a transaction is not accessing any table

        LockTable();
        Handle addRecord(LockTableRecord record); // add a new record to the lock table. returns the handle of the new row
        void removeRecord(int transactionID); // remove the record with this transaction id from the lock table
        Handles* select(); // just for testing, remove later.

};