#include "HeapTable.h"
#include "storage_engine.h"
#include <string>
#include <vector>
using namespace std;

class LockTable{
    public:
        // which type of lock a transaction currently holds: shared, exclusive, or none
        enum LockType{
            SHARED,
            EXCLUSIVE,
            NO_LOCK
        };

        // const string NO_TABLE_LOCKED = ""; // when initializing a LockTableRecord, use this if a transaction is not accessing any table

        LockTable();
        // ~LockTable();
        void insertIfNotExists(LockType lockType, Identifier tableToLock); // add a new record to the lock table. returns the handle of the new row
        void removeRecord(int transactionID); // remove the record with this transaction id from the lock table
        // Handles* select_(); // just for testing, remove later.
        void printRecords();


    private:
        // one record in the lock table
        struct LockTableRecord{
            int transactionID;
            LockType lockType; // whether this transaction is holding a shared lock, an exclusive lock, or none, on a table
            string tableLocked; // the table being locked by the lock, if any (pass "" if no table locked)
        };
        
        vector<LockTableRecord> table; // the lock table, which is a list of LockTableRecords
        // HeapTable* table;
        int lastID; // last transaction ID currently in the table. Needs to be int32_t for compatibility w/ Value()
        // const ColumnNames COLUMN_NAMES = {"transaction_id", "lock_type", "table_locked"};
        // const ColumnAttributes COL_ATTRS = {ColumnAttribute::TEXT, ColumnAttribute::TEXT, ColumnAttribute::TEXT};
        // const Identifier TABLE_NAME = "lock_table";

        bool recordExists(int transactionID);
};