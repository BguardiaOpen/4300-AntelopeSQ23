#include "LockTable.h"
namespace LockTableTests{
    void testInsert(){
        LockTable lt = LockTable();
        LockTable::LockTableRecord record;
        record.transactionID = 1;
        record.lockType = LockTable::LockType::NO_LOCK;
        record.tableLocked = "";
        lt.addRecord(record);
        Handles* handles = lt.select();
        delete handles;
    }

    void testDelete(){

    }

    void runAll(){
        testInsert();
        testDelete();
    }
}