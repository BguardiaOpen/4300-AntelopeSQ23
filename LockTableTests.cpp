#include "LockTable.h"
namespace LockTableTests{
    void runAll(){
        LockTable lt = LockTable();

        for(int i=1; i < 3; i++){
            lt.addRecord(LockTable::LockType::EXCLUSIVE, "new_table_name");
            // Handles* handles = lt.select();
            // delete handles;
        }

        for(int i=1; i < 3; i++){
            lt.removeRecord(i);
            Handles* handles = lt.select();
            delete handles;
        }
    }
}