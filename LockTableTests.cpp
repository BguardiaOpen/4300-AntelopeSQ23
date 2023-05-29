#include "LockTable.h"
namespace LockTableTests{
    void runAll(){
        LockTable lt = LockTable();

        for(int i=1; i < 3; i++){
            cout << "Adding record " << i << endl;
            lt.insertIfNotExists(LockTable::LockType::EXCLUSIVE, "foo_bar");
            lt.printRecords();
        }

        for(int i=1; i < 3; i++){
            cout << "Deleting record " << i << endl;
            lt.removeRecord(i);
            lt.printRecords();
        }
    }
}