#include "TransactionTests.h"

using namespace std;

namespace TransactionTests{
    void testAll(){
        TransactionManager tm = TransactionManager();
        DbRelation table = DbRelation();
        tm.begin_transaction();
        tm.begin_transaction();
        tm.commit_transaction();
        tm.rollback_transaction();
        
        cout << "File Descriptor: " << tm.getFD();
        
        // Test for determining whether shared or exclusive
        // lock should be used
        tm.tryToGetLock();
        
        // Test for releasing lock
        tm.releaseLock();
    }
}
