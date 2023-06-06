#include "TransactionTests.h"

using namespace std;

namespace TransactionTests{
    void testAll(){
        cout << "Testing transaction stack" << endl;
        TransactionManager tm = TransactionManager();
        tm.begin_transaction();
        tm.begin_transaction();
        tm.commit_transaction();
        tm.begin_transaction();
        tm.rollback_transaction();
        tm.rollback_transaction();
        cout << "Tests passed!" << endl;
    }
}