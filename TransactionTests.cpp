#include "Transactions.h"

using namespace std;

namespace TransactionTests{
    void testAll(){
        TransactionManager tm = TransactionManager();
        tm.begin_transaction();
        tm.begin_transaction();
        tm.commit_transaction();
        tm.rollback_transaction();
    }
}