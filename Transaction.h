#ifndef TRANSACTION_H
#define TRANSACTION_H

#include <iostream>
#include "TransactionStatement.h"

class Transaction {
  public:
    void commitTransaction();
  
    void Rollback();
  
    void endTransaction();
    
    void beginTransaction();
};
