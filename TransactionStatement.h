#ifndef __TRANSACTION_STATEMENT_H__
#define __TRANSACTION_STATEMENT_H__

#include "SQLStatement.h"

// Note: Implementations of constructors and destructors can be found in statements.cpp.
namespace hsql {
  // Represents SQL Begin statements.
  // Example "BEGIN TRANSACTION;"
  struct TransactionStatement : SQLStatement {
    BeginStatement();
    virtual ~BeginStatement();
    
    enum ActionType {
      BEGIN,
      COMMIT,
      ROLLBACK
    };

    char* name;
    char* indexName;
    ActionType type;
  };

} // namespace hsql
#endif
