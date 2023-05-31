#ifndef __TRANSACTION_STATEMENT_H__
#define __TRANSACTION_STATEMENT_H__

// #include "SQLStatement.h"
// #include "Table.h"

// Note: Implementations of constructors and destructors can be found in statements.cpp.
namespace hsql {
  // Represents SQL Begin statements.
  // Example "BEGIN TRANSACTION;"
  struct TransactionStatement : SQLStatement {
    enum ActionType {
      BEGIN,
      COMMIT,
      ROLLBACK
    };
    
    TransactionStatement(ActionType transactionStatementType);
    virtual ~TransactionStatement();
    
    

    char* name;
    char* indexName;
    ActionType type;
  };

} // namespace hsql
#endif
