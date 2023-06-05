#ifndef TRANSACTION_STATEMENT_H
#define TRANSACTION_STATEMENT_H

#include "../sql-parser/src/sql/SQLStatement.h"
// #include "../sql-parser/Table.h"

// Note: Implementations of constructors and destructors can be found in statements.cpp.
  // Represents SQL Begin statements.
  // Example "BEGIN TRANSACTION;"
namespace hsql{
  struct TransactionStatement : hsql::SQLStatement {
    enum ActionType {
      BEGIN,
      COMMIT,
      ROLLBACK
    };
    
    // Calling SQLStatement with insert just as a placeholder since I don't want to mess with the parser and I had to call
    // SQLStatement for this to compile
    TransactionStatement(ActionType transactionStatementType) : SQLStatement(hsql::StatementType::kStmtInsert), type(transactionStatementType){}
    virtual ~TransactionStatement(){}

    ActionType type;
  };

}// namespace hsql
#endif
