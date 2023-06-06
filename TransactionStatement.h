#ifndef TRANSACTION_STATEMENT_H
#define TRANSACTION_STATEMENT_H

#include "../sql-parser/src/sql/SQLStatement.h"

  // Represents SQL transaction statements.
  // Example "BEGIN TRANSACTION;"

// Precondition: CHECKPOINT should not be used by the client; that is only for the DBMS transaction manager to use
namespace hsql{
  struct TransactionStatement : hsql::SQLStatement {
    enum ActionType {
      BEGIN,
      COMMIT,
      ROLLBACK,
      CHECKPOINT
    };
    
    // Calling SQLStatement with insert just as a placeholder since I don't want to mess with the parser and I had to call
    // SQLStatement for this to compile
    TransactionStatement(ActionType transactionStatementType) : SQLStatement(hsql::StatementType::kStmtInsert), type(transactionStatementType){}
    virtual ~TransactionStatement(){}

    ActionType type;
  };

}// namespace hsql
#endif
