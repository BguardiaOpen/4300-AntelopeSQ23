/**
 * @file SQLExec.h - SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Winter 2023"
 */
#pragma once
class TransactionManager; // forward declaring to avoid compilation error


#include <exception>
#include <string>
#include <stack>
#include "SQLParser.h"
#include "SchemaTables.h"
#include "TransactionStatement.h"
#include "Transactions.h"
using namespace hsql;
using namespace std;

const string SUCCESS_MESSAGE = "Successful query result"; // message to return in a QueryResult when it's successful

/**
 * @class SQLExecError - exception for SQLExec methods
 */
class SQLExecError : public std::runtime_error {
public:
    explicit SQLExecError(std::string s) : runtime_error(s) {}
};


/**
 * @class QueryResult - data structure to hold all the returned data for a query execution
 */
class QueryResult {
public:
    QueryResult() : column_names(nullptr), column_attributes(nullptr), rows(nullptr), message("") {}

    QueryResult(std::string message) : column_names(nullptr), column_attributes(nullptr), rows(nullptr),
                                       message(message) {}

    QueryResult(ColumnNames *column_names, ColumnAttributes *column_attributes, ValueDicts *rows, std::string message)
            : column_names(column_names), column_attributes(column_attributes), rows(rows), message(message) {}

    virtual ~QueryResult();

    ColumnNames *get_column_names() const { return column_names; }

    ColumnAttributes *get_column_attributes() const { return column_attributes; }

    ValueDicts *get_rows() const { return rows; }

    const std::string &get_message() const { return message; }

    friend std::ostream &operator<<(std::ostream &stream, const QueryResult &qres);

protected:
    ColumnNames *column_names;
    ColumnAttributes *column_attributes;
    ValueDicts *rows;
    std::string message;
};


/**
 * @class SQLExec - execution engine
 */
class SQLExec {
public:
    /**
     * Execute the given SQL statement.
     * Precondition: Do NOT call this with a transaction statement
     * @param statement   the Hyrise AST of the SQL statement to execute
     * @returns           the query result (freed by caller)
     */
    static QueryResult *execute(const hsql::SQLStatement *statement);

    /**
     * Execute the given transaction statement.
     * @param statement   the transaction statement to execute
     * @returns           the query result (freed by caller)
     */
    static QueryResult *execute_transaction_command(const TransactionStatement *statement);

    // To help the TransactionManager: return a pair of DbRelations of all tables in the DBMS,
    // and their names.
    static pair<vector<DbRelation*>, vector<Identifier>*> saveTablesAndNames(); 

protected:
    // the one place in the system that holds the _tables and _indices tables
    static Tables *tables;
    static Indices *indices;
    static TransactionManager tm; 

    // recursive decent into the AST
    static QueryResult *create(const hsql::CreateStatement *statement);

    static QueryResult *drop(const hsql::DropStatement *statement);

    static QueryResult *show(const hsql::ShowStatement *statement);

    static QueryResult *show_tables();

    static QueryResult *show_columns(const hsql::ShowStatement *statement);

    static QueryResult *drop_index(const hsql::DropStatement *statement);

    static QueryResult *show_index(const hsql::ShowStatement *statement);

    static QueryResult *insert(const hsql::InsertStatement *statement);

    static QueryResult *del(const hsql::DeleteStatement *statement);

    static QueryResult *select(const hsql::SelectStatement *statement);

    static pair<int, int> requestLock(SQLStatement* stmt, Identifier tableToAccess);

    // If a transaction is executing and has a lock, this releases the lock. If no transactions, does nothing.
    // @param fdAndID: a pair with the first element being the file descriptor of the DB file being accessed by the statement,
    //                 and the second element being the ID of the transaction that's executing a statement
    static void releaseLock(pair<int, int> fdAndID);

    static void
    column_definition(const hsql::ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute);


};

