/**
 * @file SQLExec.h - SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Winter 2023"
 */
#pragma once

#include <exception>
#include <string>
#include <stack>
#include "SQLParser.h"
#include "SchemaTables.h"
#include "TransactionStatement.h"
using namespace hsql;

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
     * @param statement   the Hyrise AST of the SQL statement to execute
     * @returns           the query result (freed by caller)
     */
    static QueryResult *execute(const hsql::SQLStatement *statement);

protected:
    // stack for transaction commands
    static std::stack<TransactionStatement> transactionStack;

    // the one place in the system that holds the _tables and _indices tables
    static Tables *tables;
    static Indices *indices;

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

    static QueryResult *execute_transaction_command(const TransactionStatement *statement);

    static QueryResult *begin_transaction(const TransactionStatement *statement);

    static QueryResult *commit_transaction(const TransactionStatement *statement);

    static QueryResult *rollback_transaction(const TransactionStatement *statement);

    

    /**
     * Pull out column name and attributes from AST's column definition clause
     * @param col                AST column definition
     * @param column_name        returned by reference
     * @param column_attributes  returned by reference
     */
static QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);

    ColumnNames *column_names = new ColumnNames();
    column_names->push_back("table_name");
    column_names->push_back("column_name");
    column_names->push_back("data_type");
    
    ColumnAttributes *column_attributes = new ColumnAttributes();
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    //get handles from tables
    ValueDict where;
    where["table_name"] = Value(statement->tableName);
    Handles *handles = columns.select(&where);
    ValueDicts *rows = new ValueDicts();

    for (auto const &handle : *handles) {
        ValueDict *row = columns.project(handle, column_names);
        rows->push_back(row);
    }

    delete handles;
    return new QueryResult(column_names, column_attributes, rows, "showing columns");
}

static QueryResult *SQLExec::show_index(const ShowStatement *statement) {
    ColumnNames *colNames = new ColumnNames;
    ColumnAttributes *colAttr = new ColumnAttributes;

    colNames->push_back("table_name");
    colAttr->push_back(ColumnAttribute::TEXT);
    
    colNames->push_back("index_name");
    colAttr->push_back(ColumnAttribute::TEXT);
    
    colNames->push_back("column_name");
    colAttr->push_back(ColumnAttribute::TEXT);
    
    colNames->push_back("seq_in_index");
    colAttr->push_back(ColumnAttribute::INT);

    colNames->push_back("index_type");
    colAttr->push_back(ColumnAttribute::TEXT);

    colNames->push_back("is_unique");
    colAttr->push_back(ColumnAttribute::BOOLEAN);

    ValueDict location;
    location["table_name"] = Value(statement->tableName);
    Handles *handleList = SQLExec::indices->select(&location);

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle : *handleList) {
        ValueDict *row = SQLExec::indices->project(handle, colNames);
        rows->push_back(row);
    }
    delete handleList;
    return new QueryResult(colNames, colAttr, rows, "showing indices");
}


// Preconditions: 1) user must specify a value for each column, i.e. there are no nullable columns
//                2) only supports int and text;
//                   the values being inserted can only be literal strings or integers (hsql doesn't support booleans)
static QueryResult *SQLExec::insert(const InsertStatement *statement) {
    // check if the table exists
    ValueDict where;
    where["table_name"] = Value(statement->tableName);
    Handles* handles = tables->select(&where);

    if(handles->empty()){
        delete handles;
        handles = nullptr;
        return new QueryResult("Error: table does not exist");
    }

    delete handles; 

    // construct the ValueDict, making sure it's in the same order as the order of columns in the table
    ValueDict rowToInsert;
    ColumnNames colNames; // column names for the table that the row will be inserted into
    ColumnAttributes colAttributes; // column attributes of that table
    Expr* expr; // expressions for the values in the statement
    Value valueToInsert;
    string message = "Successfully inserted 1 row into table "; // message returned in QueryResult

    // get the order of the columns in the table 
    tables->get_columns(statement->tableName, colNames, colAttributes);

    // if there's no list of columns specified, the order is the same as the order of columns in the table
    if(statement->columns == nullptr){
        for(unsigned int i=0; i < statement->values->size(); i++){
            expr = statement->values->at(i);

            // convert the literal string or int from statement->values into a Value type
            if((expr->type == ExprType::kExprLiteralInt))
                valueToInsert = Value(expr->ival); 
            if((expr->type == ExprType::kExprLiteralString))
                valueToInsert = Value(expr->getName());

            // add the pair to the end of rowToInsert in the right order
            rowToInsert.insert(rowToInsert.end(), {colNames[i], valueToInsert});
        }
    }
    else{ // otherwise, construct the ValueDict in the right order
        for(unsigned int i=0; i < colNames.size(); i++){
            // for column i in the table, find the index position of that column in statement->columns.
            // find() returns an iterator to the position where colNames[i] appears in statement->columns
            vector<char*>::iterator it = find(statement->columns->begin(), statement->columns->end(), colNames[i]);

            // get the index of column i in the statement. distance() returns the number of increments/"hops" between two iterators.
            // The distance between columns.begin() and it is the same as the index position of it.
            // The index position of it in columns corresponds to the same index position in values, so we can use it as an index for values
            int indexInStatement = distance(statement->columns->begin(), it);

            expr = statement->values->at(indexInStatement);

            // convert the literal string or int from statement->values into a Value type
            if((expr->type == ExprType::kExprLiteralInt))
                valueToInsert = Value(expr->ival); 
            if((expr->type == ExprType::kExprLiteralString))
                valueToInsert = Value(expr->getName());

            // add the pair to the end of rowToInsert in the right order
            rowToInsert.insert(rowToInsert.end(), {colNames[i], valueToInsert});
            cout << "end of iteration"<<endl;
        }
    }

    cout << "Row to insert: ";
    for(auto elem : rowToInsert) {
    cout << elem.first << ", " 
         << (elem.second.data_type == ColumnAttribute::DataType::INT ? to_string(elem.second.n) :
            elem.second.s) << " | ";
    }

    // insert the row into the table
    DbRelation& table = tables->get_table(statement->tableName); // the relation for the table

    // check if the row already exists in the table    
    handles = table.select(&rowToInsert);
    if(!handles->empty()){
        delete handles;
        return new QueryResult("Error: The row already exists in the table");
    }

    delete handles;
    table.insert(&rowToInsert);

    // insert into any indices
    IndexNames indexNames = indices->get_index_names(statement->tableName);
    int numIndices = indexNames.size();

    if(numIndices > 0){
        ValueDict indexRowToInsert; // the row to insert into _indices

        for(string indexName : indexNames){
            // don't need to check if the index exists since it's in indexNames
            DbIndex& index = indices->get_index(statement->tableName, indexName);

            // to insert into the index, need to get a handle to the row to insert, so call select 
            // on the row just inserted.
            // use rowToInsert as the where condition since it already specifies the values
            handles = table.select(&rowToInsert);

            // handles should contain only one row
            index.insert((*handles)[0]);
            delete handles;
        }
    }

    handles = nullptr;

    // we had to add each substring individually since there were compilation errors because the data types were different
    message += statement->tableName;
    if(numIndices > 0){
        message += " and ";
        message += to_string(numIndices);
        message += (numIndices == 1 ? " index" : " indices");
    }

    return new QueryResult(message);
}

// The SELECT should translate into an evaluation plan with a projection 
// plan on a select plan. The enclosed select plan should be annotated with a table scan.
// For select, your job is to create an evaluation plan and execute it. Start the plan 
// with a TableScan:
 
// Note that we then wrap that in a Select plan. The get_where_conjunction is just a 
// local recursive 
// function that pulls apart the parse tree to pull out any equality predicates and 
// AND operations (other conditions throw an exception). Next we would wrap the 
// whole thing in either a ProjectAll or a Project. Note that I've added a new method 
// to DbRelation, get_column_attributes(ColumnNames) that will get the attributes (for
//  returning in the QueryResults) based on the projected column names.
// Next we optimize the plan and evaluate it to get our results for the user.

// Precondition: for now, no nested queries/select statements; you can only select from a table.
static QueryResult *SQLExec::select(const SelectStatement *statement) {
    // throw an error if statement->fromTable is not a table name
    if(statement->fromTable->type != TableRefType::kTableName)
        return new QueryResult("Error: only selecting from a single table is supported");

    cout << "in select" << endl;
    Identifier tableName = statement->fromTable->getName(); // name of table to select from
    DbRelation& table = tables->get_table(tableName); // get the DbRelation for the table
    TableScanPlan tableScan = TableScanPlan(&table); // start with table scan
    SelectPlan selectPlan = SelectPlan(&tableScan);    // wrap that in a SelectPlan

    // determine whether all columns are being selected or only some
    ColumnNames colsToSelect;
    ColumnAttributes colAttrs;
    bool selectAllColumns = false; // whether all columns are selected

    cout << "length: " << statement->selectList->size() << endl;

    for(Expr* expr : *statement->selectList){ 
        cout << expr->expr << endl;
        cout << "type: " << expr->type << endl; // type of expression

    }

    // get list of columns to project or "*"    
    for(long unsigned int i = 0; i < statement->selectList->size(); i++){
        Expr* expr = (*statement->selectList)[i];
        hsql::ExprType exprType = expr->type; // type of expression

        // assume the only types can be kExprStar and kExprColumnRef for now
        // if the statement is "SELECT *", then the length of the select list would be 1.
        // otherwise, get all column names.
        if(exprType == kExprStar)
            selectAllColumns = true;
        else
            colsToSelect.push_back(expr->getName());

        // delete expr;
    }

    cout << "end of loop" << endl;

    // re-declaring variables since there's no empty ctor or assignment operator for EvalPlan
    if(!selectAllColumns){
        cout << "not selecting all cols" << endl;

        EvalPlan projection = EvalPlan(false, colsToSelect, &selectPlan);
        
        // cout << "optimizing" << endl;
        // EvalPlan optimizedPlan = projection.optimize();
        
        cout << "evaluating" << endl;
        ValueDicts result = projection.evaluate();

        cout << "getting cols" << endl;

        tables->get_columns(tableName, colsToSelect, colAttrs); // get all column names since we didn't get them in the loop
        cout << "returning from SQLExec::Select()" << endl;
        return new QueryResult(&colsToSelect, &colAttrs, &result, SUCCESS_MESSAGE);
    }

    cout << "selecting all cols" << endl;
        
    EvalPlan projection = EvalPlan(true, ColumnNames(), &selectPlan);

    cout << "Getting col attrs" << endl;
    colAttrs = *tables->get_column_attributes(colsToSelect);

    cout << "optimizing" << endl;
    // EvalPlan optimizedPlan = projection.optimize();

    cout << "evaluating" << endl;
    ValueDicts result = projection.evaluate();

    cout << "returning" << endl;
    return new QueryResult(&colsToSelect, &colAttrs, &result, SUCCESS_MESSAGE);
}

static QueryResult *SQLExec::execute_transaction_command(const TransactionStatement *statement){
    switch(statement->type){
        case TransactionStatement::BEGIN:
            return begin_transaction(statement);
        case TransactionStatement::COMMIT:
            return commit_transaction(statement);
        case TransactionStatement::ROLLBACK:
            return rollback_transaction(statement);
        default:
            throw SQLExecError("invalid transaction type");
    }
}

QueryResult *SQLExec::begin_transaction(const TransactionStatement *statement){
    transactionStack.push(*statement);
    return new QueryResult("Opened transaction level " + transactionStack.size());
}

QueryResult *SQLExec::commit_transaction(const TransactionStatement *statement){
    if(transactionStack.empty())
        throw SQLExecError("Attempted to commit a transaction when there are no transactions pending");
    
    int oldNumTransactions = transactionStack.size(); // number of transactions before popping stack
    transactionStack.pop();


    return new QueryResult("Transaction level " + to_string(oldNumTransactions) + " committed, " + 
                            (transactionStack.empty() ? "no" : to_string(transactionStack.size()))
                            + " transactions pending");
}

QueryResult *SQLExec::rollback_transaction(const TransactionStatement *statement){
    if(transactionStack.empty())
        throw SQLExecError("Attempted to roll back a transaction when there are no transactions pending");
    
    int oldNumTransactions = transactionStack.size(); // number of transactions before popping stack
    transactionStack.pop();

    return new QueryResult("Transaction level " + to_string(oldNumTransactions) + " rolled back, " + 
                            (transactionStack.empty() ? "no" : to_string(transactionStack.size()))
                            + " transactions pending");
}
};

