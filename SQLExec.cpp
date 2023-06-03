/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Winter 2023"
 */
#include "SQLExec.h"
#include "ParseTreeToString.h"
#include "SchemaTables.h"
#include "EvalPlan.h"
#include "storage_engine.h"
#include <iostream>

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;
Indices *SQLExec::indices = nullptr;

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
                        break;
                    default:
                        out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
    return out;
}

QueryResult::~QueryResult() {
    //just in case the pointers are nullptr
    if(this->column_names)
        delete column_names;

    if(this->column_attributes)
        delete column_attributes;
    
    if(this->rows) {
        for (auto row: *rows)
            delete row;
        delete rows;
    }
}


/**
 * @brief Executes create, drop, and show statements
 * 
 * @param statement the statement to be executed
 * @return QueryResult* the result of the statement
 */
QueryResult *SQLExec::execute(const SQLStatement *statement) {
    cout << endl << "in SQLExec::Execute()" << endl;
    // Initializes _tables table if not null
    if (SQLExec::tables == nullptr) {
        SQLExec::tables = new Tables();
        SQLExec::indices = new Indices();
    }

    // initialize stack
    // transactionStack = TransactionStack();
    
    cout << endl << "Entering try block" << endl;
    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            case kStmtInsert:
                return insert((const InsertStatement *) statement);
            case kStmtSelect:
                cout << "In select case of switch stmt" << endl;
                return select((const SelectStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError &e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

/**
 * @brief Sets up the column definitions
 * 
 * @param col the column to be changed
 * @param column_name name of the column
 * @param column_attribute type of the column
 */
void SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute) {
    column_name = col->name;
    switch(col->type) {
        case ColumnDefinition::INT:
            column_attribute.set_data_type(ColumnAttribute::INT);
            break;
        case ColumnDefinition::TEXT:
            column_attribute.set_data_type(ColumnAttribute::TEXT);
            break;
        default:
            throw SQLExecError("Column type not supported");
    }
}

/**
 * @brief Executes a create statement
 * 
 * @param statement the create statement to be executed
 * @return QueryResult* the result of the create statement
 */
QueryResult *SQLExec::create(const CreateStatement *statement) {
    //check create type (future proofed for other types)
    switch(statement->type) {
        // ATTRIBUTION: we copied create table from Prof. Lundeen's solution repo
        case CreateStatement::kTable: 
            { 
                Identifier table_name = statement->tableName;
    ColumnNames column_names;
    ColumnAttributes column_attributes;
    Identifier column_name;
    ColumnAttribute column_attribute;
    for (ColumnDefinition *col : *statement->columns) {
        column_definition(col, column_name, column_attribute);
        column_names.push_back(column_name);
        column_attributes.push_back(column_attribute);
    }

    // Add to schema: _tables and _columns
    ValueDict row;
    row["table_name"] = table_name;
    Handle t_handle = SQLExec::tables->insert(&row);  // Insert into _tables
    try {
        Handles c_handles;
        DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
        try {
            for (uint i = 0; i < column_names.size(); i++) {
                row["column_name"] = column_names[i];
                row["data_type"] = Value(column_attributes[i].get_data_type() == ColumnAttribute::INT ? "INT" : "TEXT");
                c_handles.push_back(columns.insert(&row));  // Insert into _columns
            }

            // Finally, actually create the relation
            DbRelation &table = SQLExec::tables->get_table(table_name);
            if (statement->ifNotExists)
                table.create_if_not_exists();
            else
                table.create();

        } catch (...) {
            // attempt to remove from _columns
            try {
                for (auto const &handle: c_handles)
                    columns.del(handle);
            } catch (...) {}
            throw;
        }

    } catch (exception &e) {
        try {
            // attempt to remove from _tables
            SQLExec::tables->del(t_handle);
        } catch (...) {}
        throw;
    }
    return new QueryResult("created " + table_name);
            }
            // ATTRIBUTION: We copied the following code for CREATE INDEX from
            // the Milestone 4 tag of Prof. Lundeen's solution repository
        case CreateStatement::kIndex:
            {
                Identifier index_name = statement->indexName;
                Identifier table_name = statement->tableName;

                // get underlying relation
                DbRelation &table = SQLExec::tables->get_table(table_name);

                // check that given columns exist in table
                const ColumnNames &table_columns = table.get_column_names();
                for (auto const &col_name: *statement->indexColumns)
                    if (std::find(table_columns.begin(), table_columns.end(), col_name) == table_columns.end())
                        throw SQLExecError(std::string("Column '") + col_name + "' does not exist in " + table_name);

                // insert a row for every column in index into _indices
                ValueDict row;
                row["table_name"] = Value(table_name);
                row["index_name"] = Value(index_name);
                row["index_type"] = Value(statement->indexType);
                row["is_unique"] = Value(std::string(statement->indexType) == "BTREE"); // assume HASH is non-unique --
                int seq = 0;
                Handles i_handles;
                try {
                    for (auto const &col_name: *statement->indexColumns) {
                        row["seq_in_index"] = Value(++seq);
                        row["column_name"] = Value(col_name);
                        i_handles.push_back(SQLExec::indices->insert(&row));
                    }

                    DbIndex &index = SQLExec::indices->get_index(table_name, index_name);
                    index.create();

                } catch (...) {
                    // attempt to remove from _indices
                    try {  // if any exception happens in the reversal below, we still want to re-throw the original ex
                        for (auto const &handle: i_handles)
                            SQLExec::indices->del(handle);
                    } catch (...) {}
                    throw;  // re-throw the original exception (which should give the client some clue as to why it did
                }
                return new QueryResult("created index " + index_name);
            }
        default: 
            return new QueryResult("Only CREATE TABLE and CREATE INDEX supported"); 
    }
    return nullptr;
}

/**
 * @brief Executes a drop statement
 * 
 * @param statement the statement to be executed
 * @return QueryResult* the result of the drop statement
 */
QueryResult *SQLExec::drop(const DropStatement *statement) {
    switch(statement->type) {
        case DropStatement::kTable:
            //new scope block to prevent any scoping issues/warnings with "default"
            {
                //check table is not a schema table
                Identifier tableName = statement->name;
                if(tableName == Tables::TABLE_NAME || tableName == Columns::TABLE_NAME)
                    throw SQLExecError("Error: schema tables cannot be dropped");


                DbRelation &table = SQLExec::tables->get_table(tableName);
                ValueDict where;
                where["table_name"] = Value(tableName);

                //remove indices
                for(const auto &name : SQLExec::indices->get_index_names(tableName)) {
                    DbIndex &index = SQLExec::indices->get_index(tableName, name);
                    index.drop();
                }
                Handles* indexHandles = SQLExec::indices->select(&where);
                for(const auto &handle : *indexHandles)
                    SQLExec::indices->del(handle);
                delete indexHandles;


                //remove columns
                DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
                Handles *columnHandles = columns.select(&where);
                for(const auto &handle : *columnHandles) 
                    columns.del(handle);
                delete columnHandles;


                //drop table and remove from schema
                table.drop();
                Handles* tableHandles = SQLExec::tables->select(&where);
                SQLExec::tables->del(*tableHandles->begin());
                delete tableHandles;
            }
            return new QueryResult("Table successfully dropped!");
        case DropStatement::kIndex:
            {
                Identifier tableName = statement->name;
                Identifier indexName = statement->indexName;

                DbIndex &index = SQLExec::indices->get_index(tableName, indexName);
                index.drop();
                
                // remove from indices table
                ValueDict location;
                location["table_name"] = Value(tableName);
                location["index_name"] = Value(indexName);
                Handles *handleList = SQLExec::indices->select(&location);
                for (Handle &handle : *handleList) {
                    SQLExec::indices->del(handle);
                }
                delete handleList;

                return new QueryResult("Index successfully dropped");
            }
        default:
            return new QueryResult("only DROP TABLE and DROP INDEX implemented"); // FIXME
    }
}

/**
 * @brief Executes a show statement
 * 
 * @param statement the statement to be executed
 * @return QueryResult* the result of the show statement
 */
QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch (statement->type) {
        case ShowStatement::kTables:
            return show_tables();
        case ShowStatement::kColumns:
            return show_columns(statement);
        case ShowStatement::kIndex:
            return show_index(statement);
        default:
            throw SQLExecError("invalid show type");
    }
}

/**
 * @brief Shows all tables
 * 
 * @return QueryResult* the result of the show
 */
QueryResult *SQLExec::show_tables() {
    ColumnNames *colNames = new ColumnNames();
    colNames->push_back("table_name");
    ColumnAttributes *colAttributes = new ColumnAttributes();
    colAttributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    Handles *handles = SQLExec::tables->select();
    ValueDicts *rows = new ValueDicts;
    for (auto &handle: *handles) {
        ValueDict *row = SQLExec::tables->project(handle, colNames);
        Identifier name = row->at("table_name").s;
        if (name != Columns::TABLE_NAME && name != Indices::TABLE_NAME)
            rows->push_back(row);
        else
            delete row;
    }


    delete handles;
    return new QueryResult(colNames, colAttributes, rows, "showing tables");
}

/**
 * @brief Shows all columns
 * 
 * @param statement the statement to be executed
 * @return QueryResult* the result of show
 */
QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
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

QueryResult *SQLExec::show_index(const ShowStatement *statement) {
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
QueryResult *SQLExec::insert(const InsertStatement *statement) {
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
QueryResult *SQLExec::select(const SelectStatement *statement) {
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

// QueryResult *SQLExec::execute_transaction_command(const TransactionStatement *statement){
//     switch(statement->type){
//         case TransactionStatement::BEGIN:
//             return begin_transaction(statement);
//         case TransactionStatement::COMMIT:
//             return commit_transaction(statement);
//         case TransactionStatement::ROLLBACK:
//             return rollback_transaction(statement);
//         default:
//             throw SQLExecError("invalid transaction type");
//     }
// }

// QueryResult *SQLExec::begin_transaction(const TransactionStatement *statement){
//     transactionStack.push(0);
//     return new QueryResult("Opened transaction level " + transactionStack.size());
// }

// QueryResult *SQLExec::commit_transaction(const TransactionStatement *statement){
//     if(transactionStack.empty())
//         throw SQLExecError("Attempted to commit a transaction when there are no transactions pending");
    
//     int oldNumTransactions = transactionStack.size(); // number of transactions before popping stack
//     transactionStack.pop();


//     return new QueryResult("Transaction level " + to_string(oldNumTransactions) + " committed, " + 
//                             (transactionStack.empty() ? "no" : to_string(transactionStack.size()))
//                             + " transactions pending");
// }

// QueryResult *SQLExec::rollback_transaction(const TransactionStatement *statement){
//     if(transactionStack.empty())
//         throw SQLExecError("Attempted to roll back a transaction when there are no transactions pending");
    
//     int oldNumTransactions = transactionStack.size(); // number of transactions before popping stack
//     transactionStack.pop();

//     return new QueryResult("Transaction level " + to_string(oldNumTransactions) + " rolled back, " + 
//                             (transactionStack.empty() ? "no" : to_string(transactionStack.size()))
//                             + " transactions pending");
// }