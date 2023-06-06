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
#include "Transactions.h"
#include <iostream>

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;
Indices *SQLExec::indices = nullptr;
TransactionManager SQLExec::tm = TransactionManager();

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
    // Initializes _tables table if not null
    if (SQLExec::tables == nullptr) {
        SQLExec::tables = new Tables();
        SQLExec::indices = new Indices();
    }

    // initialize transaction manager
    // if(SQLExec::tm == nullptr)
    //     SQLExec::tm = TransactionManager();
    
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
                return select((const SelectStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError &e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

QueryResult *SQLExec::execute_transaction_command(const TransactionStatement *statement){
    switch(statement->type){
        case TransactionStatement::BEGIN:
            tm.begin_transaction();
            return new QueryResult("Successfully executed begin transaction");
        case TransactionStatement::COMMIT:
            tm.commit_transaction();
            return new QueryResult("Successfully executed commit transaction");
        case TransactionStatement::ROLLBACK:
            tm.rollback_transaction();
            return new QueryResult("Successfully executed rollback transaction");
        default:
            return new QueryResult("invalid transaction type");
    }
    return new QueryResult("invalid transaction type");
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
    // don't need transaction handling for create since it creates a new file, it doesn't 
    // modify an existing one. 
    // In case 2 transactions create the same table, HeapFile::create handles the case that
    // the table already exists 

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

// If there's a transaction currently running, requests a lock on the table file.
// Returns a pair of (file descriptor for the DB file for the table, ID of the current transaction)
// so that releaseLock can use those. Returns a pair of (-1, -1) if there are no transactions running
// @param stmt: a SQL statement that's inside the transaction
// @param tableToAccess: table being read or written to by the statement
pair<int, int> SQLExec::requestLock(SQLStatement* stmt, Identifier tableToAccess){
    // check if there are transactions in the active transaction list
    if(tm.getActiveTransactions().size() > 0){
        int fd = tm.getFD(tableToAccess); // get FD for table
        int currentTransID = tm.getCurrentTransactionID(); // transaction ID of this transaction

        if(currentTransID == -1) // check if stack is empty again, just in case
            return std::pair<int, int>(-1, -1);
        
        tm.tryToGetLock(currentTransID, *stmt, fd);
        return std::pair<int, int>(fd, currentTransID);
    }

    return std::pair<int, int>(-1, -1); // if there are no active transactions
}

// If a transaction is executing and has a lock, this releases the lock. If no transactions, does nothing.
// @param fdAndID: a pair with the first element being the file descriptor of the DB file being accessed by the statement,
//                 and the second element being the ID of the transaction that's executing a statement
void SQLExec::releaseLock(pair<int, int> fdAndID){
    if(fdAndID != pair<int, int>(-1, -1))
        tm.releaseLock(fdAndID.first, fdAndID.second);
    tm.updateTablesAndNames();
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

                pair<int, int> fdAndID = requestLock((SQLStatement*)statement, tableName); // request lock on table to drop
                
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

                
                releaseLock(fdAndID); // if this statement is part of a transaction, release the lock
            }
            return new QueryResult("Table successfully dropped!");
        case DropStatement::kIndex:
            {
                Identifier tableName = statement->name;
                Identifier indexName = statement->indexName;

                pair<int, int> fdAndID = requestLock((SQLStatement*)statement, indexName); // request lock on index to drop

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

                releaseLock(fdAndID); // if this statement is part of a transaction, release the lock

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

    // show tables statement to pass to requestLock
    SQLStatement stmt = ShowStatement(ShowStatement::EntityType::kTables);

    // lock _tables schema table
    pair<int, int> fdAndID = requestLock(&stmt, Tables::TABLE_NAME);

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
    releaseLock(fdAndID);
    return new QueryResult(colNames, colAttributes, rows, "showing tables");
}

/**
 * @brief Shows all columns
 * 
 * @param statement the statement to be executed
 * @return QueryResult* the result of show
 */
QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    // lock _columns schema table
    pair<int, int> fdAndID = requestLock((SQLStatement*)statement, Columns::TABLE_NAME);

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
    releaseLock(fdAndID);
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

    // lock _tables schema table
    pair<int, int> fdAndID = requestLock((SQLStatement*)statement, Indices::TABLE_NAME);
    Handles *handleList = SQLExec::indices->select(&location);

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle : *handleList) {
        ValueDict *row = SQLExec::indices->project(handle, colNames);
        rows->push_back(row);
    }
    delete handleList;
    releaseLock(fdAndID);
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

    pair<int, int> fdAndID = requestLock((SQLStatement*)statement, statement->tableName); 

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
        }
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

    releaseLock(fdAndID);

    // insert into any indices
    IndexNames indexNames = indices->get_index_names(statement->tableName);
    int numIndices = indexNames.size();

    if(numIndices > 0){
        ValueDict indexRowToInsert; // the row to insert into _indices

        for(string indexName : indexNames){
            // need to request a lock on each index too
            fdAndID = requestLock((SQLStatement*)statement, indexName);

            // don't need to check if the index exists since it's in indexNames
            DbIndex& index = indices->get_index(statement->tableName, indexName);

            // to insert into the index, need to get a handle to the row to insert, so call select 
            // on the row just inserted.
            // use rowToInsert as the where condition since it already specifies the values
            handles = table.select(&rowToInsert);

            // handles should contain only one row
            index.insert((*handles)[0]);
            delete handles;

            releaseLock(fdAndID);
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

// Precondition: no nested queries/select statements; you can only select from a table.
QueryResult *SQLExec::select(const SelectStatement *statement) {
    // throw an error if statement->fromTable is not a table name
    if(statement->fromTable->type != TableRefType::kTableName)
        return new QueryResult("Error: only selecting from a single table is supported");

    Identifier tableName = statement->fromTable->getName(); // name of table to select from

    pair<int, int> fdAndID = requestLock((SQLStatement*)statement, tableName);

    DbRelation& table = tables->get_table(tableName); // get the DbRelation for the table
    TableScanPlan tableScan = TableScanPlan(&table); // start with table scan
    SelectPlan selectPlan = SelectPlan(&tableScan);    // wrap that in a SelectPlan

    // get all column names and attributes in the table
    ColumnNames allColNames; // all column names in the table
    ColumnAttributes allColAttrs; // all column attributes in the table
    tables->get_columns(tableName, allColNames, allColAttrs);

    // determine whether all columns are being selected or only some
    ColumnNames colsToSelect;
    bool selectAllColumns = false; // whether all columns are selected

    for(Expr* expr : *statement->selectList){
        // Assume the only types are kExprStar and kExprColumnRef. If the statement is "SELECT *", 
        // then the length of the select list would be 1. Otherwise, get the column names being selected
        if(expr->type == kExprStar) selectAllColumns = true;
        else colsToSelect.push_back(expr->getName());
    }

    // We can pass colsToSelect in both cases since the column names will be ignored if projecting all columns.
    EvalPlan projection = EvalPlan(selectAllColumns ? true : false, colsToSelect, &selectPlan);
    ValueDicts result = projection.evaluate();
    ColumnAttributes selectedColAttrs; // if it's not a select * statement, these are the attributes of only the columns being selected   

    if(!selectAllColumns){        
        // Need to get the column attributes only corresponding to the columns we're selecting
        for(Identifier selectedColName : colsToSelect){
            // get an iterator to the location of selectedColName in allColNames
            ColumnNames::iterator it = find(allColNames.begin(), allColNames.end(), selectedColName);

            // get the index of selectedColName in allColNames, which is also the index of that column's
            // attribute in allColAttrs
            int index = distance(allColNames.begin(), it);

            // get the column attribute corresponding to selectedColName; add it to the list of attributes
            selectedColAttrs.push_back(allColAttrs[index]);
        }   

        releaseLock(fdAndID);
        return new QueryResult(&colsToSelect, &selectedColAttrs, &result, SUCCESS_MESSAGE);
    }

    releaseLock(fdAndID);
    return new QueryResult(&allColNames, &allColAttrs, &result, SUCCESS_MESSAGE);
}

pair<vector<DbRelation*>, vector<Identifier>*> SQLExec::saveTablesAndNames(){
    vector<DbRelation*> tableList; // list of all DbRelations for tables
    vector<Identifier>* tableNameList = new vector<Identifier>(); // list of all names of tables
    Handles* handles = tables->select(); // get handles to all table names

    // get DbRelations for all the tables
    for(Handle h : *handles){
        ValueDict* tableName = tables->project(h);
        tableNameList->push_back((*tableName)["table_name"].s);
        DbRelation* table = &tables->get_table((*tableName)["table_name"].s);
        tableList.push_back(table);
    }

    return pair<vector<DbRelation*>, vector<Identifier>*>(tableList, tableNameList);
}