/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Winter 2023"
 */
#include "SQLExec.h"
#include "ParseTreeToString.h"
#include "SchemaTables.h"
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
    // Initializes _tables table if not null
    if (SQLExec::tables == nullptr) {
        SQLExec::tables = new Tables();
        SQLExec::indices = new Indices();
    }

    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
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
        case CreateStatement::kTable: 
            //blocked to prevent scoping issues in the try catch
            { 
                //add columns to table
                Identifier name = statement->tableName;
                Identifier colName;
                ColumnNames colNames;
                ColumnAttribute colAttribute;
                ColumnAttributes colAttributes;

                for(ColumnDefinition *col : *statement->columns) {
                    //ColumnAttribute colAttribute(ColumnAttribute::INT);
                    //create a column binding for column to a name and attribute and add
                    //to colNames and colAttributes
                    column_definition(col, colName, colAttribute);
                    colNames.push_back(colName);
                    colAttributes.push_back(colAttribute);
                }

                //insert an empty row into the new table to instantiate change
                ValueDict row;
                row["table_name"] = name;
                Handle handle = SQLExec::tables->insert(&row);
                try {
                    Handles handleList;
                    DbRelation &cols = SQLExec::tables->get_table(Columns::TABLE_NAME);
                    try {
                        //add columns to schema, and remove existing on error
                        for(uint index = 0; index < colNames.size(); index++) {
                            row["column_name"] = colNames[index];
                            //add type of column appropriately
                            if(colAttributes[index].get_data_type() == ColumnAttribute::INT)
                                row["data_type"] = Value("INT");
                            else    
                                row["data_type"] = Value("TEXT");
                            handleList.push_back(cols.insert(&row));
                        }

                        //create actual relation in system, accounting for prexistence
                        DbRelation &table = SQLExec::tables->get_table(name);
                        if(statement->ifNotExists) {;
                            table.create_if_not_exists();
                        } else {
                            table.create();
                        }

                    } catch(exception &e) {
                        try {
                            //delete remaining handles
                            for(auto const &handle : handleList) 
                                cols.del(handle);
                        } catch (...) {
                        //...doesn't really matter if there's an error, 
                        //just need to try to delete the handle if it exists
                        }
                        return new QueryResult(e.what()); 
                    }
                } catch(exception &e) {
                    //delete the handle
                    try {
                        SQLExec::tables->del(handle);
                    } catch (...) {
                        //...doesn't really matter if there's an error, 
                        //just need to try to delete the handle if it exists
                    }
                    return new QueryResult(e.what()); 
                }
                return new QueryResult("Table " + name + " created successfully");
            }
        case CreateStatement::kIndex:
            {
                Identifier tableName = statement->tableName;
                Identifier indexName = statement->indexName;

                DbRelation &table = SQLExec::tables->get_table(tableName);
                const ColumnNames &cols = table.get_column_names();

                // make sure columns in index are actually in table
                for (auto const &colName : *statement->indexColumns) {
                    if (find(cols.begin(), cols.end(), colName) == cols.end())
                        throw SQLExecError("Index column does not exist in table");
                }

                // add index to indices table
                ValueDict row;
                row["table_name"] = Value(tableName);
                row["index_name"] = Value(indexName);
                row["index_type"] = Value(statement->indexType);
                if (string(statement->indexType) == "BTREE")
                    row["is_unique"] = Value("true");
                else
                    row["is_unique"] = Value("false");

                Handles handleList;
                try {
                    int count = 0;
                    
                    for (auto const &colName : *statement->indexColumns) {
                        row["seq_in_index"] = Value(count);
                        row["column_name"] = Value(colName);
                        count++;
                        handleList.push_back(SQLExec::indices->insert(&row));
                    }

                    DbIndex &index = SQLExec::indices->get_index(tableName, indexName);
                    index.create();
                }
                catch (...) {
                    try {
                        for (auto const &handle : handleList)
                            SQLExec::indices->del(handle);
                    } catch (...) {
                        
                    }
                    return new QueryResult("Index could not be created");
                }
                
                return new QueryResult("Index successfully created");
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