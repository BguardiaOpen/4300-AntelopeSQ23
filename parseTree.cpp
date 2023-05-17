#include<iostream>
#include <stdlib.h>
#include <string>
#include "db_cxx.h"
#include "SQLParser.h"
#include "SQLParserResult.h"
#include "heap_storage.h"
using namespace std;
using namespace hsql;

// Added our own functions from MS1
string parseCreate(CreateStatement* createStmt);
string parseTableRef(hsql::TableRef* tableRef);
string parseSelect(hsql::SelectStatement* selectStatement);
string parseExpression(hsql::Expr* expr);
std::string execute(SQLStatement* statement);

DbEnv *_DB_ENV;

// execute function that takes an SQLStatement and returns its canonical string representation
std::string execute(SQLStatement* statement) {
    std::string result;
    switch (statement->type()) {
        case kStmtCreate:
            result = parseCreate((CreateStatement*)statement);
            break;
        case kStmtSelect:
            result = parseSelect((SelectStatement*)statement);
            break;
        default:
            result = "Invalid SQL: "; // TODO: maybe remove this case since it's already handled in the loop, but this isn't a priority.
            break;
    }
    return result;
}

int main(int argc, char *argv[]) {
    if (argc <= 1) {
        std::cout<<"Invalid SQL statement"<<std::endl;
        return -1;
    }

    // create a Berkeley DB environment
    DbEnv env(0U);
    try{
        env.set_message_stream(&std::cerr);
        env.set_error_stream(&std::cerr);
        env.set_flags(DB_CREATE, 1);
        std::string dbEnvPath = argv[1];
        env.open(dbEnvPath.c_str(), DB_CREATE, 0);
    }catch(DbException &err){
        cout<< "Error creating db\n";
        return -1;
    }
    _DB_ENV = &env;

    // user-input loop
    std::string input;
    while (true) {
        std::cout << "SQL> ";
        std::getline(std::cin, input);
        if(input.length() == 0){
            continue;
        }
        if (input == "quit") {
            break;
        }
        if(input == "test"){
            std::cout<<"Calling test_heap_storage... "<< (test_heap_storage() ? "Passed" : "failed") << std::endl;
            continue;
        }
        else
        {// parse the SQL statement
            SQLParserResult* result = SQLParser::parseSQLString(input);
            if (!result->isValid()) {
                std::cout << "String is not a valid SQL query" << std::endl;
                return -1;
            }
            // execute the SQL statement and print its canonical string representation
            std::cout << execute((SQLStatement*)result->getStatement(0)) << std::endl;
        }
    }
    env.close(0);
    return 0;
}

string parseTableRef(hsql::TableRef* tableRef){
    string finalString = "";

    if(tableRef->list != nullptr){ // if there is >1 table in the list, parse all of them        
        for(int i=0; i < tableRef->list->size(); i++){
            finalString += parseTableRef((*tableRef->list)[i]);
            if(i < tableRef->list->size() - 1) 
                finalString += ", ";
        }
    }

    hsql::TableRefType tableRefType = tableRef->type; // type of table expression (join, table name, etc)
    switch(tableRefType){
        case hsql::kTableName:{ // if the table ref is a table name
            finalString += tableRef->name; // add table name
            if(tableRef->alias != nullptr) // check if there's an alias (tableName AS alias)
                finalString += " AS " + (string)tableRef->alias; // add "AS alias"
            break;
        }
        case hsql::kTableJoin:{ // if the table ref is a join expression
            hsql::JoinDefinition* joinDefinition = tableRef->join;
            hsql::JoinType joinType = joinDefinition->type; // type of join (inner, outer, etc)
            hsql::Expr* joinCondition = joinDefinition->condition;
            string joinTypeString = ""; // type of join (inner, outer, etc)
            string leftTableString = parseTableRef(joinDefinition->left); // parse the left table in the join; get the string
            string rightTableString = parseTableRef(joinDefinition->right); // same for the right table in the join
                
            switch(joinType){
                case hsql::kJoinLeft:{
                    joinTypeString = "LEFT JOIN";
                    break;
                }case hsql::kJoinRight:{
                    joinTypeString = "RIGHT JOIN";
                    break;
                }case hsql::kJoinInner:{
                    joinTypeString = "JOIN";
                    break;
                }case hsql::kJoinOuter:{
                    joinTypeString = "OUTER JOIN";
                    break;
                }
            }
            finalString = leftTableString + " " + joinTypeString + " " + rightTableString + " ON " + parseExpression(joinCondition);
        }
    }

    return finalString;
}

string parseExpression(hsql::Expr* expr){
    string finalString = "";

    switch(expr->type){
        // Base cases: if the expression doesn't have an operator, just parse the literal or column
        case hsql::kExprLiteralFloat:
            finalString += to_string(expr->fval); // TODO: fix floating point conversion
            break;
        case hsql::kExprLiteralInt:{
            finalString += to_string(expr->ival); 
            break;
        }case hsql::kExprLiteralString:{
            finalString += expr->getName();
            break;
        }case hsql::kExprColumnRef:{ // a column name, might have a table name
            char* varName = expr->name;
            char* table = expr->table;

            if(table != nullptr) // if there's a table name, add tableName.colName to finalString
                finalString += (string)table + ".";
            finalString += varName;
            break;
        }case hsql::kExprOperator:{
            finalString += parseExpression(expr->expr); // parse the expression on the left side of the operator, which might have an operator, add that            

            // add the operator character (=, >, etc.) to the final string, in between the LHS and RHS
            if(expr->opType != 0){ // if there is an operator, parse it
                        switch(expr->opType){
                            case 3:{ 
                                switch(expr->opChar){
                                    case '=':{
                                        finalString += " = "; 
                                        break;
                                    }case '<':{
                                        finalString += " < "; 
                                        break;
                                    }case '>':{
                                        finalString += " > "; 
                                        break;
                                    }case '+':{
                                        finalString += " + "; 
                                        break;
                                    }case '-':{
                                        finalString += " - "; 
                                        break;
                                    }case '*':{
                                        finalString += " * "; 
                                        break;
                                    }case '/':{
                                        finalString += " / "; 
                                        break;
                                    }case '%':{
                                        finalString += " % "; 
                                        break;
                                    }
                                }
                                break;
                            }case 4:{
                                finalString += " <> ";
                                break;
                            }case 5:{
                                finalString += " <= ";
                                break;
                            }case 6:{
                                finalString += " >= ";
                                break;
                            }
                        }
            }

            finalString += parseExpression(expr->expr2);
        }
    }

    return finalString;
}


string parseSelect(hsql::SelectStatement* selectStatement){
    string finalString = "SELECT ";
    hsql::TableRef* table = selectStatement->fromTable;
    hsql::Expr* whereClause = selectStatement->whereClause;
    vector<hsql::Expr*>* columnsToSelect = selectStatement->selectList;

    for(int i = 0; i < columnsToSelect->size(); i++){
        hsql::Expr* expr = (*columnsToSelect)[i];
        hsql::ExprType exprType = expr->type; // type of expression

        switch(exprType){
            case hsql::kExprStar: // if we're selecting *, add * to the string
                finalString += "*";
                break;           
            case hsql::kExprColumnRef:{ // if we are selecting columns, add the column names to the string
                if(expr->hasTable()) // if there's a table name before the column name (tableName.colName)
                    finalString += (string)expr->table + ".";
               
                if(expr->name != nullptr)
                    finalString += (string)expr->name;

                if(expr->hasAlias())
                    finalString += " AS " + (string)expr->alias;

                if(i < columnsToSelect->size() - 1) 
                    finalString += ", ";

                break;
            }
        }
    }

    finalString += (" FROM "+ parseTableRef(table));
    if(whereClause != nullptr)
        finalString += " WHERE " + parseExpression(whereClause);
    return finalString;
}

std::string parseCreate(CreateStatement* createStmt) {
    string parsed = "";
    if(createStmt->type == CreateStatement::kTable){
        parsed = "CREATE TABLE ";
        parsed += createStmt->tableName;
        parsed += " (";

        // TODO: constraint on data type?
        for(ColumnDefinition* colDef : *(createStmt->columns)){
            parsed += colDef->name;
            parsed += " ";
            parsed += colDef->type;
            parsed += ", ";
        }
        
        parsed += ")";
    }else if(createStmt->type == CreateStatement::kIndex){
        parsed = "CREATE INDEX ";
        parsed += createStmt->indexName;
        parsed += " ON ";
        parsed += createStmt->tableName;

        // not handling the USING clause

        for(char* indexCol : *(createStmt->indexColumns)){
            parsed += indexCol;
            parsed += ", ";
        }
        
        parsed += ")";
    }else
        parsed = "Invalid SQL statement: only CREATE TABLE and CREATE INDEX supported";
    return parsed;
}