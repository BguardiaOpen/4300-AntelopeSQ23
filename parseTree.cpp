#include<iostream>
#include <stdlib.h>
#include <string>
#include "db_cxx.h"
#include "SQLParser.h"
#include "SQLParserResult.h"

DbEnv *_DB_ENV;

// execute function that takes an SQLStatement and returns its canonical string representation
std::string execute(SQLStatement* statement) {
    std::string result;
    switch (statement->type()) {
        case kStmtCreate:
            result = dynamic_cast<CreateStatement*>(statement)->toString();
            break;
        case kStmtSelect:
            result = dynamic_cast<SelectStatement*>(statement)->toString();
            break;
        default:
            result = "Invalid SQL: " + statement->toString();
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
            hsql::SQLParserResult result;
            hsql::SQLParser::parse(input, &result);
            if (!result.isValid()) {
                std::cout << "String is not a valid SQL query" << std::endl;
                return -1;
            }
            // execute the SQL statement and print its canonical string representation
            std::cout << execute(result.getStatement(0)) << std::endl;
        }
    }
    env.close(0);
    return 0;
}
