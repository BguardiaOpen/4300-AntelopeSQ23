#include "LockTable.h"

LockTable::LockTable(){
    lastID = 0;
}

// Returns whether there is a record that exists in the lock table with the given transaction ID 
// (true if there is one, false if not)
// @param transactionID: the ID of the transaction to check for
bool LockTable::recordExists(int transactionID){
    // find the position of the record
    vector<LockTableRecord>::iterator it = table.begin();
    bool found = false; // whether the record is found in the table

    while(it != table.end() && !found){
        LockTableRecord record = *it;
        if(record.transactionID == transactionID)
            found = true;
        else it++;
    }

    return found;
}

// Insert a transaction if it doesn't already exist in the lock table.
// @param lockType: type of lock (shared, exclusive, or none) that the transaction needs
// @param tableToLock: the table that the transaction needs to lock
void LockTable::insertIfNotExists(LockType lockType, Identifier tableToLock){
    lastID++;

    // check if the transaction id exists in the table; no duplicates allowed
    if(recordExists(lastID)){
        lastID--;
        cout << "transaction already exists" << endl;
        return;
    }

    // create and insert the new record
    LockTableRecord record;
    record.transactionID = lastID;
    record.lockType = lockType;
    record.tableLocked = tableToLock;
    table.push_back(record);
}

void LockTable::removeRecord(int transactionID){
    if(!recordExists(transactionID)){
        cout << "Error: attempted to delete a row that does not exist, id=" << transactionID << endl;
        return;
    }

    // find the position of the record
    vector<LockTableRecord>::iterator it = table.begin();
    bool found = false; // whether the record is found in the table

    while(it != table.end() && !found){
        LockTableRecord record = *it;
        if(record.transactionID == transactionID)
            found = true;
        else it++;
    }

    table.erase(it);
}

void LockTable::printRecords(){
    cout << endl << endl;
    for(LockTableRecord record : this->table)
        cout << "transactionID: " << record.transactionID << ", lockType: " << record.lockType << ", tableLocked: " << record.tableLocked;
}

    // cout << "------------------ In LockTable.select ------------------" << endl;

    // Handles* h = table->select();
    // // cout << "selected" << endl;
    // // ValueDict vals;

    // // cout << "Number of rows selected: " << h->size() << endl;
    
    // // for(Handle handle : *h){
    // //     vals = *table->project(handle);
    // //     for(pair<Identifier, Value> element : vals){
    // //         cout << element.first << " " 
    // //              << (element.second.data_type == ColumnAttribute::INT ? to_string((int)element.second.n) : element.second.s) << endl;
    // //     }
    // //     cout << endl;
    // // }

    // return h;


// @param lockType: type of lock (shared, exclusive, or none) that the transaction needs
// @param tableToLock: the table that the transaction needs to lock
// Handle LockTable::addRecord(LockType lockType, Identifier tableToLock){
//     cout << "------------------- In addRecord --------------" << endl;

//     ValueDict row;
//     lastID++;

//     // check if the transaction id exists in the table; no duplicates allowed
//     if(recordExists(lastID)){
//         lastID--;
//         cout << "Error: attempted to add a transaction that already exists" << endl;
//         return Handle();
//     }

//     row["transaction_id"] = Value(to_string(lastID)); // converting every int to a string since we had issues printing the int while testing

//     // convert LockType to int so we can store it in a ValueDict
//     // In the lock_type field of the table, 0 = shared, 1 = exclusive, 2 = none
//     if(lockType == LockTable::LockType::SHARED) row["lock_type"] = Value(to_string(0));
//     else if(lockType == LockTable::LockType::EXCLUSIVE) row["lock_type"] = Value(to_string(1));
//     else if(lockType == LockTable::LockType::NO_LOCK) row["lock_type"] = Value(to_string(2));
//     else{
//         cout << "invalid lock type" << endl;
//         return Handle();
//     }
    
//     row[this->COLUMN_NAMES[2]] = Value(tableToLock);

//     cout << "Row: ";
//     for(pair<Identifier, Value> element : row) 
//         cout << element.first << ", " << (element.second.data_type == ColumnAttribute::INT ? to_string((int)element.second.n) : element.second.s) << endl;

//     return table->insert(&row);
// }

// void LockTable::removeRecord(int transactionID){
//     cout << "in lockTable::delete" << endl;

//     // if(!recordExists(transactionID)){
//     //     cout << "Error: attempted to delete a row that does not exist, id=" << transactionID << endl;
//     //     return;
//     // }

//     // get a handle to the record to delete
//     ValueDict where;
//     where[COLUMN_NAMES[0]] = Value(to_string(transactionID));
//     Handles* handles = table->select(&where);

//     // there should be only 1 handle with this transaction id, so we can use handles[0] 
//     table->del((*handles)[0]);
//     delete handles;
// }

// Handles* LockTable::select_(){
//     cout << "------------------ In LockTable.select ------------------" << endl;

//     Handles* h = table->select();
//     // cout << "selected" << endl;
//     // ValueDict vals;

//     // cout << "Number of rows selected: " << h->size() << endl;
    
//     // for(Handle handle : *h){
//     //     vals = *table->project(handle);
//     //     for(pair<Identifier, Value> element : vals){
//     //         cout << element.first << " " 
//     //              << (element.second.data_type == ColumnAttribute::INT ? to_string((int)element.second.n) : element.second.s) << endl;
//     //     }
//     //     cout << endl;
//     // }

//     return h;
// }


// LockTable::~LockTable(){
//     table->close();
//     table->drop(); // we want old transactions to be erased after quitting the program
//     delete table;
// }

// Returns whether there is a record that exists in the lock table with the given transaction ID 
// (true if there is one, false if not)
// @param transactionID: the ID of the transaction to check for
// bool LockTable::recordExists(int transactionID){
//     ValueDict where;
//     where["transaction_id"] = Value(to_string(transactionID)); // converting every int to a string since we had issues printing the int while testing

//     Handles* handles = table->select(&where);
//     bool isEmpty = handles->empty(); 
//     delete handles;
//     return !isEmpty;
// }
