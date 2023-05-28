#include "LockTable.h"

LockTable::LockTable(){
    // casting since these are const
    table = new HeapTable((Identifier)TABLE_NAME, (ColumnNames)COLUMN_NAMES, (ColumnAttributes)COL_ATTRS);
    lastID = 0;
    table->create_if_not_exists();
    table->open();
}

LockTable::~LockTable(){
    table->close();
    table->drop(); // we want old transactions to be erased after quitting the program
    delete table;
}

// @param lockType: type of lock (shared, exclusive, or none) that the transaction needs
// @param tableToLock: the table that the transaction needs to lock
Handle LockTable::addRecord(LockType lockType, Identifier tableToLock){
    cout << "------------------- In addRecord --------------" << endl;
    // cout << "record.lockType: " << record.lockType << ", record.tableLocked: " << record.tableLocked << " lastid: " 
    //      << lastID << endl;

    ValueDict row;
    lastID++;
    row["transaction_id"] = Value((int32_t)lastID);

    cout << "Row before: ";
    for(pair<Identifier, Value> element : row) cout << element.first << ", " << element.second.s << endl;

    Handles* handles = table->select(&row);

    // check if the transaction id exists in the table; no duplicates allowed
    if(!handles->empty()){
        lastID--;
        delete handles;
        cout << "Error: attempted to add a transaction that already exists" << endl;
        return Handle();
    }

    delete handles;

    // convert LockType to int so we can store it in a ValueDict
    // In the lock_type field of the table, 0 = shared, 1 = exclusive, 2 = none
    if(lockType == LockTable::LockType::SHARED) row["lock_type"] = Value((int32_t)0);
    else if(lockType == LockTable::LockType::EXCLUSIVE) row["lock_type"] = Value((int32_t)1);
    else if(lockType == LockTable::LockType::NO_LOCK) row["lock_type"] = Value((int32_t)2);
    else{
        cout << "invalid lock type" << endl;
        return Handle();
    }

    // switch(lockType){
    //     case LockTable::LockType::SHARED:
    //         row["lock_type"] = Value((int32_t)0);
    //     case LockTable::LockType::EXCLUSIVE:
    //         row["lock_type"] = Value((int32_t)1);
    //     case LockTable::LockType::NO_LOCK:
    //         row["lock_type"] = Value((int32_t)2);
    //     default:{
    //         cout << "invalid lock type" << endl;
    //         return Handle();
    //     }
    // }

    row[this->COLUMN_NAMES[2]] = Value(tableToLock);

    cout << "Row: ";
    for(pair<Identifier, Value> element : row) cout << element.first << ", " << element.second.s << endl;

    return table->insert(&row);
}

void LockTable::removeRecord(int transactionID){
    // check if the transaction id exists in the table
    ValueDict where;
    where[COLUMN_NAMES[0]] = Value(transactionID);
    Handles* handles = table->select(&where);

    if(handles->empty()){
        delete handles;
        cout << "Error: attempted to delete a row from the lock table that does not exist, id=" << transactionID << endl;
        return;
    }

    // there should be only 1 handle with this transaction id, so we can use handles[0] 
    table->del((*handles)[0]);
    delete handles;
}

Handles* LockTable::select(){
    // cout << "------------------ In LockTable.select ------------------" << endl;

    Handles* h = table->select();
    ValueDict vals;

    // cout << "Number of rows selected: " << h->size() << endl;
    
    // for(Handle handle : *h){
    //     vals = *table->project(handle);
    //     for(pair<Identifier, Value> element : vals){
    //         cout << element.first << " " << element.second.n << element.second.s << endl;
    //     }
    //     cout << endl;
    // }

    return h;
}