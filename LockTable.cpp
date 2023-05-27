#include "LockTable.h"

LockTable::LockTable(){
    // casting since these are const
    table = new HeapTable((Identifier)TABLE_NAME, (ColumnNames)COLUMN_NAMES, (ColumnAttributes)COL_ATTRS);
}

Handle LockTable::addRecord(LockTableRecord record){
    ValueDict row;
    row[this->COLUMN_NAMES[0]] = record.transactionID;
    
    // convert LockType to int so we can store it in a ValueDict
    // In the lock_type field of the table, 0 = shared, 1 = exclusive, 2 = none
    if(record.lockType == LockTable::LockType::SHARED){
        row[this->COLUMN_NAMES[1]] = Value(0);
    }else if(record.lockType == LockTable::LockType::EXCLUSIVE){
        row[this->COLUMN_NAMES[1]] = Value(1);
    }else if(record.lockType == LockTable::LockType::NO_LOCK){
        row[this->COLUMN_NAMES[1]] = Value(2);
    }else{
        cout << "Error: invalid lock type" << endl;
        return Handle();
    }

    row[this->COLUMN_NAMES[2]] = Value(record.tableLocked);

    // check if the transaction id exists in the table; no duplicates allowed
    ValueDict where;
    where[COLUMN_NAMES[0]] = record.transactionID;

    Handles* handles = table->select(&where);

    if(!handles->empty()){
        delete handles;
        cout << "Error: attempted to add a transaction that already exists" << endl;
        return Handle();
    }
    
    delete handles;
    return this->table->insert(&row);
}

void LockTable::removeRecord(int transactionID){
    // check if the transaction id exists in the table
    ValueDict where;
    where[COLUMN_NAMES[0]] = Value(transactionID);
    Handles* handles = table->select(&where);

    if(handles->empty()){
        delete handles;
        cout << "Error: attempted to delete a row from the lock table that does not exist" << endl;
        return;
    }

    // there should be only 1 handle with this transaction id, so we can use handles[0] 
    table->del((*handles)[0]);
    delete handles;
}

Handles* LockTable::select(){
    Handles* h = table->select();
    ValueDict vals;
    
    for(Handle handle : *h){
        vals = *table->project(handle);
        for(pair<Identifier, Value> element : vals){
            cout << element.first << " " << element.second.n << element.second.s << endl;
        }
    }

    return h;
}