#include "HeapTable.h"
#include <algorithm>
#include <iterator>
#include<vector> 
using namespace std;
using u16 = u_int16_t;
using u32 = u_int32_t;

//HeapTable STARTS HERE
/**
 * Constructor
 * @param table_name
 * @param column_names
 * @param column_attributes
 */
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes) : DbRelation(
        table_name, column_names, column_attributes), file(table_name) {

        }



//file is the HeapFile associated with the HeapTable object
void HeapTable::create() {
    file.create();
}

//This is just a more complicated version of the above
void HeapTable::create_if_not_exists() {
    try {
        file.create();
    } catch(...) {
        file.open();
    }
}

//this calls drop() from the HeapFile on this one
void HeapTable::drop() {
    file.drop();
}

//as above, so below
void HeapTable::open() {
    file.open();
}

//closes the table
void HeapTable::close() {
    file.close();
}

//Handle is a pair of blockID, recordID defined in the abstract classes
Handle HeapTable::insert(const ValueDict *row) {
    this->open();
    ValueDict *full_row = validate(row);
    Handle handle = append(full_row);
    delete full_row;
    return handle;
}


// this is the SQL UPDATE analogue.
void HeapTable::update(const Handle handle, const ValueDict *new_values) {
    throw DbRelationError("Not implemented");
}

// DELETE operation analogue.  Take the block and record ID out, go find it and delete.
void HeapTable::del(const Handle handle) {
    this->open();
    BlockID block_id = handle.first;
    RecordID record_id = handle.second;
    SlottedPage *block = this->file.get(block_id);
    block->del(record_id);
    this->file.put(block);
    delete block;
    close();
}

// Temporarily copied David and Haley's code from MS2
//I have no idea what the empty select is supposed to do.
Handles *HeapTable::select() {
    // cout << endl << "in empty select" << endl;
    // Handles* handles = new Handles();
    // BlockIDs* block_ids = file.block_ids();
    // for (auto const& block_id: *block_ids) {
    //     SlottedPage* block = file.get(block_id);
    //     RecordIDs* record_ids = block->ids();
    //     for (auto const& record_id: *record_ids)
    //         handles->push_back(Handle(block_id, record_id));
    //     delete record_ids;
    //     delete block;
    // }
    // delete block_ids;

    // cout << "returning from empty select" << endl;
    // return handles;
    return select(nullptr);
}

Handles *HeapTable::select(const ValueDict *where) {
    open();
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id: *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const &record_id: *record_ids) {
            Handle handle(block_id, record_id);
            ValueDict copy;
            if(where != nullptr) {
                // std::cout << "Debug for select / where" << std::endl;
                copy.insert(where->begin(), where->end()); 
                if (selected(handle, &copy))
                    handles->push_back(Handle(block_id, record_id));
            } else
            {
                //std::cout << "Debug for select / no where block id {" << block_id << "} and recordid {" << record_id<< "}" << std::endl;
                handles->push_back(Handle(block_id, record_id));
            }
        }
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

Handles *select(Handles *current_selection, const ValueDict *where){
    cout << "not implemented" << endl;
    return new Handles();
}


// ATTRIBUTION: we copied this method from Professor Lundeen's solution repo
/**
 * See if the row at the given handle satisfies the given where clause
 * @param handle  row to check
 * @param where   conditions to check
 * @return        true if conditions met, false otherwise
 */
bool HeapTable::selected(Handle handle, const ValueDict *where) {
    if (where == nullptr)
        return true;
    ValueDict *row = this->project(handle, *where);
    bool is_selected = *row == *where;
    delete row;
    return is_selected;
}


// Just pulls out the column names from a ValueDict and passes that to the usual form of project().
ValueDict *HeapTable::project(Handle handle, ValueDict where) {
    ColumnNames t;
    for (auto const &column: where)
        t.push_back(column.first);

    // cout << "returning from HeapTable::project( handle,  where) " << endl;
    return this->project(handle, &t);
}

// I guess this is just a public call so that you can use it when you only have the handle.  It figures out the column names and goes and grabs them.
ValueDict *HeapTable::project(Handle handle) {
    // cout << endl << "In HeapTable::project( handle) " << endl;
    return project(handle, &this->column_names);
}

// This is the part that actually does the projecting.  
ValueDict *HeapTable::project(Handle handle, const ColumnNames *column_names) {

    BlockID block_id = handle.first;
    RecordID record_id = handle.second;
    SlottedPage *block = file.get(block_id);
    Dbt *data = block->get(record_id);
    ValueDict *row = unmarshal(data);
    delete data;
    delete block;
    if (column_names->empty())
        return row;
    ValueDict *result = new ValueDict();
    for (auto const &column_name: *column_names) {
        if (row->find(column_name) == row->end())
            throw DbRelationError("Column does not exist: '" + column_name + "'");
        (*result)[column_name] = (*row)[column_name];
    }
    delete row;

    // cout << "returning from HeapTable::project( handle,  colNames) " << endl;

    return result;
}

//Check if this row is acceptable to insert.
ValueDict *HeapTable::validate(const ValueDict *row) {
    ValueDict *full_row = new ValueDict();
    for (auto const &column_name: this->column_names) {
        Value value;
        ValueDict::const_iterator column = row->find(column_name);
        if (column == row->end())
            throw DbRelationError("Incoherent screaming.");
        else
            value = column->second;
        (*full_row)[column_name] = value;
    }
    return full_row;
}

//Add another record.  Returns the handle of the new row.
Handle HeapTable::append(const ValueDict *row) {
    Dbt *data = marshal(row);
    SlottedPage *block = this->file.get(this->file.get_last_block_id());
    RecordID record_id;
    try {
        record_id = block->add(data);
    } catch (DbBlockNoRoomError &e) {
        delete block;
        block = this->file.get_new();
        record_id = block->add(data);
    }
    this->file.put(block);
    delete block;
    delete[] (char *) data->get_data();
    delete data;
    return Handle(this->file.get_last_block_id(), record_id);
}

// ATTRIBUTION: we copied marshal() from Prof. Lundeen's solution repo
// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
Dbt *HeapTable::marshal(const ValueDict *row) {
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const &column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;

        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            if (offset + 4 > DbBlock::BLOCK_SZ - 4)
                throw DbRelationError("row too big to marshal");
            *(int32_t *) (bytes + offset) = value.n;
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            u_long size = value.s.length();
            if (size > UINT16_MAX)
                throw DbRelationError("text field too long to marshal");
            if (offset + 2 + size > DbBlock::BLOCK_SZ)
                throw DbRelationError("row too big to marshal");
            *(u16 *) (bytes + offset) = size;
            offset += sizeof(u16);
            memcpy(bytes + offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        } else if (ca.get_data_type() == ColumnAttribute::DataType::BOOLEAN) {
            if (offset + 1 > DbBlock::BLOCK_SZ - 1)
                throw DbRelationError("row too big to marshal");
            *(uint8_t *) (bytes + offset) = (uint8_t) value.n;
            offset += sizeof(uint8_t);
        } else {
            throw DbRelationError("Only know how to marshal INT, TEXT, and BOOLEAN");
        }
    }
    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;
}

// ATTRIBUTION: we copied unmarshal from Prof. Lundeen's solution repo
ValueDict *HeapTable::unmarshal(Dbt *data) {
    ValueDict *row = new ValueDict();
    Value value;
    char *bytes = (char *) data->get_data();
    uint offset = 0;
    uint col_num = 0;
    for (auto const &column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        value.data_type = ca.get_data_type();
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            value.n = *(int32_t *) (bytes + offset);
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            u16 size = *(u16 *) (bytes + offset);
            offset += sizeof(u16);
            char buffer[DbBlock::BLOCK_SZ];
            memcpy(buffer, bytes + offset, size);
            buffer[size] = '\0';
            value.s = string(buffer);  // assume ascii for now
            offset += size;
        } else if (ca.get_data_type() == ColumnAttribute::DataType::BOOLEAN) {
            value.n = *(uint8_t *) (bytes + offset);
            offset += sizeof(uint8_t);
        } else {
            throw DbRelationError("Only know how to unmarshal INT, TEXT, and BOOLEAN");
        }
        (*row)[column_name] = value;
    }
    return row;
}

// PREVIOUS CODE: Echidna

// SELECT operation analogue.  Load up your block IDs from the file, then your IDs from the block and where they match, push back a handle object and return it once they're all checked.
// Handles* HeapTable::select(const ValueDict* where) {
//     open();
//     cout << "opened" << endl;
//     Handles* handles = new Handles();
//     BlockIDs* block_ids = file.block_ids();
//     cout << "got past blockIDS" << endl;
//     for (auto const& block_id: *block_ids) {
//         SlottedPage* block = file.get(block_id);
//         RecordIDs* record_ids = block->ids();
//         for (auto const &record_id: *record_ids) {
//             Handle handle(block_id, record_id);
//             ValueDict copy;
//             if(where != nullptr) {
//                 std::cout << "Debug for select / where" << std::endl;
//                 copy.insert(where->begin(), where->end()); 
//                 if (selected(handle, copy))
//                     handles->push_back(Handle(block_id, record_id));
//             } else{
//                 std::cout << "Debug for select / no where block id {" << block_id << "} and recordid {" << record_id<< "}" << std::endl;
//                 handles->push_back(Handle(block_id, record_id));
//             }
//         }
//         delete record_ids;
//         delete block;
//     }
//     delete block_ids;
//     return handles;
// }

// bool HeapTable::selected(Handle handle, ValueDict where) {
//     ValueDict* row = this->project(handle, where);
//     bool is_selected = *row == where;
//     delete row;
//     return is_selected;
// }