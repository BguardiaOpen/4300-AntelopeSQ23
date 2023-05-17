#include <stdio.h>
#include <stdlib.h>
#include <memory.h>
#include "heap_storage.h"
typedef u_int16_t u16;

/*
 * SlottedPage Class Implementation
 * */
//SlottedPage Constructor: create a new block or get the number of records and pointer to the free memory
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
    if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } else {
        get_header(this->num_records, this->end_free);
    }
}

// Add a new record to the block. Return its id.
RecordID SlottedPage::add(const Dbt* data) {
    //If there is no room for the new record
    if (!has_room(data->get_size()))
        throw DbBlockNoRoomError("not enough room for new record");
    //Otherwise add to number of records
    u16 id = ++this->num_records;
    //Get the size of new record and add to free space
    u16 size = (u16) data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1;
    //update record count, end of space pointer, and add new record header and data
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}
//Get a record
Dbt* SlottedPage::get(RecordID record_id){
    u16 size = 0;
    u16 location=0;
    //get header of the record
    this->get_header(size, location, record_id);
    //If there's no record
    if(location == 0)
        return nullptr;
    //otherwise return new record
    return new Dbt(this->address(location),size);
}
//Put data in a record
void SlottedPage::put(RecordID record_id, const Dbt &data){
    u16 size=0;
    u16 location=0;
    //get header of record
    this->get_header(size, location, record_id);
    //get size of new data
    u16 new_size = (u16) data.get_size();
    //the extra size
    u16 extra = new_size - size;

    if (new_size > size) {
        //If there is no room for to put the new data
        if (!this->has_room(extra))
            throw DbBlockNoRoomError("not enough room");
        //since data size is too big, slide records to copy the new data
        this->slide(location, location - extra);
        memcpy(this->address(location-extra), data.get_data(), new_size);
    } else {
        //since data can fit, copy the new data into the current record then slide the records
        memcpy(this->address(location), data.get_data(), new_size);
        this->slide(location+new_size, location+size);
    }
    //Update the size and location of the record
    this->put_header(record_id, new_size, location);
}
//Delete a record. The header (size and location) needs to be set to zero but all the record id remains the same
void SlottedPage::del(RecordID record_id){
    u16 size =0;
    u16 location = 0;
    //get header of the record
    this->get_header(size, location, record_id);
    //set the size and location of that record to 0 to indicate delete
    this->put_header(record_id, 0, 0);
    //slide records after deleting
    this->slide(location, location+size);
}
//Return vector of all existing record ids
RecordIDs *SlottedPage::ids(void){
    RecordIDs* vec_ids = new RecordIDs();
    u16 size=0;
    u16 location =0;
    //add all the record ids into our vector
    for (RecordID record_id = 1; record_id <= this->num_records; record_id++) {
        this->get_header(size, location, record_id);
        if (location != 0)
            vec_ids->push_back(record_id);
    }
    return vec_ids;
}
//Get the header (size and location) of an id
void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id){
    size = this->get_n((u16) 4*id);
    location = this->get_n((u16)(4*id + 2));
}
// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    if (id == 0) { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4*id, size);
    put_n(4*id + 2, loc);
}

//Check if there is more room for the size
bool SlottedPage::has_room(u_int16_t size){
    u16 room_available = this->end_free - (u16)(4*(this->num_records+2));
    return size <= room_available;
}
//Slide the records(and header) right (if start<end) or left (if start>end)
void SlottedPage::slide(u_int16_t start, u_int16_t end){
    int shift = end - start;
    //If location is the same, there's no shift
    if (shift == 0)
        return;

    //get the address of start and end location
    void *to = this->address((u16)(this->end_free + 1 + shift));
    void *from = this->address((u16)(this->end_free + 1));
    int bytes = start - (this->end_free + 1);

    char temp[bytes];
    memcpy(temp, from, bytes);
    memcpy(to, temp, bytes);

    // After sliding, fix the header information
    RecordIDs* vector_record_ids = this->ids();
    u16 size=0;
    u16 location = 0;
    for (auto const& record_id : *vector_record_ids) {
        get_header(size, location, record_id);
        //change the location according to the shift
        if (location <= start) {
            location += shift;
            put_header(record_id, size, loc);
        }
    }
    delete vector_record_ids;

    this->end_free += shift;
    this->put_header();
}

// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset) {
    return *(u16*)this->address(offset);
}

// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u16 offset, u16 n) {
    *(u16*)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block.
void* SlottedPage::address(u16 offset) {
    return (void*)((char*)this->block.get_data() + offset);
}

/*
 * HeapFile Class Implementation
 * */
HeapFile::HeapFile(string name) : DbFile(name), dbfilename(""), last(0), closed(true), db(_DB_ENV, 0) {
    this->dbfilename = this->name+".db";
}

// Create file.
void HeapFile::create(void) {
    this->db_open(DB_CREATE|DB_EXCL);
    DbBlock* block = this->get_new();
    this->put(block);
}

// Delete file.
void HeapFile::drop(void) {
    this->close();
    db.remove(this->dbfilename.c_str(), nullptr, 0);
}

// Open file.
void HeapFile::open(void) {
    this->db_open();
}

// Close file.
void HeapFile::close(void) {
    this->db.close(0);
    this->closed = true;
}
// Allocate a new block for the database file.
// Returns the new empty DbBlock that is managing the records in this block and its block id.
SlottedPage* HeapFile::get_new(void) {
    char block[DbBlock::BLOCK_SZ];
    std::memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    int block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

    // write out an empty block and read it back in so Berkeley DB is managing the memory
    SlottedPage* page = new SlottedPage(data, this->last, true);
    this->db.put(nullptr, &key, &data, 0); // write it out with initialization applied
    this->db.get(nullptr, &key, &data, 0);
    return page;
}

// Get a block from the file
SlottedPage* HeapFile::get(BlockID block_id) {
    Dbt key(&block_id, sizeof(block_id));
    Dbt block;
    this->db.get(nullptr, &key, &block, 0);
    return new SlottedPage(data, block_id);
}

// Write a block to the file.
void HeapFile::put(DbBlock* block) {
    int block_id = block->get_block_id();
    Dbt key(&block_id, sizeof(block_id));
    this->db.put(nullptr, &key, block->get_block(), 0);
}

// Iterate through all block ids.
BlockIDs* HeapFile::block_ids() const {
    BlockIDs* vec_block_ids = new BlockIDs();
    //add all block ids to our vector
    for (BlockID block_id = 1; block_id <= this->last; block_id++)
        vec_block_ids->push_back(block_id);
    return vec_block_ids;
}
//Return the last block
u_int32_t HeapFile::get_last_block_id(){
    //Set the db statistics
    DB_BTREE_STAT stat;
    this->db.stat(nullptr, &stat, DB_FAST_STAT);
    return stat.bt_ndata;
}
void HeapFile::db_open(uint flags = 0){
    //if heap file is already opened, don't open
    if(!this->closed) return;
    //otherwise set the block size and open db
    this->db.set_re_len(DbBlock::BLOCK_SZ);
    this->db.open(NULL, this->dbfilename.c_str(), NULL, DB_RECNO, flags, 0644);
    //Set last block
    if(flags == 0) {
        this-> last =get_last_block_id();
    } else
        this-> last = 0;
    //since db is open now, set closed to false
    this->closed = false;
}


/*
 * HeapTable Class Implementation
 * */
//HeapTable Constructor
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes):
        DbRelation(table_name, column_names, column_attributes), file(table_name){
}

//Create a HeapTable
void HeapTable::create(){
    file.create();
}
//Create a heap table if the table can't be opened
void HeapTable::create_if_not_exists(){
    try{
        this->open();
    }
    catch (DbException& error) {
        this->create();
    }
}
//delete table
void HeapTable::drop(){
    file.drop();
}
//open table
void HeapTable::open(){
    file.open();
}
//close table
void HeapTable::close(){
    file.close();
}
//insert row
Handle HeapTable::insert(const ValueDict *row){
    this->open();
    Handle handle = append(this->validate(row));
    return handle;
}
Handles* HeapTable::select(const ValueDict* where) {
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id: *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id: *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}
//Validate whether the row can be inserted and return the entire row, otherwise raise error
ValueDict* HeapTable::validate(const ValueDict *row) {
    ValueDict* entire_row = new ValueDict();
    for (auto &col_name : this->column_names) {
        Value val;
        ValueDict::const_iterator col_iter = row->find(col_name);
        //if iterator reaches the end
        if (col_iter == row->end())
            throw DbRelationError("Unable to handle certain cases");
        //otherwise assign the value to the appropriate column name
        else
            val = col_iter->second;
        (*entire_row)[col_name] = val;
    }
    return entire_row;
}
//Append a record
Handle HeapTable::append(const ValueDict *row) {
    //marshall row
    Dbt* data = marshal(row);
    SlottedPage* block = this->file.get(this->file.get_last_block_id());
    RecordID id;
    try {
        //try to add data to the last block
        id = block->add(data);
    } catch (DbBlockNoRoomError &err) {
        //otherwise add to a new block
        block = this->file.get_new();
        id = block->add(data);
    }
    //Append block to file
    this->file.put(block);
    //delete ptrs
    delete [] (char *) data->get_data();
    delete data;
    delete block;
    return Handle(this->file.get_last_block_id(), id);
}
// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
Dbt* HeapTable::marshal(const ValueDict* row) {
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            *(int32_t*) (bytes + offset) = value.n;
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            uint size = value.s.length();
            *(u16*) (bytes + offset) = size;
            offset += sizeof(u16);
            memcpy(bytes+offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        } else {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;
}
// test function -- returns true if all tests pass
bool HeapTable::test_heap_storage() {
    ColumnNames column_names;
    column_names.push_back("a");
    column_names.push_back("b");
    ColumnAttributes column_attributes;
    ColumnAttribute ca(ColumnAttribute::INT);
    column_attributes.push_back(ca);
    ca.set_data_type(ColumnAttribute::TEXT);
    column_attributes.push_back(ca);
    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
    table1.create();
    std::cout << "create ok" << std::endl;
    table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
    std::cout << "drop ok" << std::endl;

    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    std::cout << "create_if_not_exsts ok" << std::endl;

    ValueDict row;
    row["a"] = Value(12);
    row["b"] = Value("Hello!");
    std::cout << "try insert" << std::endl;
    table.insert(&row);
    std::cout << "insert ok" << std::endl;
    Handles* handles = table.select();
    std::cout << "select ok " << handles->size() << std::endl;
    ValueDict *result = table.project((*handles)[0]);
    std::cout << "project ok" << std::endl;
    Value value = (*result)["a"];
    if (value.n != 12)
        return false;
    value = (*result)["b"];
    if (value.s != "Hello!")
        return false;
    table.drop();

    return true;
}

