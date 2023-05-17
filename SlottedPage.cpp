#include <cstring>
#include "SlottedPage.h"

using namespace std;
using u16 = u_int16_t;
using u32 = u_int32_t;


//SlottedPage PUBLIC METHODS STARTS HERE
//Basic constructor.  
//memcpy - source pointer, destination pointer, number of bytes to copy
//location - newsize - size is the formula to find 
SlottedPage::SlottedPage(Dbt& block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new){
    if(is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } else{
        get_header(this->num_records, this->end_free);
    }
}

// Add a new record to the block. Return its id.
RecordID SlottedPage::add(const Dbt* data) {
    if (!has_room(data->get_size()))
        throw DbBlockNoRoomError("not enough room for new record");
    u16 id = ++this->num_records;
    u16 size = (u16) data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}

//Given a record ID, get the bits stored in that record
Dbt *SlottedPage::get(RecordID record_id) {
    u16 size, location;
    get_header(size, location, record_id);
    if(location == 0)
        return nullptr;
    return new Dbt(this->address(location), size);
}

//This method replaces at location recordID with the given data encapsulated isn the Dbt.
void SlottedPage::put(RecordID recordID, const Dbt &data) {
    u16 size = get_n(4*recordID); //This is the size of the entry
    u16 location = get_n(4*recordID+2); //This is the offset, gotten using the id
    u16 newSize = (u16)data.get_size(); //This is the new size of the data in the entry
    if(newSize>size) { //If the new entry is larger
        if(!this->has_room(newSize-size)) {
            cout << "No space for new page." << endl;
        } else{
            memcpy(this->address(location-newSize-size), data.get_data(), newSize); //Copy from start of old data to end of new data
            this->slide(location, location-newSize-size); //
        }
    } else{ //if newsize is smaller than oldsize
        memcpy(this->address(location), data.get_data(), newSize); //copy data from data of newsize over this->address
        this->slide(location+newSize, location+size);
    }
    get_header(size, location, recordID);
    put_header(recordID, newSize, location);
}

//delete a record given the record ID.  
void SlottedPage::del(RecordID record_id){
    u16 size, location;
    this->get_header(size, location, record_id);
    this->put_header(record_id);
    this->slide(location, location + size);
}
//This method returns all of the ids containted within the object.
RecordIDs *SlottedPage::ids(){
	u16 size,loc;
	RecordIDs* idsets = new RecordIDs();
	for (u16 i = 1; i <= this->num_records; i++) {
		get_header(size, loc, i);
		if (loc > 0) {
			idsets->push_back(i);
		}
	}
	return idsets;
}
// SLOTTEDPAGE PROTECTED METHODS START HERE


//Pass by reference, so size and location are changed to the values held at record_id.  The +2 is the offset.
void SlottedPage::get_header(u16 &size, u16 &loc, RecordID id) {
    size = get_n((u16)4*id);
    loc = get_n((u16)(4*id+2));
}

//Put_header is the opposite, setting the values at given record ID
void SlottedPage::put_header(RecordID id, u16 size, u16 loc){
    if(id == 0){
        size = this->num_records;
        loc = this->end_free;
    }
    put_n((u16)4*id, size);
    put_n((u16)(4*id+2), loc);
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

//Check available room in the page
bool SlottedPage::has_room(u_int16_t size) {
	u16 available=this->end_free - (this->num_records + 2) * 4;
	return (size <= available);
}
//move data down to make room
void SlottedPage::slide(u_int16_t start, u_int16_t end){
	u16 shift = end - start;
	if(shift==0) return;
    // slide data
    void *to = this->address((u16) (this->end_free + 1 + shift));
    void *from = this->address((u16) (this->end_free + 1));
    int bytes = start - (this->end_free + 1U);
    memmove(to, from, bytes);
    
	//correct headers
	u16 size, location;
	RecordIDs* idset = this->ids();
	for(RecordID id:*idset){
		get_header(size, location,id);
		if (location <= start) {
			location += shift;
			put_header(id, size, location);
		}
	}
    delete idset; 
	this->end_free += shift;
	this->put_header();
}

