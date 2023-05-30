#include "HeapFile.h"
#include <cstring>
#include "db_cxx.h"

using namespace std;
using u16 = u_int16_t;
using u32 = u_int32_t;


// HEAPFILE PUBLIC METHODS START HERE

// This method gets a new block of data adds it to the file, then returns the pointer to the new object.
SlottedPage* HeapFile::get_new(void) {
    char block[DbBlock::BLOCK_SZ];
    std::memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    int block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

    // write out an empty block and read it back in so Berkeley DB is managing the memory
    SlottedPage *page = new SlottedPage(data, this->last, true);
    this->db.put(nullptr, &key, &data, 0); // write it out with initialization done to it
    delete page;
    this->db.get(nullptr, &key, &data, 0);
    return new SlottedPage(data, this->last, true);
}

void HeapFile::create(void){
    this->db_open(DB_CREATE | DB_EXCL);
    SlottedPage* block = this->get_new();
    delete block;
}

void HeapFile::open(void){
    this->db_open(0);
}

void HeapFile::close(void){
    this->db.close(0);
    this->closed = true;
}

void HeapFile::drop(void){
    this->close();
    Db db(_DB_ENV, 0);
    db.remove(this->dbfilename.c_str(), nullptr, 0);
}

SlottedPage* HeapFile::get(BlockID block_id){
    Dbt key(&block_id, sizeof(block_id)), block;
    this->db.get(NULL, &key, &block, 0);
    return new SlottedPage(block, block_id, false);
}

void HeapFile::put(DbBlock* block) {
    BlockID block_id = block->get_block_id();
    Dbt key(&block_id, sizeof(block_id));
    this->db.put(nullptr, &key, block->get_block(), 0);
}

BlockIDs* HeapFile::block_ids() {
    BlockIDs* block_ids = new BlockIDs();
    for (BlockID block_id = 1; block_id <= this->last; block_id++)
        block_ids->push_back(block_id);
    return block_ids;
}

// ATTRIBUTION: We copied this method from Professor Lundeen's solution repo
// void HeapFile::db_open(uint flags) {
//     cout << endl << "In HeapFile::db_open" << endl;

//     if (!this->closed)
//         return;
    
//     cout << "File not already open" << endl;
//     this->db.set_re_len(DbBlock::BLOCK_SZ); // record length - will be ignored if file already exists

//     cout << "calling Berkeley open" << endl;
//     this->db.open(nullptr, this->dbfilename.c_str(), nullptr, DB_RECNO, flags, 0644);

//     cout << "getting block count" << endl;
//     this->last = flags ? 0 : get_block_count();
//     this->closed = false;

//     cout << "Returning from HeapFile::db_open" << endl;
// }

void HeapFile::db_open(uint flags) {
  cout << "In db_open" << endl;
  //handle closed state
  if(!this->closed){
    cout << "Table already open. returning" << endl;
    return;
  }

  cout << "setting rec length" << endl;
  //set block size and open db
  this->db.set_re_len(DbBlock::BLOCK_SZ);

  cout << "Passed set_re_len" << endl;
  if(flags == 0)
    this->db.open(NULL, this->dbfilename.c_str(), NULL, DB_RECNO, 0, 0644);  
  else
    this->db.open(NULL, this->dbfilename.c_str(), NULL, DB_RECNO, flags, 0644);

  cout << "Called berkeley open" << endl;

  //intialize db statisitcs and set last block
  if(flags == 0) {
    DB_BTREE_STAT *stat;
    this->db.stat(nullptr, &stat, DB_FAST_STAT);
    this->last = stat->bt_ndata;
    free(stat);
  } else this-> last = 0;

  //set closed to false to indicate db is open
  this->closed = false;

  cout << "exiting open" << endl;
}

// ATTRIBUTION: We copied this method from Professor Lundeen's solution repo
uint32_t HeapFile::get_block_count() {
    cout << endl << "In getBlockCount" << endl;
    DB_BTREE_STAT *stat;
    this->db.stat(nullptr, &stat, DB_FAST_STAT);
    uint32_t bt_ndata = stat->bt_ndata;
    free(stat);

    cout << "Returning from getBlockCount" << endl;
    return bt_ndata;
}

//Ryan forgot to include this, so I'm using my own code --Ishan
// void HeapFile::db_open(uint flags) {
//   cout << "In db_open" << endl;
//   //handle closed state
//   if(!this->closed){
//     cout << "Table already open. returning" << endl;
//     return;
//   }

//   //set block size and open db
//   this->db.set_re_len(DbBlock::BLOCK_SZ);
//   if(flags == 0)
//     this->db.open(NULL, this->dbfilename.c_str(), NULL, DB_RECNO, 0, 0644);  
//   else
//     this->db.open(NULL, this->dbfilename.c_str(), NULL, DB_RECNO, flags, 0644);


//   //intialize db statisitcs and set last block
//   if(flags == 0) {
//     DB_BTREE_STAT *stat;
//     this->db.stat(nullptr, &stat, DB_FAST_STAT);
//     this->last = stat->bt_ndata;
//     free(stat);
//   } else this-> last = 0;

//   //set closed to false to indicate db is open
//   this->closed = false;

//   cout << "exiting open" << endl;
// }
