#pragma once

#include "SlottedPage.h"
#include <cstring>
#include "db_cxx.h"
using namespace std;
using u16 = u_int16_t;
using u32 = u_int32_t;

/**
 * @class HeapFile - heap file implementation of DbFile
 *
 * Heap file organization. Built on top of Berkeley DB RecNo file. There is one of our
        database blocks for each Berkeley DB record in the RecNo file. In this way we are using Berkeley DB
        for buffer management and file management.
        Uses SlottedPage for storing records within blocks.
 */
class HeapFile : public DbFile {
public:
    HeapFile(std::string name) : DbFile(name), last(0), closed(true), db(_DB_ENV, 0) {this->dbfilename = name + ".db";};

    virtual ~HeapFile() {} //nothing to delete for now, ignore

    HeapFile(const HeapFile &other) = delete;

    HeapFile(HeapFile &&temp) = delete;

    HeapFile &operator=(const HeapFile &other) = delete;

    HeapFile &operator=(HeapFile &&temp) = delete;

    virtual void create(void);

    virtual void drop(void);

    virtual void open(void);

    virtual void close(void);

    virtual SlottedPage *get_new(void);

    virtual SlottedPage *get(BlockID block_id);

    virtual void put(DbBlock *block);

    virtual BlockIDs *block_ids();

    virtual u_int32_t get_last_block_id() { return last; }

    bool isOpen() {return this->closed == false;}

protected:
    std::string dbfilename;
    u_int32_t last;
    bool closed;
    Db db;

    virtual void db_open(uint flags = 0);
};