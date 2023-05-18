#pragma once 

#include "storage_engine.h"
using namespace std;
using u16 = u_int16_t;
using u32 = u_int32_t;

/**
 * @class SlottedPage - heap file implementation of DbBlock.
 *
 *      Manage a database block that contains several records.
        Modeled after slotted-page from Database Systems Concepts, 6ed, Figure 10-9.
        Record id are handed out sequentially starting with 1 as records are added with add().
        Each record has a header which is a fixed offset from the beginning of the block:
            Bytes 0x00 - Ox01: number of records
            Bytes 0x02 - 0x03: offset to end of free space
            Bytes 0x04 - 0x05: size of record 1
            Bytes 0x06 - 0x07: offset to record 1
            etc.
 *
 */


class SlottedPage : public DbBlock {
public:

    //Preconditons: block MUST be an intialized object, block_id is a valid block id
    //              and is_new MUST be correct (this is a contractual requirement)
    SlottedPage(Dbt &block, BlockID block_id, bool is_new);

    // Big 5 - we only need the destructor, copy-ctor, move-ctor, and op= are unnecessary
    // but we delete them explicitly just to make sure we don't use them accidentally
    virtual ~SlottedPage() {}

    SlottedPage(const SlottedPage &other) = delete;

    SlottedPage(SlottedPage &&temp) = delete;

    SlottedPage &operator=(const SlottedPage &other) = delete;

    SlottedPage &operator=(SlottedPage &temp) = delete;

    virtual RecordID add(const Dbt *data);


    virtual Dbt* get(RecordID record_id);

    virtual void put(RecordID record_id, const Dbt &data);

    virtual void del(RecordID record_id);

    virtual RecordIDs *ids(void);

protected:
    u_int16_t num_records;
    u_int16_t end_free;

    virtual void get_header(u_int16_t &size, u_int16_t &loc, RecordID id = 0);

    virtual void put_header(RecordID id = 0, u_int16_t size = 0, u_int16_t loc = 0);

    virtual bool has_room(u_int16_t size);

    virtual void slide(u_int16_t start, u_int16_t end);

    virtual u_int16_t get_n(u_int16_t offset);

    virtual void put_n(u_int16_t offset, u_int16_t n);

    virtual void *address(u_int16_t offset);
};

