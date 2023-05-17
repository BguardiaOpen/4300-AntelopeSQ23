
/**
 * @file heap_storage.h - Implementation of storage_engine with a heap file structure.
 * SlottedPage: DbBlock
 * HeapFile: DbFile
 * HeapTable: DbRelation
 *
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Winter 2023"
 */
//I decided to split the files up to allow for flexibility in Linking
#include "HeapFile.h"
#include "HeapTable.h"
#include "SlottedPage.h"
using namespace std;