/**
 * @file EvalPlan.cpp - implementation of evaluation plan classes
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Winter 2023"
 */

#include "EvalPlan.h"

// To use as a placeholder for initializing EvalPlan.table when we're creating an EvalPlan
// that's not a TableScan, since those EvalPlans don't need actual tables, but we still need
// to initialize EvalPlan.table with something.
class EmptyTable : public DbRelation {
public:
    static EmptyTable &one() {
        static EmptyTable d;
        return d;
    }

    EmptyTable() : DbRelation("empty_table", ColumnNames(), ColumnAttributes()) {}

    virtual void create() {};

    virtual void create_if_not_exists() {};

    virtual void drop() {};

    virtual void open() {};

    virtual void close() {};

    virtual Handle insert(const ValueDict *row) { return Handle(); }

    virtual void update(const Handle handle, const ValueDict *new_values) {}

    virtual void del(const Handle handle) {}

    virtual Handles *select() { return nullptr; };

    virtual Handles *select(const ValueDict *where) { return nullptr; }

    virtual Handles *select(Handles *current_selection, const ValueDict *where) { return nullptr; }

    virtual ValueDict *project(Handle handle) { return nullptr; }

    virtual ValueDict *project(Handle handle, const ColumnNames *column_names) { return nullptr; }
};

class ExtendedDbRelation : public DbRelation {
public:
    virtual void create() {};

    virtual void create_if_not_exists() {};

    virtual void drop() {};

    virtual void open() {};

    virtual void close() {};

    virtual Handle insert(const ValueDict *row) { return Handle(); }

    virtual void update(const Handle handle, const ValueDict *new_values) {}

    virtual void del(const Handle handle) {}

    virtual Handles *select() { return nullptr; };

    virtual Handles *select(const ValueDict *where) { return nullptr; }

    // select from a 
    virtual Handles *select(Handles *current_selection) { return nullptr; }

    virtual ValueDict *project(Handle handle) { return nullptr; } // this one already implemented

    // this one already implemented
    virtual ValueDict *project(Handle handle, const ColumnNames *column_names) { return nullptr; }
};

EvalPlan::EvalPlan(PlanType type, EvalPlan *relation) : type(type), relation(relation), projection(nullptr),
                                                        table(EmptyTable::one()) {
}

EvalPlan::EvalPlan(ColumnNames *projection, EvalPlan *relation) : type(Project), relation(relation),
                                                                  projection(projection), table(EmptyTable::one()) {
}

// for Select
EvalPlan::EvalPlan(EvalPlan *relation) : type(Select), relation(relation), projection(nullptr), table(EmptyTable::one()) {
}

EvalPlan::EvalPlan(DbRelation &table) : type(TableScan), relation(nullptr), projection(nullptr),
                                         table(table) {
}

EvalPlan::~EvalPlan() {
    delete relation;
    delete projection;
}

EvalPlan *EvalPlan::optimize() {
    return new EvalPlan(this);  // For now, we don't know how to do anything better
}

ValueDicts EvalPlan::evaluate() {
    ValueDicts ret;
    if (this->type != ProjectAll && this->type != Project)
        throw DbRelationError("Invalid evaluation plan--not ending with a projection");

    EvalPipeline pipeline = this->relation->pipeline();
    DbRelation *temp_table = pipeline.first;
    Handles *handles = pipeline.second;
    if (this->type == ProjectAll)
        ret = temp_table->project(handles);
    else if (this->type == Project)
        ret = temp_table->project(handles, this->projection);
    delete handles;
    return ret;
}

EvalPipeline EvalPlan::pipeline() {
    // base cases
    if (this->type == TableScan)
        return EvalPipeline(&this->table, this->table.select());
    if (this->type == Select && this->relation->type == TableScan)
        return EvalPipeline(&this->relation->table, this->relation->table.select());

    // recursive case
    if (this->type == Select) {
        EvalPipeline pipeline = this->relation->pipeline();
        DbRelation *temp_table = pipeline.first;
        Handles *handles = pipeline.second;
        EvalPipeline ret(temp_table, temp_table->select());
        delete handles;
        return ret;
    }

    throw DbRelationError("Not implemented: pipeline other than Select or TableScan");
}

