#include "EvalPlan.h"

TableScanPlan::TableScanPlan(DbRelation* tableToScan){
    table = tableToScan;
}

TableScanPlan::~TableScanPlan(){
    delete table;
}

DbRelation* TableScanPlan::getTable(){
    return table;
}

EvalPipeline TableScanPlan::pipeline(){
    return EvalPipeline(this->table, *table->select());
}


SelectPlan::SelectPlan(TableScanPlan* tableScanPlan){
    tableScan = tableScanPlan;
}

// SelectPlan::~SelectPlan(){
//     cout << "In SelectPlan dtor" << endl;

//     // delete tableScan;
// }

EvalPipeline SelectPlan::pipeline(){
    DbRelation* table = tableScan->getTable(); // the table in the TableScanPlan
    return EvalPipeline(table, *table->select());
}

EvalPlan::EvalPlan(bool projectAllColumns, ColumnNames projectionColumns, SelectPlan* select_plan){
    projectAll = projectAllColumns;
    selectPlan = select_plan;
    columnsToProject = projectionColumns;
}

ValueDicts EvalPlan::evaluate(){
    ValueDicts ret;
    
    EvalPipeline pipeline = selectPlan->pipeline();
    DbRelation* temp_table = pipeline.first;
    Handles handles = pipeline.second;

    if (projectAll)
        ret = *temp_table->project(&handles);
    else
        ret = *temp_table->project(&handles, &columnsToProject);

    return ret;
}

// EvalPlan::~EvalPlan(){
//     cout << "In EvalPlan dtor" << endl;
//     // delete selectPlan;
// }