#include "storage_engine.h"
using namespace std;

typedef std::pair<DbRelation*, Handles> EvalPipeline;

class TableScanPlan{
    public:
        TableScanPlan(DbRelation* tableToScan);
        EvalPipeline pipeline();
        DbRelation* getTable(); // return the table being scanned
        ~TableScanPlan();
    private:
        DbRelation* table;  
};

class SelectPlan{
    public:
        SelectPlan(TableScanPlan* tableScanPlan);
        // ~SelectPlan();
        EvalPipeline pipeline();
        // void operator=(const SelectPlan& selectPlan);
    private:
        TableScanPlan* tableScan;
};

// Project or ProjectAll plan
class EvalPlan{
    private:
        bool projectAll; // whether to project all columns; true if yes, false if no
        ColumnNames columnsToProject;
        SelectPlan* selectPlan;
    public:
        // When projecting only some columns, pass those to projectionColumns.
        // If you're projecting all columns, set projectAllColumns to true, and projectionColumns
        // will be ignored no matter what you set it to.
        EvalPlan(bool projectAllColumns, ColumnNames projectionColumns, SelectPlan* select_plan);
        // ~EvalPlan();
         // Attempt to get the best equivalent evaluation plan
        // EvalPlan optimize();

        // Evaluate the plan: evaluate gets values, pipeline gets handles
        ValueDicts evaluate();
};  

