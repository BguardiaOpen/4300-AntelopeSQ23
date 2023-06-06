# 4300-Antelope
## Description
DB Relation Manager project for CPSC4300 at Seattle U, Spring 2023, Project Antelope

## New Features
### Milestone 5 
Executes INSERT and SELECT statements, and contains the structure for BEGIN / COMMIT / ROLLBACK of transactions.

### Milestone 6
Builds off of Milestone 5 and creates locks that handle transactions 

## Installation
1. Clone the repository on CS1

` git clone https://github.com/BguardiaOpen/4300-AntelopeSQ23.git `

2. Ensure the ` .bash_profile ` path is configured correctly

```
export PATH=/usr/local/db6/bin:$PATH
export LD_LIBRARY_PATH=/usr/local/db6/lib:$LD_LIBRARY_PATH
export PYTHONPATH=/usr/local/db6/lib/site-packages:$PYTHONPATH 
```

## Usage
1. Create a directory to hold the database (first time usage only)
2. Compile the program with ` make `
3. Run the program with ` ./cpsc4300 path_to_database_directory `
    
    * The path must be the path to the directory from the root user@cs1
4. Other ``` make ``` options
    
    * ` make clean `: removes the object code files
5. User input options

    * SQL `CREATE`, `DROP`, and `SHOW` statements (see example)
    * ` quit ` exits the program


## Milestone 5/6 Example

```

#Instance 1:

SQL> create table foo (id int, data text)
CREATE TABLE foo (id INT, data TEXT)
created foo
SQL> insert into foo values (1,"one");insert into foo values(2,"two"); insert into foo values (2, "another two")
INSERT INTO foo VALUES (1, "one")
successfully inserted 1 row into foo
INSERT INTO foo VALUES (2, "two")
successfully inserted 1 row into foo
INSERT INTO foo VALUES (2, "another two")
successfully inserted 1 row into foo
SQL> select * from foo
SELECT * FROM foo
id data 
+----------+----------+
1 "one" 
2 "two" 
2 "another two" 
successfully returned 3 rows
SQL> begin transaction
BEGIN TRANSACTION - level 1
SQL> insert into foo values (4,"four");
INSERT INTO foo VALUES (4, "four")
successfully inserted 1 row into foo
SQL> select * from foo
SELECT * FROM foo
id data 
+----------+----------+
1 "one" 
2 "two" 
2 "another two" 
4 "four" 
successfully returned 4 rows
SQL> commit transaction
successfully committed transaction level 1
SQL> quit

#Instance 2, right after Instance 1 ran begin transaction:

SQL> select * from foo
SELECT * FROM foo
id data 
+----------+----------+
1 "one" 
2 "two" 
2 "another two" 
successfully returned 3 rows

Instance 2, right after Instance 1 ran INSERT INTO foo VALUES (4, "four"):

SQL> select * from foo
SELECT * FROM foo
Waiting for lock - table foo

id data 
+----------+----------+
1 "one" 
2 "two" 
2 "another two" 
4 "four" 
successfully returned 4 rows

```

## Example

```
$ ./cpsc4300 cpsc4300/data
SQL> create table foo (a text, b integer)
CREATE TABLE foo (a TEXT, b INT)
SQL> show tables
SHOW TABLES
table_name 
+----------+
"foo" 
SQL> show columns from foo
SHOW COLUMNS FROM foo
table_name column_name data_type 
+----------+----------+----------+
"foo" "a" "TEXT" 
"foo" "b" "INT"
SQL> drop table foo
DROP TABLE foo
SQL> show tables
SHOW TABLES
table_name 
+----------+
SQL> create index fx on foo (a)
CREATE INDEX fx ON foo USING BTREE (a)
created index fx
SQL> show index from foo
SHOW INDEX FROM goober
table_name index_name column_name seq_in_index index_type is_unique 
+----------+----------+----------+----------+----------+----------+
"foo" "fx" "a" 1 "BTREE" true
SQL> not real sql
Invalid SQL: not real sql
SQL> quit
```

## Acknowledgements
* [Berkeley DB](https://www.oracle.com/database/technologies/related/berkeleydb.html)
* [Berkeley DB Dbt](https://docs.oracle.com/cd/E17076_05/html/api_reference/CXX/frame_main.html)
* [Hyrise SQL Parser](https://github.com/klundeen/sql-parser)
* [Professor Lundeen's 5300-Instructor base code](https://github.com/klundeen/5300-Instructor/releases/tag/Milestone2h) 🙏🙏
* [The Python equivalent](https://github.com/BguardiaOpen/cpsc4300py)
