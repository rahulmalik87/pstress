#include "common.hpp"
#include "pstress.hpp"
#include <iostream>

Opx *options = new Opx;
Ser_Opx *server_options = new Ser_Opx;

/* Process --mso=abc=30=40 to abc,{30,40}*/
void add_server_options(std::string str) {
  auto found = str.find_first_of(":", 0);
  int probability = 100;
  if (found != std::string::npos) {
    probability = std::stoi(str.substr(0, found));
    str = str.substr(found + 1, str.size());
  }

  /* extract probability */
  found = str.find_first_of("=", 0);
  if (found == std::string::npos)
    throw std::runtime_error("Invalid string, " + str);

  std::string name = str.substr(0, found);
  Server_Option *so = new Server_Option(name);
  so->prob = probability;
  server_options->push_back(so);
  str = str.substr(found + 1, str.size());

  found = str.find_first_of("=");
  while (found != std::string::npos) {
    auto val = str.substr(0, found);
    so->values.push_back(val);
    str = str.substr(found + 1, str.size());
    found = str.find_first_of("=");
  }
  /* push the last one */
  so->values.push_back(str);
}

/* process file. and push to mysqld server options */
void add_server_options_file(std::string file_name) {
  std::ifstream f1;
  f1.open(file_name);
  if (!f1)
    throw std::runtime_error("unable to open " + file_name);
  std::string option;
  while (f1 >> option)
    add_server_options(option);
  f1.close();
}

/* add new options */
inline Option *newOption(Option::Type t, Option::Opt o, std::string s) {
  auto *opt = new Option(t, o, s);
  options->at(o) = opt;
  return opt;
}

/* All available options */
void add_options() {
  options->resize(Option::MAX);
  Option *opt;

  /* Mode of Pstress */
  opt = newOption(Option::BOOL, Option::PQUERY, "pquery");
  opt->help = "run pstress as pquery 2.0. sqls will be executed from --infine "
              "in some order based on shuffle. basically it will run in pquery "
              "mode you can also use -k";
  opt->setBool(false); // todo disable in release
  opt->setArgs(no_argument);

  /* Intial Seed for test */
  opt = newOption(Option::INT, Option::INITIAL_SEED, "seed");
  opt->help = "Initial seed used for the test";
  opt->setInt(1);

  /* Number of General tablespaces */
  opt =
      newOption(Option::INT, Option::NUMBER_OF_GENERAL_TABLESPACE, "tbs-count");
  opt->setInt("1");
  opt->help = "random number of different general tablespaces ";

  /* Number of Undo tablespaces */
  opt = newOption(Option::INT, Option::NUMBER_OF_UNDO_TABLESPACE,
                  "undo-tbs-count");
  opt->setInt("3");
  opt->help = "Number of default undo tablespaces ";

  /* Engine */
  opt = newOption(Option::STRING, Option::ENGINE, "engine");
  opt->help = "Engine used ";
  opt->setString("INNODB");

  /* Just Load DDL*/
  opt = newOption(Option::BOOL, Option::JUST_LOAD_DDL, "jlddl");
  opt->help = "load DDL and exit";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* DDL option */
  opt = newOption(Option::BOOL, Option::NO_DDL, "no-ddl");
  opt->help = "do not use ddl in workload";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* Only command line sql option */
  opt = newOption(Option::BOOL, Option::ONLY_CL_SQL, "only-cl-sql");
  opt->help = "only run command line sql. other sql will be disable";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* Only command line ddl option */
  opt = newOption(Option::BOOL, Option::ONLY_CL_DDL, "only-cl-ddl");
  opt->help = "only run command line ddl. other ddl will be disable";
  opt->setBool(false);
  opt->setArgs(no_argument);

  opt = newOption(Option::BOOL, Option::SECONDARY_AFTER_CREATE,
                  "secondary-after-create");
  opt->help = "set secondary engine after table is created";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* select in SECONDARY */
  opt = newOption(Option::BOOL, Option::SELECT_IN_SECONDARY,
                  "select-in-secondary");
  opt->help = "Execute all SELECT in SECONDARY";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* Delay in secondary*/
  opt =
      newOption(Option::INT, Option::DELAY_IN_SECONDARY, "delay-in-compare-ms");
  opt->help = "Add milliseconds of delay before executing the secondary";
  opt->setInt(0);

  /* disable table compression */
  opt = newOption(Option::BOOL, Option::NO_TABLE_COMPRESSION,
                  "no-table-compression");
  opt->help = "Disable table compression";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* disable column compression */
  opt = newOption(Option::BOOL, Option::NO_COLUMN_COMPRESSION,
                  "no-column-compression");
  opt->help = "Disable column compression. It is percona style compression";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* disable all type of encrytion */
  opt = newOption(Option::BOOL, Option::USE_ENCRYPTION, "use-encryption");
  opt->help = "Enable encryption of table, tablespace";
  opt->setBool(false);
  opt->setArgs(no_argument);

  opt = newOption(Option::STRING, Option::IGNORE_ERRORS, "retry-errors");
  opt->help = "Ignore MySQL errors. example --mysql-ignore-error=2013,1047";
  opt->setString("NONE");

  opt = newOption(Option::STRING, Option::FUNCTION_CONTAINS_DML,
                  "function-contains-dml");
  opt->help =
      "Function contains DML. It tels what type of  SQL FUNCTION CONTAINS"
      "--function-contains-dml=update,delete,insert. Even order matters. for "
      "example if it insert,update then it would first insert and then update "
      "or";
  opt->short_help = "functions";
  opt->setString("insert,update");

  opt = newOption(Option::INT, Option::IGNORE_DML_CLAUSE,
                  "ignore-dml-clause-prob");
  opt->help = "Adding Ignore clause to update delete and insert ";
  opt->setInt(10);

  opt = newOption(Option::BOOL, Option::WAIT_FOR_SYNC, "wait-for-sync");
  opt->help = "Sleep after create table";
  opt->setBool(0);
  opt->setArgs(no_argument);

  opt = newOption(Option::BOOL, Option::RANDOM_COLUMNS, "random-columns");
  opt->help = "use random number of column for table";
  opt->setBool(false);
  opt->setArgs(no_argument);

  opt = newOption(Option::BOOL, Option::RANDOM_INDEXES, "random-indexes");
  opt->help = "Use random number of indexes for each table ";
  opt->setBool(false);
  opt->setArgs(no_argument);

  opt = newOption(Option::INT, Option::POSITIVE_INT_PROB, "positive-prob");
  opt->help =
      "Probablity of using positive interger for random value in a int column";
  opt->setInt(70);

  opt = newOption(Option::INT, Option::CALL_FUNCTION, "call-function-prob");
  opt->help = "Probability of calling function ";
  opt->setInt(10);
  opt->setSQL();
  opt->short_help = "Function";
  opt->setDDL();

  /* todo set default to all */
  opt = newOption(Option::STRING, Option::ENCRYPTION_TYPE, "encryption-type");
  opt->help =
      "all ==> keyring/Y/N \n oracle ==> Y/N \n x ==> x \n if some string "
      "other than all/oracle is given it would use it as encryption type";
  opt->setString("oracle");

  /* create,alter,drop undo tablespace */
  opt = newOption(Option::INT, Option::UNDO_SQL, "undo-tbs-sql");
  opt->help = "Assign probability of running create/alter/drop undo tablespace";
  opt->setInt(1);
  opt->setSQL();
  opt->short_help = "UndoTablespace";
  opt->setDDL();

  /* Add new table */
  opt = newOption(Option::INT, Option::ADD_NEW_TABLE, "add-table");
  opt->help = "Add new table";
  opt->setInt(1);
  opt->setSQL();
  opt->short_help = "AddTable";
  opt->setDDL();

  opt = newOption(Option::BOOL, Option::SINGLE_THREAD_DDL, "single-thread-ddl");
  opt->help = "Single thread DDL. Execute all ddl by a single thread";
  opt->setBool(false);
  opt->setArgs(no_argument);

  opt = newOption(Option::INT, Option::THREAD_DOING_ONLY_SELECT,
                  "select-threads");
  opt->help = "Number of threads doing only select. ";
  opt->setInt(0);

  /* disable virtual columns*/
  opt = newOption(Option::BOOL, Option::NO_VIRTUAL_COLUMNS, "no-virtual");
  opt->help = "Disable virtual columns";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* each thread to work on single table */
  opt = newOption(Option::BOOL, Option::THREAD_PER_TABLE, "thread-per-table");
  opt->help = "Each thread to work on single table . If there is 10 threads "
              "and 30 tables then pstress will work on on 10 tables and each "
              "thread will work on one table";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* disable blob columns*/
  opt = newOption(Option::BOOL, Option::NO_BLOB, "no-blob");
  opt->help = "Disable blob columns";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* disable json columns */
  opt = newOption(Option::BOOL, Option::NO_JSON, "no-json");
  opt->help = "Disable json columns";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* use session random timezone */
  opt = newOption(Option::INT, Option::RANDOM_TIMEZONE, "timezone-session");
  opt->help = "Use random timezone for each session";
  opt->setInt(1);
  opt->short_help = "Timezone";
  opt->setSQL();

  /* disable all type of encrytion */
  opt = newOption(Option::BOOL, Option::NO_TABLESPACE, "no-tbs");
  opt->help = "disable all type of tablespace including the general tablespace";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* call SET SESSION wsrep_osu_method=NBO  befor drop table */
  opt = newOption(Option::INT, Option::DROP_WITH_NBO, "drop-with-nbo-prob");
  opt->help = "call SET SESSION wsrep_osu_method=NBO  befor drop table";
  opt->setInt(0);

  /* Initial Table */
  opt = newOption(Option::INT, Option::TABLES, "tables");
  opt->help = "Number of initial tables";
  opt->setInt(10);

  /* Number of indexes in a table */
  opt = newOption(Option::INT, Option::INDEXES, "indexes");
  opt->help = "maximum indexes in a table,default depends on page-size as well";
  opt->setInt(7);

  /* Process option prob file */
  opt = newOption(Option::STRING, Option::OPTION_PROB_FILE, "option-prob-file");
  opt->help = " option prob file, see File should contain lines "
              "like\n 20:option1=on|off\n, means 20% chances that it would be "
              "processed. For each seed it would pick one option from the ";
  opt->setString("");

  opt = newOption(Option::BOOL, Option::NO_TIMESTAMP, "no-timestamp");
  opt->help = "Disable timestamp";
  opt->setBool(false);
  opt->setArgs(no_argument);

  opt = newOption(Option::BOOL, Option::NO_DATE, "no-date");
  opt->help = "Disable date";
  opt->setBool(false);
  opt->setArgs(no_argument);

  opt = newOption(Option::BOOL, Option::NO_BIT, "no-bit");
  opt->help = "Disable bit";
  opt->setBool(false);
  opt->setArgs(no_argument);

  opt = newOption(Option::BOOL, Option::NO_DATETIME, "no-datetime");
  opt->help = "Disable time";
  opt->setBool(false);
  opt->setArgs(no_argument);

  opt = newOption(Option::BOOL, Option::COMPARE_RESULT, "compare-result");
  opt->help =
      "Compare result of SELECT query with and without secondary ENGINE. User "
      "is resposible to ensure no change in data during the test. ";
  opt->setBool(false);
  opt->setArgs(no_argument);

  opt = newOption(Option::STRING, Option::SECONDARY_ENGINE, "secondary-engine");
  opt->help = "Use secondary engine for some tables";

  /* algorithm for alter */
  opt = newOption(Option::STRING, Option::ALGORITHM, "alter-algorithm");
  opt->help = "algorithm used in alter table.\n"
              "--alter-algorithm INPLACE,COPY,DEFAULT\n --alter-algorithm all "
              "means randomly one of them will be picked. Pass options comma "
              "seperated without space";
  opt->setString("all");

  opt = newOption(Option::STRING, Option::COLUMN_TYPES, "column-types");
  opt->help = "Column types to be used in the table. Pass options comma "
              "seperated without space example --column-type=int,date. It will "
              "create table with only int and date";
  opt->setString("all");

  /* lock for alter */
  opt = newOption(Option::STRING, Option::LOCK, "alter-lock");
  opt->help = "lock mechanism used in alter table.\n "
              "--alter-lock DEFAULT,NONE,SHARED,EXCLUSIVE\n --alter-lock all "
              "means randomly one of them will be picked. Pass options comma "
              "seperated without space";
  opt->setString("all");

  /* Number of columns in a table */
  opt = newOption(Option::INT, Option::COLUMNS, "columns");
  opt->help = "maximum columns in a table, default depends on page-size, "
              "branch. for 8.0 it is 7 for 5.7 it 10";
  opt->setInt(10);

  /* Number of columns in an index of a table */
  opt = newOption(Option::INT, Option::INDEX_COLUMNS, "index-columns");
  opt->help = "maximum columns in an index of a table, default depends on "
              "page-size as well";
  opt->setInt(10);

  /* autoinc column */
  opt = newOption(Option::BOOL, Option::NO_AUTO_INC, "no-auto-inc");
  opt->help = "Disable auto inc columns in table, including pkey";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* desc index support */
  opt = newOption(Option::BOOL, Option::NO_DESC_INDEX, "no-desc-index");
  opt->help = "Disable index with desc on tables ";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* unique index probability out of thousand */
  opt =
      newOption(Option::INT, Option::UNIQUE_INDEX_PROB_K, "unique-key-prob-k");
  opt->help = "probability of creating unique index on table out of 1000. "
              "Currently only supported if a table have int column";
  opt->setInt(1);

  /* Only Partition tables */
  opt =
      newOption(Option::BOOL, Option::ONLY_PARTITION, "only-partition-tables");
  opt->help = "Work only on Partition tables";
  opt->setArgs(no_argument);
  opt->setBool(false);

  /* Only Temporary tables */
  opt = newOption(Option::BOOL, Option::ONLY_TEMPORARY, "only-temp-tables");
  opt->help = "Work only on temporary tables";
  opt->setArgs(no_argument);
  opt->setBool(false);

  /* No FK tables */
  opt = newOption(Option::BOOL, Option::NO_FK, "no-fk-tables");
  opt->help = "do not work on foriegn tables";
  opt->setArgs(no_argument);
  opt->setBool(false);

  /* No Partition tables */
  opt = newOption(Option::BOOL, Option::NO_PARTITION, "no-partition-tables");
  opt->help = "do not work on partition tables";
  opt->setArgs(no_argument);
  opt->setBool(false);

  /* NO Temporary tables */
  opt = newOption(Option::BOOL, Option::NO_TEMPORARY, "no-temp-tables");
  opt->help = "do not work on temporary tables";
  opt->setArgs(no_argument);
  opt->setBool(false);

  opt = newOption(Option::INT, Option::FK_PROB, "fk-prob");
  opt->help = R"(
    Probability of each normal table having the FK. Currently, FKs are only linked
    to the primary key of the parent table. So, even with 100% probability, a table
    will have an FK only if its parent has a primary key.
  )";
  opt->setInt(50);

  opt = newOption(Option::INT, Option::PARTITION_PROB, "partition-prob");
  opt->help = "Probability of parititon tables";
  opt->setInt(10);

  /* Ratio of temporary table to normal table */
  opt = newOption(Option::INT, Option::TEMPORARY_PROB, "temporary-prob");
  opt->help = "Probability of temporary tables";
  opt->setInt(10);

  /* Initial Records in table */
  opt = newOption(Option::STRING, Option::INITIAL_RECORDS_IN_TABLE, "records");
  opt->help =
      "Number of initial records (N) in each table. The table will have random "
      "records in range of 0 to N. Also check --random-initial-records. You "
      "can "
      "also pass 10K, 1M  ";
  opt->setString("1K");

  /* plain rewrite */
  opt = newOption(Option::BOOL, Option::PLAIN_REWRITE, "plain-rewrite");
  opt->help = "Execute rewrite without passing any option";
  opt->setBool(false);
  opt->setArgs(no_argument);

  opt =
      newOption(Option::BOOL, Option::LOG_PK_BULK_INSERT, "log-bulk-insert-pk");
  opt->help = "Log primary key of table used during bulk insert";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* Initial Records in table */
  opt = newOption(Option::BOOL, Option::RANDOM_INITIAL_RECORDS,
                  "random-initial-records");
  opt->help = " When passed with --records (N) option "
              " rand(N) records are inserted";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* Execute workload for number of seconds */
  opt = newOption(Option::INT, Option::NUMBER_OF_SECONDS_WORKLOAD, "seconds");
  opt->help = "Number of seconds to execute workload";
  opt->setInt(1000);

  /* number of queries N last queries */
  opt = newOption(Option::INT, Option::N_LAST_QUERIES, "log-last-queries");
  opt->help = "Number of N queries logged in logdir /*sql last queries. F "
              "means query was FAIL and S mean it was success";
  opt->setInt(10);

  /* primary key probability */
  opt = newOption(Option::INT, Option::PRIMARY_KEY, "pk-prob");
  opt->help = "Probability of adding primary key in a table";
  opt->setInt(50);

  /*Encrypt table */
  opt = newOption(Option::INT, Option::ALTER_TABLE_ENCRYPTION,
                  "alter-table-encrypt");
  opt->help = "Alter table set Encryption";
  opt->setInt(10);
  opt->setSQL();
  opt->short_help = "AlterEncrypt";
  opt->setDDL();

  opt = newOption(Option::INT, Option::BULK_INSERT_WIDTH, "bulk-insert-width");
  opt->help = "The maximum width of each insert in bulk insert default is " +
              std::to_string(1024 * 1024);
  opt->setInt(1024 * 1024);

  /* ENFORCE REWRITE  */
  opt = newOption(Option::INT, Option::ENFORCE_MERGE, "enforce-merge-prob");
  opt->help = "ENFORCE MERGE in secondary by calling PRAGMA rewrite. It calls "
              "rewrite with partial merge";
  opt->setInt(1);
  opt->setSQL();
  opt->short_help = "Rewrite";
  opt->setDDL();

  opt = newOption(Option::INT, Option::REWRITE_ROW_GROUP_MIN_ROWS,
                  "enforce-row-group-min-rows-prob");
  opt->help = "row_group_min_rows prob in REWRITE TABLE";
  opt->setInt(60);

  opt = newOption(Option::INT, Option::REWRITE_ROW_GROUP_MAX_BYTES,
                  "enfroce-row-group-max-bytes-prob");
  opt->help = "row_group_max_bytes prob in REWRITE TABLE";
  opt->setInt(60);

  opt = newOption(Option::INT, Option::REWRITE_ROW_GROUP_MAX_ROWS,
                  "enforce-row-group-max-rows-prob");
  opt->help = "row_group_max_rows prob in REWRITE TABLE";
  opt->setInt(60);

  opt = newOption(Option::INT, Option::REWRITE_DELTA_NUM_ROWS,
                  "enforce-delta-num-rows-prob");
  opt->help = "delta_num_rows prob in REWRITE TABLE";
  opt->setInt(60);

  opt = newOption(Option::INT, Option::REWRITE_DELTA_NUM_UNDO,
                  "enforce-delta-num-undo-prob");
  opt->help = "delta_num_undo prob in REWRITE TABLE";
  opt->setInt(60);

  opt = newOption(Option::INT, Option::REWRITE_GC, "enforce-gc-prob");
  opt->help = "gc prob in REWRITE TABLE";
  opt->setInt(60);

  opt =
      newOption(Option::INT, Option::REWRITE_BLOCKING, "enforce-blocking-prob");
  opt->help = "blocking prob in REWRITE TABLE";
  opt->setInt(60);

  opt = newOption(Option::INT, Option::REWRITE_MAX_ROW_ID_HASH_MAP,
                  "enforce-max-row-id-hash-map-prob");
  opt->help = "max_row_id_hash_map prob in REWRITE TABLE";
  opt->setInt(60);

  opt = newOption(Option::INT, Option::REWRITE_FORCE, "enforce-force-prob");
  opt->help = "force prob in REWRITE TABLE";
  opt->setInt(60);

  opt = newOption(Option::INT, Option::REWRITE_NO_RESIDUAL,
                  "enforce-no-residual-prob");
  opt->help = "no_residual prob in REWRITE TABLE";
  opt->setInt(60);

  opt = newOption(Option::INT, Option::REWRITE_MAX_INTERNAL_BLOB_SIZE,
                  "enforce-max-internal-blob-size-prob");
  opt->help = "max_internal_blob_size prob in REWRITE TABLE";
  opt->setInt(60);

  opt = newOption(Option::INT, Option::REWRITE_BLOCK_COOKER_ROW_GROUP_MAX_ROWS,
                  "enforce-block-cooker-row-group-max-rows-prob");
  opt->help = "block_cooker_row_group_max_rows prob in REWRITE TABLE";
  opt->setInt(60);

  opt = newOption(Option::INT, Option::REWRITE_PARTIAL, "enforce-partial-prob");
  opt->help = "partial prob in REWRITE TABLE";
  opt->setInt(60);

  /* secondary GARBAGE COLLECT */
  opt = newOption(Option::INT, Option::SECONDARY_GC, "secondary-gc");
  opt->help = "Execute garbage collect in secondary";
  opt->setInt(1);
  opt->setSQL();
  opt->short_help = "GC";
  opt->setDDL();

  /* probability of null value in a column */
  opt = newOption(Option::INT, Option::NULL_PROB, "null-prob-k");
  opt->help = "Probability that a column would have null value. It is used for "
              "update, insert, delete and also in where clause";
  opt->setInt(1);

  /* probability of null  columns */
  opt = newOption(Option::INT, Option::UNIQUE_RANGE, "range");
  opt->help = "range for random number int, integers, floats and double.  more "
              "the value. Default to 100. If target is success insert the "
              "choose a high value. If it is update/delete choose low value  ";
  opt->setInt(100);

  /* dictionary file */
  opt = newOption(Option::STRING, Option::DICTIONARY_FILE, "dictionary-file");
  opt->help = "Dictionary file for random string";
  opt->setString("english_dictionary.txt");

  /* disable text columns*/
  opt = newOption(Option::BOOL, Option::NO_TEXT, "no-text");
  opt->help = "Disable text columns";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* probability of creating not-secondary columns out of hundred */
  opt = newOption(Option::INT, Option::NOT_SECONDARY, "column-skip-to-secondary");
  opt->help = "Probability of creating not secondary columns";
  opt->setInt(0);

  /* probability of modifying columns with/without NOT SECONDARY clause" */
  opt = newOption(Option::INT, Option::MODIFY_COLUMN_SECONDARY_ENGINE, "alter-column-secondary-engine");
  opt->help = "Probability of modifying existing column with NOT SECONDARY clause";
  opt->setInt(1);
  opt->setSQL();
  opt->short_help = "AlterColumnSecondary";
  opt->setDDL();

  /* probability of modifying columns with/without NOT SECONDARY clause" */
  opt = newOption(Option::INT, Option::USING_PK_PROB, "using-pkey");
  opt->help = "Probability of using pk column in where clause. if table does "
              "not pk column then first column of index. and if it does not "
              "have index then any random column";
  opt->setInt(50);

  opt = newOption(Option::BOOL, Option::NO_PKEY_IN_SET, "no-pk-in-set");
  opt->help = "Do not use ipkey column in update tt_N set col=";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* disable char columns*/
  opt = newOption(Option::BOOL, Option::NO_CHAR, "no-char");
  opt->help = "Disable char columns";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* disable VARCHAR columns*/
  opt = newOption(Option::BOOL, Option::NO_VARCHAR, "no-varchar");
  opt->help = "Disable varchar columns";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* disable FLOAT columns*/
  opt = newOption(Option::BOOL, Option::NO_FLOAT, "no-float");
  opt->help = "Disable float columns";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* disable double columns*/
  opt = newOption(Option::BOOL, Option::NO_DOUBLE, "no-double");
  opt->help = "Disable double columns";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* disable bool columns*/
  opt = newOption(Option::BOOL, Option::NO_BOOL, "no-bool");
  opt->help = "Disable bool columns";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* disable integer columns*/
  opt = newOption(Option::BOOL, Option::NO_INTEGER, "no-integer");
  opt->help = "Disable integer columns";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* disable int columns*/
  opt = newOption(Option::BOOL, Option::NO_INT, "no-int");
  opt->help = "Disable int  columns";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* modify column */
  opt = newOption(Option::INT, Option::ALTER_COLUMN_MODIFY, "modify-column");
  opt->help = "Alter table column modify";
  opt->setInt(10);
  opt->setSQL();
  opt->short_help = "ModifyColumn";
  opt->setDDL();

  /*compress table */
  opt = newOption(Option::INT, Option::ALTER_TABLE_COMPRESSION,
                  "alter-table-compress");
  opt->help = "Alter table compression";
  opt->setInt(10);
  opt->setSQL();
  opt->short_help = "AlterCompress";
  opt->setDDL();

  /* Row Format */
  opt = newOption(Option::STRING, Option::ROW_FORMAT, "row-format");
  opt->help =
      "create table row format. it is the row format of table. a "
      "table can have compressed, dynamic, redundant row format.\n "
      "valid values are :\n all: use compressed, dynamic, redundant. all "
      "combination key block size will be used. \n uncompressed: do not use "
      "compressed row_format, i.e. key block size will not used. \n"
      "none: do not use any encryption";
  opt->setString("all");

  /* MySQL server option */
  opt = newOption(Option::STRING, Option::MYSQLD_SERVER_OPTION, "mso");
  opt->help =
      "mysqld server options variables which are set during the load, see "
      "--set-variable. n:option=v1=v2 where n is probabality of picking "
      "option, v1 and v2 different value that is supported. "
      "for e.g. --md=20:innodb_temp_tablespace_encrypt=on=off";

  opt = newOption(Option::STRING, Option::SERVER_OPTION_FILE, "sof");
  opt->help =
      "server options file, MySQL server options file, picks some of "
      "the mysqld options, "
      "and try to set them during the load , using set global and set "
      "session.\n see --set-variable.\n File should contain lines like\n "
      "20:innodb_temp_tablespace_encrypt=on=off\n, means 20% chances "
      "that it would be processed. ";

  /* Set Global */
  opt = newOption(Option::INT, Option::SET_GLOBAL_VARIABLE, "set-variable");
  opt->help = "set mysqld variable during the load.(session|global)";
  opt->setInt(3);
  opt->setSQL();
  opt->short_help = "SetVariable";
  opt->setDDL();

  /* alter instance disable/enable redo logging */
  opt = newOption(Option::INT, Option::ALTER_REDO_LOGGING, "alter-redo-log");
  opt->help = "Alter instance enable/disable redo log";
  opt->setInt(0);
  opt->setSQL();
  opt->short_help = "AlterRedo";
  opt->setDDL();

  /* alter instance rotate innodb master key */
  opt = newOption(Option::INT, Option::ALTER_MASTER_KEY, "rotate-master-key");
  opt->help = "Alter instance rotate innodb master key";
  opt->setInt(1);
  opt->setSQL();
  opt->short_help = "RotateMaster";
  opt->setDDL();

  opt = newOption(Option::INT, Option::ALTER_SECONDARY_ENGINE,
                  "alter-secondary-engine");
  opt->help = "run secondary-engine sql";
  opt->setInt(1);
  opt->setSQL();
  opt->short_help = "AlterSecondary";
  opt->setDDL();

  /* alter instance rotate innodb system key */
  opt = newOption(Option::INT, Option::ALTER_ENCRYPTION_KEY,
                  "rotate-encryption-key");
  opt->help = "Alter instance rotate innodb system key X";
  opt->setInt(1);
  opt->setSQL();
  opt->short_help = "AlterInstanceSystemKey";
  opt->setDDL();

  /* alter instance rotate gcache master key */
  opt = newOption(Option::INT, Option::ALTER_GCACHE_MASTER_KEY,
                  "rotate-gcache-key");
  opt->help = "Alter instance rotate gcache master key";
  opt->setInt(1);
  opt->setSQL();
  opt->short_help = "AlterInstanceGcache";
  opt->setDDL();

  /* Reload keyring component configuration */
  opt = newOption(Option::INT, Option::ALTER_INSTANCE_RELOAD_KEYRING,
                  "reload-keyring");
  opt->help = "Alter instance reload keyring";
  opt->setInt(1);
  opt->setSQL();
  opt->short_help = "AlterInstanceReloadKey";
  opt->setDDL();

  /* rotate redo log key */
  opt = newOption(Option::INT, Option::ROTATE_REDO_LOG_KEY,
                  "rotate-redo-log-key");
  opt->help = "Rotate redo log key";
  opt->setInt(1);
  opt->short_help = "RotateRedoLog";
  opt->setSQL();
  opt->setDDL();

  /*Tablespace Encrytion */
  opt = newOption(Option::INT, Option::ALTER_TABLESPACE_ENCRYPTION,
                  "alt-tbs-enc");
  opt->help = "Alter tablespace set Encryption including the mysql tablespace";
  opt->setInt(1);
  opt->setSQL();
  opt->short_help = "AlterTablespaceEncrypt";
  opt->setDDL();

  /*Discard tablespace */
  opt = newOption(Option::INT, Option::ALTER_DISCARD_TABLESPACE,
                  "alt-discard-tbs");
  opt->help = "Alter table to discard file-per-tablespace";
  opt->setInt(1);
  opt->setSQL();
  opt->short_help = "AlterDiscardTablespace";
  opt->setDDL();

  /*Database Encryption */
  opt = newOption(Option::INT, Option::ALTER_DATABASE_ENCRYPTION, "alt-db-enc");
  opt->help = "Alter Database Encryption mode to Y/N";
  opt->setInt(1);
  opt->setSQL();
  opt->short_help = "AlterDatabaseEncrypt";
  opt->setDDL();

  /*Database collation */
  opt = newOption(Option::INT, Option::ALTER_DATABASE_COLLATION, "alt-db-col");
  opt->help = "Alter Database collation probability";
  opt->setInt(1);
  opt->setSQL();
  opt->short_help = "AlterDatabaseCollation";
  opt->setDDL();

  /* Tablespace Rename */
  opt =
      newOption(Option::INT, Option::ALTER_TABLESPACE_RENAME, "alt-tbs-rename");
  opt->help = "Alter tablespace rename";
  opt->setInt(1);
  opt->setSQL();
  opt->short_help = "AlterTablespaceRename";
  opt->setDDL();

  /* SELECT */
  opt = newOption(Option::BOOL, Option::NO_SELECT, "no-select");
  opt->help = "do not execute any type select on tables";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* Add no cascade to FK */
  opt = newOption(Option::BOOL, Option::NO_FK_CASCADE, "no-cascade");
  opt->help = "Disable to cacading of FK in pstress";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* INSERT */
  opt = newOption(Option::BOOL, Option::NO_INSERT, "no-insert");
  opt->help = "do not execute insert into tables";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* UPDATE */
  opt = newOption(Option::BOOL, Option::NO_UPDATE, "no-update");
  opt->help = "do not execute any type of update on tables";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* DELETE */
  opt = newOption(Option::BOOL, Option::NO_DELETE, "no-delete");
  opt->help = "do not execute any type of delete on tables";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* PREPARE new metadata */
  opt = newOption(Option::BOOL, Option::PREPARE, "prepare");
  opt->help = "create new random tables and insert initial records";
  opt->setBool(false);
  opt->setArgs(no_argument);

  opt = newOption(Option::INT, Option::SELECT_ROW_USING_PKEY, "select-precise");
  opt->help = "Select table using single row";
  opt->short_help = "SP";
  opt->setInt(800);
  opt->setSQL();

  opt = newOption(Option::INT, Option::THROTTLE_SLEEP, "throttle");
  opt->help = "slowing the thread";
  opt->setInt(0);
  opt->setSQL();
  opt = newOption(Option::INT, Option::SELECT_FOR_UPDATE,
                  "select-precise-update");
  opt->help = "Select table using single for update";
  opt->short_help = "SPU";
  opt->setInt(8);
  opt->setSQL();

  opt = newOption(Option::INT, Option::SELECT_ALL_ROW, "select-bulk");
  opt->help = "select all table data and in case of partition randomly pick "
              "some partition";
  opt->setInt(8);
  opt->short_help = "SB";
  opt->setSQL();

  opt = newOption(Option::BOOL, Option::ONLY_SELECT, "only-select");
  opt->help = "Disable all other SQL";
  opt->setBool(false);
  opt->setArgs(no_argument);

  opt = newOption(Option::INT, Option::SELECT_FOR_UPDATE_BULK,
                  "select-bulk-update");
  opt->help = "SELECT bulk for update ";
  opt->setInt(20);
  opt->short_help = "SBU";
  opt->setSQL();

  opt = newOption(Option::INT, Option::NON_INT_PK, "non-int-pk-prob");
  opt->help = "Probability of primary key column being non integer";
  opt->setInt(25);

  opt = newOption(Option::INT, Option::PK_COLUMN_AUTOINC, "pk-auto-inc-prob");
  opt->help = "Probability of primary key column being auto increment";
  opt->setInt(75);

  opt = newOption(Option::INT, Option::INSERT_BULK_COUNT, "insert-bulk-count");
  opt->help = "Number of rows to insert in a single insert statement";
  opt->setInt(10);

  opt = newOption(Option::INT, Option::INSERT_BULK, "insert-bulk");
  opt->help = "insert random row";
  opt->setInt(1);
  opt->short_help = "IB";
  opt->setSQL();

  opt = newOption(Option::INT, Option::INSERT_RANDOM_ROW, "insert");
  opt->help = "insert random row";
  opt->setInt(600);
  opt->short_help = "I";
  opt->setSQL();

  /* Update row using pkey */
  opt = newOption(Option::INT, Option::UPDATE_ROW_USING_PKEY, "update-precise");
  opt->help = "Update using where clause";
  opt->setInt(200);
  opt->short_help = "UP";
  opt->setSQL();

  opt = newOption(Option::INT, Option::UPDATE_ALL_ROWS, "update-bulk");
  opt->help = "Update bulk of a table";
  opt->setInt(10);
  opt->short_help = "UB";
  opt->setSQL();

  /* Delete row using pkey */
  opt = newOption(Option::INT, Option::DELETE_ROW_USING_PKEY, "delete-precise");
  opt->help = "delete where condition";
  opt->setInt(100);
  opt->short_help = "DP";
  opt->setSQL();

  /* Delete all rows */
  opt = newOption(Option::INT, Option::DELETE_ALL_ROW, "delete-bulk");
  opt->help = "delete bulk rows of table";
  opt->setInt(8);
  opt->short_help = "DB";
  opt->setSQL();

  /* Drop column */
  opt = newOption(Option::INT, Option::DROP_COLUMN, "drop-column");
  opt->help = "alter table drop some random column";
  opt->setInt(1);
  opt->setSQL();
  opt->short_help = "DropColumn";
  opt->setDDL();

  /* Add column */
  opt = newOption(Option::INT, Option::ADD_COLUMN, "add-column");
  opt->help = "alter table add some random column";
  opt->setInt(1);
  opt->setSQL();
  opt->short_help = "AddColumn";
  opt->setDDL();

  /* Drop index */
  opt = newOption(Option::INT, Option::DROP_INDEX, "drop-index");
  opt->help = "alter table drop random index";
  opt->setInt(1);
  opt->setSQL();
  opt->short_help = "DropIndex";
  opt->setDDL();

  /* Add column */
  opt = newOption(Option::INT, Option::ADD_INDEX, "add-index");
  opt->help = "alter table add random index";
  opt->setInt(1);
  opt->short_help = "AddIndex";
  opt->setSQL();
  opt->setDDL();

  /* Rename Index */
  opt = newOption(Option::INT, Option::RENAME_INDEX, "rename-index");
  opt->help = "alter table rename index";
  opt->setInt(1);
  opt->setSQL();
  opt->short_help = "RenameIndex";
  opt->setDDL();

  /* Rename Column */
  opt = newOption(Option::INT, Option::RENAME_COLUMN, "rename-column");
  opt->help = "alter table rename column";
  opt->setInt(1);
  opt->setSQL();
  opt->short_help = "RenameColumn";
  opt->setDDL();

  /* Analyze Table */
  opt = newOption(Option::INT, Option::ANALYZE, "analyze");
  opt->help = "analyze table, for partition table randomly analyze either "
              "partition or full table";
  opt->setInt(1);
  opt->setSQL();
  opt->short_help = "Analyze";
  opt->setDDL();

  /* Check Table */
  opt = newOption(Option::INT, Option::CHECK_TABLE, "check");
  opt->help = "check table, for partition table randomly check either "
              "partition or full table";
  opt->setInt(1);
  opt->setSQL();
  opt->short_help = "Check";
  opt->setDDL();

  /* Check Table Pre-load */
  opt = newOption(Option::BOOL, Option::CHECK_TABLE_PRELOAD, "check-preload");
  opt->help = "check table, for partition table randomly check either "
              "partition or full table before the load is started";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* Add drop Partition */
  opt =
      newOption(Option::INT, Option::ADD_DROP_PARTITION, "add-drop-partition");
  opt->help = "randomly add drop new partitions";
  opt->setInt(3);
  opt->setSQL();
  opt->short_help = "AddDropPartition";
  opt->setDDL();

  /* maximum number Partition */
  opt = newOption(Option::INT, Option::MAX_PARTITIONS, "max-partitions");
  opt->help =
      "maximum number of partitions in table. choose between 1 and 8192";
  opt->setInt(25);

  /* Total number of partition supported */
  opt =
      newOption(Option::STRING, Option::PARTITION_SUPPORTED, "partition-types");
  opt->help =
      "total partition supported, all for LIST, HASH, KEY, RANGE or to provide "
      "or pass for example --partition-types LIST,HASH,RANGE without space";
  opt->setString("all");

  /* Optimize Table */
  opt = newOption(Option::INT, Option::OPTIMIZE, "optimize");
  opt->help = "optimize table, for paritition table randomly optimize either "
              "partition or full table ";
  opt->setInt(3);
  opt->setSQL();
  opt->short_help = "Optimize";
  opt->setDDL();

  /* Truncate table */
  opt = newOption(Option::INT, Option::TRUNCATE, "truncate");
  opt->help = "truncate table or in case of partition truncate partition";
  opt->setInt(1);
  opt->setSQL();
  opt->short_help = "Truncate";
  opt->setDDL();

  /* Drop and recreate table */
  opt = newOption(Option::INT, Option::DROP_CREATE, "recreate-table");
  opt->help = "drop and recreate table";
  opt->setInt(1);
  opt->setSQL();
  opt->short_help = "RecreateTable";
  opt->setDDL();

  /* DATABASE */
  opt = newOption(Option::STRING, Option::DATABASE, "database");
  opt->help = "The database to connect to";
  opt->setString("test");

  /* Address */
  opt = newOption(Option::STRING, Option::ADDRESS, "address");
  opt->help = "IP address to connect to";

  /* Infile */
  opt = newOption(Option::STRING, Option::INFILE, "infile");
  opt->help = "The SQL input file";
  opt->setString("pquery.sql");

  /* Logdir */
  opt = newOption(Option::STRING, Option::LOGDIR, "logdir");
  opt->help = "Log directory";
  opt->setString("/tmp");

  /* Socket */
  opt = newOption(Option::STRING, Option::SOCKET, "socket");
  opt->help = "Socket file to use";

  {
    /* grep from env */
    const char *socket_env = getenv("SOCKET");
    if (socket_env) {
      opt->setString(socket_env);
    } else
      opt->setString("/tmp/socket.sock");
  }

  /*config file */
  opt = newOption(Option::STRING, Option::CONFIGFILE, "config-file");
  opt->help = "Config file to use for test";

  /*Port */
  opt = newOption(Option::STRING, Option::PORT, "port");
  opt->help =
      "Ports to use, It value is a LIST like 3108,3403,3408, Then it "
      "would create 3 nodes to connect and run load on each node. Also to "
      "connect using port, pass --address for example --address=127.0.0.1 ";
  opt->setString("3306");

  /* Password*/
  opt = newOption(Option::STRING, Option::PASSWORD, "password");
  opt->help = "The MySQL user's password";
  opt->setString("");

  /* HELP */
  opt = newOption(Option::BOOL, Option::HELP, "help");
  opt->help = "user asked for help";
  opt->setArgs(optional_argument);

  opt = newOption(Option::BOOL, Option::VERBOSE, "verbose");
  opt->help = "verbose";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* Threads */
  opt = newOption(Option::INT, Option::THREADS, "threads");
  opt->help = "The number of threads to use";
  opt->setInt(1);

  /* User*/
  opt = newOption(Option::STRING, Option::USER, "user");
  opt->help = "The MySQL userID to be used";
  opt->setString("root");

  /* log all queries */
  opt = newOption(Option::BOOL, Option::LOG_ALL_QUERIES, "log-all-queries");
  opt->help = "Log all queries (succeeded and failed)";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* execute sql sequentially */
  opt = newOption(Option::BOOL, Option::NO_SHUFFLE, "no-shuffle");
  opt->help = "execute SQL sequentially | randomly\n";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* log query statistics */
  opt = newOption(Option::BOOL, Option::LOG_QUERY_STATISTICS,
                  "log-query-statistics");
  opt->help = "extended output of query result";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* log client output*/
  opt = newOption(Option::BOOL, Option::LOG_CLIENT_OUTPUT, "log-client-output");
  opt->help = "Log query output to separate file";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* log query number*/
  opt = newOption(Option::BOOL, Option::LOG_QUERY_NUMBERS, "log-query-numbers");
  opt->help = "write query # to logs";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* log query duration */
  opt =
      newOption(Option::BOOL, Option::LOG_QUERY_DURATION, "log-query-duration");
  opt->help = "Log query duration in milliseconds";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* log failed queries */
  opt =
      newOption(Option::BOOL, Option::LOG_FAILED_QUERIES, "log-failed-queries");
  opt->help = "Log all failed queries";
  opt->setBool(true);
  opt->setArgs(no_argument);

  /* log success queries */
  opt = newOption(Option::BOOL, Option::LOG_SUCCEDED_QUERIES,
                  "log-succeeded-queries");
  opt->help = "Log succeeded queries";
  opt->setBool(false);
  opt->setArgs(no_argument);

  /* queries per thread */
  opt =
      newOption(Option::INT, Option::QUERIES_PER_THREAD, "queries-per-thread");
  opt->help = "The number of queries per thread";
  opt->setInt(1);

  /* test connection */
  opt = newOption(Option::BOOL, Option::TEST_CONNECTION, "test-connection");
  opt->help = "Test connection to server and exit";
  opt->setBool(false);
  opt->setArgs(no_argument);

  opt = newOption(Option::INT, Option::PRINT_TRANSACTION_RATE, "print-rate");
  opt->help = "Print transaction rate per N second";
  opt->setInt(0);

  /* transaction probability */
  opt = newOption(Option::INT, Option::TRANSATION_PRB_K, "trx-prob-k");
  opt->help = "probability(out of 1000) of combining sql as single trx";
  opt->setInt(1);

  /* XA TRansaction */
  opt = newOption(Option::INT, Option::XA_TRANSACTION, "xa-trx-prob-k");
  opt->help = "Probablity of running XA transaction. Trx size option control "
              "the size of XA transaction";
  opt->setInt(0);

  /* Probablity of killing running transaction */
  opt = newOption(Option::INT, Option::KILL_TRANSACTION, "kill-trx-prob-k");
  opt->help = "Probablity of killing running transaction";
  opt->setInt(1);
  opt->setSQL();
  opt->short_help = "KillQuery";
  opt->setDDL();

  /* tranasaction size */
  opt = newOption(Option::INT, Option::TRANSACTIONS_SIZE, "trx-size");
  opt->help = "average size of each trx";
  opt->setInt(10);

  /* probability of executing commit */
  opt = newOption(Option::INT, Option::COMMIT_PROB, "commit-prob");
  opt->help = "probability of executing commit after a transaction. Else it "
              "would be rollback ";
  opt->setInt(95);

  /* number of savepoints in trxs */
  opt = newOption(Option::INT, Option::SAVEPOINT_PRB_K, "savepoint-prob-k");
  opt->help = "probability of using savepoint in a transaction.\n Also 10% "
              "such transaction will be rollback to some savepoint";
  opt->setInt(10);

  /* steps */
  opt = newOption(Option::INT, Option::STEP, "step");
  opt->help = "current step in pstress script";
  opt->setInt(1);

  /* metadata file path */
  opt = newOption(Option::STRING, Option::METADATA_PATH, "metadata-path");
  opt->help = "path of metadata file";

  /* sql format for */
  opt = newOption(Option::INT, Option::GRAMMAR_SQL, "grammar-sql");
  opt->help = "grammar sql";
  opt->setInt(10);
  opt->short_help = "Grammar";
  opt->setSQL();

  /* file name of special sql */
  opt = newOption(Option::STRING, Option::GRAMMAR_FILE, "grammar-file");
  opt->help =
      "file to be used  for grammar sql\nT1_INT_1, T1_INT_2 will be replaced "
      "with int columns of some table\n in database T1_VARCHAR_1, T1_VARCHAR_2 "
      "will be replaced with varchar columns of some table in database";
  opt->setString("grammar.sql");
}

Option::~Option() {}

void Option::print_pretty() {
  std::string s;
  s = "--" + name + "      ";
  std::cout << "--" << name << ": " << help << std::endl;
  std::cout << " default";
  switch (type) {
  case STRING:
    std::cout << ": " << getString() << std::endl;
    break;
  case INT:
    std::cout << "#: " << getInt() << std::endl;
    break;
  case BOOL:
    std::cout << ": " << getBool() << std::endl;
  }
  std::cout << std::endl;
}

/* delete options and server options */
void delete_options() {
  for (auto &i : *options)
    delete i;
  delete options;
  /* delete server options */
  for (auto &i : *server_options)
    delete i;
  delete server_options;
}

void show_help(Option::Opt option) {
  /* Print all avilable options */

  if (option == Option::MAX) {
    print_version();
    for (auto &it : *options) {
      it->print_pretty();
    };
  } else {
    std::cout << " Invalid options " << std::endl;
    std::cout << " use --help --verbose to see all supported options "
              << std::endl;
  }
}

void print_version(void) {
  std::cout << " - PStress v" << PQVERSION << "-" << PQREVISION
            << " compiled with " << FORK << "-" << mysql_get_client_info()
            << std::endl;
}

void show_help(std::string help) {
  if (help.compare("verbose") == 0) {

    for (auto &op : *options) {
      if (op != nullptr)
        op->print_pretty();
    }
  }
  bool help_found = false;
  for (auto &op : *options) {
    if (op != nullptr && help.size() > 0 && help.compare(op->getName()) == 0) {
      help_found = true;
      op->print_pretty();
    }
  }
  if (!help_found)
    std::cout << "Not a valid option! " << help << std::endl;
}

void show_help() {
  print_version();
  std::cout << " - For complete help use => pstress  --help --verbose"
            << std::endl;
  std::cout << " - For help on any option => pstress --help=OPTION e.g. \n "
               "            pstress --help=ddl"
            << std::endl;
}

void show_cli_help(void) {
  print_version();
  std::cout << " - General usage: pstress --user=USER --password=PASSWORD "
               "--database=DATABASE"
            << std::endl;
  std::cout << "=> pstress doesn't support multiple nodes when using "
               "commandline options mode!"
            << std::endl;
  std::cout << "---------------------------------------------------------------"
               "--------------------------\n"
            << "| OPTION               | EXPLANATION                           "
               "       | DEFAULT         |\n"
            << "---------------------------------------------------------------"
               "--------------------------\n"
            << "--database             | The database to connect to            "
               "       | \n"
            << "--address              | IP address to connect to              "
               "       | \n"
            << "--port                 | The port to connect to                "
               "       | 3306\n"
            << "--infile               | The SQL input file                    "
               "       | pquery.sql\n"
            << "--logdir               | Log directory                         "
               "       | /tmp\n"
            << "--socket               | Socket file to use                    "
               "       | /tmp/my.sock\n"
            << "--user                 | The MySQL userID to be used           "
               "       | shell user\n"
            << "--password             | The MySQL user's password             "
               "       | <empty>\n"
            << "--threads              | The number of threads to use          "
               "       | 1\n"
            << "--queries-per-thread   | The number of queries per thread      "
               "       | 10000\n"
            << "--verbose              | Duplicates the log to console when "
               "threads=1 | no\n"
            << "--log-all-queries      | Log all queries (succeeded and "
               "failed)       | no\n"
            << "--log-succeeded-queries| Log succeeded queries                 "
               "       | no\n"
            << "--log-failed-queries   | Log failed queries                    "
               "       | no\n"
            << "--no-shuffle           | Execute SQL sequentially              "
               "       | randomly\n"
            << "--test-connection      | Test connection to server and exit    "
               "       | no\n"
            << "--log-query-number     | Write query # to logs                 "
               "       | no\n"
            << "--log-client-output    | Log query output to separate file     "
               "       | no\n"
            << "--ddl		    | USE DDL in command line option           "
               "    | true\n"
            << "---------------------------------------------------------------"
               "--------------------------"
            << std::endl;
}

void show_config_help(void) {

  print_version();

  std::cout << " - Usage: pstress --config-file=pstress.cfg" << std::endl;
  std::cout << " - CLI params has been replaced by config file (INI format)"
            << std::endl;
  std::cout << " - You can redefine any global param=value pair in "
               "host-specific section"
            << std::endl;
  std::cout << "\nConfig example:\n" << std::endl;
  std::cout <<

      "[node0.domain.tld]\n"
            << "# The database to connect to\n"
            << "database = \n"
            << "# IP address to connect to, default is AF_UNIX\n"
            << "address = <empty>\n"
            << "# The port to connect to\n"
            << "port = 3306\n"
            << "# The SQL input file\n"
            << "infile = pquery.sql\n"
            << "# Directory to store logs\n"
            << "logdir = /tmp\n"
            << "# Socket file to use\n"
            << "socket = /tmp/my.sock\n"
            << "# The MySQL userID to be used\n"
            << "user = test\n"
            << "# The MySQL user's password\n"
            << "password = test\n"
            << "# The number of threads to use by worker\n"
            << "threads = 1\n"
            << "# The number of queries per thread\n"
               "queries-per-thread = 10000\n"
            << "# Duplicates the log to console when threads=1 and workers=1\n"
               "verbose = No\n"
            << "# Log all queries\n"
            << "log-all-queries = No\n"
            << "# Log succeeded queries\n"
            << "log-succeeded-queries = No\n"
            << "# Log failed queries\n"
            << "log-failed-queries = No\n"
            << "# Log output from executed query (separate log)\n"
            << "log-client-output = No\n"
            << "# Log query numbers along the query results and statistics\n"
            << "log-query-number = No\n\n"
            << "[node1.domain.tld]\n"
            << "address = 10.10.6.10\n"
            << "# default for \"run\" is No, need to set it explicitly\n"
            << "run = Yes\n\n"
            << "[node2.domain.tld]\n"
            << "address = 10.10.6.11\n"
            << std::endl;
}

void read_option_prob_file(const std::string &prob_file) {
  std::ifstream file(prob_file);
  if (!file.is_open()) {
    std::cout << "Error opening file " << prob_file << std::endl;
    exit(EXIT_FAILURE);
  }

  std::string line;
  std::cout << "Picking options from " << prob_file << std::endl;
  while (std::getline(file, line)) {
    if (line[0] == '#' || line.empty())
      continue;

    if (line.find(':') == std::string::npos) {
      std::cerr << "Invalid format " << std::endl;
      exit(EXIT_FAILURE);
    }

    int prob = atoi(line.substr(0, line.find(":")).c_str());

    if (prob < rand_int(100)) {
      continue;
    }

    auto line_options =
        splitStringToArray<std::string>(line.substr(line.find(":") + 1), ';');

    for (auto &each_option : line_options) {

      auto option_value = splitStringToArray<std::string>(each_option, '=');
      auto option = option_value.at(0);
      std::vector<std::string> values;
      if (option_value.size() > 1)
        values = splitStringToArray<std::string>(option_value.at(1), '|');

      Option *op = nullptr;
      for (auto &i : *options) {
        if (i != nullptr && option.compare(i->getName()) == 0) {
          op = i;
          break;
        }
      }
      if (op == nullptr) {
        std::cout << "Invalid option  " << option << " found in " << prob_file;
        exit(EXIT_FAILURE);
      }
      if (op->cl)
        continue;

      if (op->getType() == Option::BOOL) {
        op->setBool(true);
        std::cout << option << std::endl;
        continue;
      }

      if (values.size() == 0) {
        std::cout << "option " << option << " does not have values in "
                  << prob_file << std::endl;
        exit(EXIT_FAILURE);
      }

      if (op->getType() == Option::INT) {
        auto value = values.at(rand_int(values.size() - 1));
        std::cout << option << ":" << value << std::endl;
        op->setInt(value);
      } else {
        auto value = values.at(rand_int(values.size() - 1));
        op->setString(value);
        std::cout << option << ":" << value << std::endl;
      }
    }
  }
}
