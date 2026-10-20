/* Every pstress option, as one table.

   This is the single place an option is declared: its Option::Opt enum
   value (common.hpp), its --long-name, its type, default, argument style,
   whether it is a workload action (OPT_SQL, and OPT_DDL for DDL) that
   run_some_query() picks by weight, the short label the per-thread summary
   prints, and the --help text. add_options() turns each row into an Option.

   To add an option: append its enum value at the END of Option::Opt (step
   metadata files record options by enum value, so inserting in the middle
   renumbers everything after it), add a row here, then read it where it is
   used with opt_int()/opt_bool()/opt_string(). --dump-options-json prints
   the resulting table at runtime.

   Workload options get a non-zero default so an ordinary run exercises
   them; 0 is only for things that are unsound by default. */

#include "common.hpp"
#include <cstdlib>
#include <cstring>
#include <iostream>

namespace {

enum : unsigned { OPT_SQL = 1, OPT_DDL = 2 };

struct OptionSpec {
  Option::Opt opt;
  const char *enum_name;
  const char *name; /* --long-name */
  Option::Type type;
  const char *def;  /* default, parsed like a command line value */
  short args;       /* required_argument, no_argument or optional_argument */
  unsigned flags;   /* OPT_SQL | OPT_DDL */
  const char *short_help;
  const char *help;
};

#define OPT(E, name, type, def, args, flags, short_help, help)                 \
  { Option::E, #E, name, Option::type, def, args, flags, short_help, help }

/* A default that depends on which backend this binary was built for. */
const char *fork_default(const char *mysql, const char *clickhouse,
                         const char *other) {
  if (strcmp(FORK, "MySQL") == 0)
    return mysql;
  if (strcmp(FORK, "ClickHouse") == 0)
    return clickhouse;
  return other;
}

/* --socket defaults to the SOCKET environment variable when it is set. */
const char *socket_default() {
  const char *env = getenv("SOCKET");
  return env ? env : "/tmp/socket.sock";
}

const OptionSpec kOptionSpecs[] = {
  /* Intial Seed for test */
  OPT(INITIAL_SEED, "seed", INT, "1", required_argument, 0, "",
      "Initial seed used for the test"),

  /* Number of General tablespaces */
  OPT(NUMBER_OF_GENERAL_TABLESPACE, "tbs-count", INT, "1", required_argument, 0, "",
      "random number of different general tablespaces "),

  /* Number of Undo tablespaces */
  OPT(NUMBER_OF_UNDO_TABLESPACE, "undo-tbs-count", INT, "0", required_argument, 0, "",
      "Number of default undo tablespaces "),

  /* Engine. For ClickHouse this names the MergeTree family member used for
     every table: MergeTree, ReplacingMergeTree, SummingMergeTree or
     AggregatingMergeTree (a Replicated prefix is added when --port names more
     than one port). The default is ReplacingMergeTree, which is what pstress
     used unconditionally before the engine was honoured; pass
     --engine=MergeTree for a source that never collapses rows, which is what
     the materialized view consistency check needs to catch duplicates. */
  OPT(ENGINE, "engine", STRING, fork_default("INNODB", "ReplacingMergeTree", ""), required_argument, 0, "",
      "Engine used "),

  /* Just Load DDL*/
  OPT(JUST_LOAD_DDL, "jlddl", BOOL, "false", no_argument, 0, "",
      "load DDL and exit"),

  /* DDL option */
  OPT(NO_DDL, "no-ddl", BOOL, "false", no_argument, 0, "",
      "do not use ddl in workload"),

  /* Only command line sql option */
  OPT(ONLY_CL_SQL, "only-cl-sql", BOOL, "false", no_argument, 0, "",
      "only run command line sql. other sql will be disable"),

  /* Only command line ddl option */
  OPT(ONLY_CL_DDL, "only-cl-ddl", BOOL, "false", no_argument, 0, "",
      "only run command line ddl. other ddl will be disable"),

  OPT(SECONDARY_AFTER_CREATE, "secondary-after-create", BOOL, "false", no_argument, 0, "",
      "set secondary engine after table is created"),

  /* select in SECONDARY */
  OPT(SELECT_IN_SECONDARY, "select-in-secondary", BOOL, "false", no_argument, 0, "",
      "Execute all SELECT in SECONDARY"),

  /* Delay in secondary*/
  OPT(DELAY_IN_SECONDARY, "delay-in-compare-ms", INT, "0", required_argument, 0, "",
      "Add milliseconds of delay before executing the secondary"),

  /* disable table compression */
  OPT(NO_TABLE_COMPRESSION, "no-table-compression", BOOL, "false", no_argument, 0, "",
      "Disable table compression"),

  /* disable column compression */
  OPT(NO_COLUMN_COMPRESSION, "no-column-compression", BOOL, "false", no_argument, 0, "",
      "Disable column compression. It is percona style compression"),

  /* disable all type of encrytion */
  OPT(USE_ENCRYPTION, "use-encryption", BOOL, "false", no_argument, 0, "",
      "Enable encryption of table, tablespace"),

  OPT(IGNORE_ERRORS, "retry-errors", STRING, "NONE", required_argument, 0, "",
      "Ignore MySQL errors. example --mysql-ignore-error=2013,1047"),

  OPT(FUNCTION_CONTAINS_DML, "function-contains-dml", STRING, "insert,update", required_argument, 0, "functions",
      "Function contains DML. It tells what type of  SQL FUNCTION CONTAINS"
      "--function-contains-dml=update,delete,insert. Even order matters. for "
      "example if it insert,update then it would first insert and then update "
      "or"),

  OPT(IGNORE_DML_CLAUSE, "ignore-dml-clause-prob", INT, "10", required_argument, 0, "",
      "Adding Ignore clause to update delete and insert "),

  OPT(WAIT_FOR_SYNC, "wait-for-sync", BOOL, "0", no_argument, 0, "",
      "Sleep after create table"),

  OPT(RANDOM_COLUMNS, "random-columns", BOOL, "false", no_argument, 0, "",
      "use random number of column for table"),

  OPT(INDEXES_PROB, "index-prob", INT, "10", required_argument, 0, "",
      "Probability a table would have index"),

  OPT(POSITIVE_INT_PROB, "positive-prob", INT, "70", required_argument, 0, "",
      "Probablity of using positive interger for random value in a int column"),

  OPT(COMPOSITE_KEY_PROB, "composite-key-prob", INT, "1", required_argument, 0, "",
      "Probability that a column is added to the primary key beyond the "
      "enforcing one (ipkey or vpkey), making the key composite. The "
      "roll is per candidate column, not per table, so the share of "
      "tables that end up composite is 1-(1-p)^candidates. Candidates "
      "exclude the enforcing key, partition columns, BLOB/JSON/TEXT and "
      "any column whose null_val is set, which in practice leaves about "
      "a third of the columns -- so 5 yields roughly one table in 7 "
      "with a 2-column key, not one in three. On ClickHouse the primary "
      "key is also the ORDER BY, so this is what produces a "
      "multi-column ORDER BY; it defaults to 5 there and 1 elsewhere. "
      "0 turns it off."),

  OPT(CH_VERIFY_INTERVAL, "ch-verify-interval", INT, "0", required_argument, 0, "",
      "ClickHouse replica verification interval in seconds (0=disabled, "
      "only at end). When > 0, a background thread pauses all worker "
      "threads and checks count+checksum on all replicas every N seconds."),

  OPT(CALL_FUNCTION, "call-function-prob", INT, "10", required_argument, OPT_SQL | OPT_DDL, "Function",
      "Probability of calling function "),

  /* todo set default to all */
  OPT(ENCRYPTION_TYPE, "encryption-type", STRING, "oracle", required_argument, 0, "",
      "all ==> keyring/Y/N \n oracle ==> Y/N \n x ==> x \n if some string "
      "other than all/oracle is given it would use it as encryption type"),

  /* create,alter,drop undo tablespace */
  OPT(UNDO_SQL, "undo-tbs-sql", INT, "1", required_argument, OPT_SQL | OPT_DDL, "UndoTablespace",
      "Assign probability of running create/alter/drop undo tablespace"),

  /* Add new table */
  OPT(ADD_NEW_TABLE, "add-table", INT, "1", required_argument, OPT_SQL | OPT_DDL, "AddTable",
      "Add new table"),

  OPT(SINGLE_THREAD_DDL, "single-thread-ddl", BOOL, "false", no_argument, 0, "",
      "Single thread DDL. Execute all ddl by a single thread"),

  OPT(THREAD_DOING_ONLY_SELECT, "select-threads", INT, "0", required_argument, 0, "",
      "Number of threads doing only select. "),

  /* disable virtual columns*/
  OPT(NO_VIRTUAL_COLUMNS, "no-virtual", BOOL, "false", no_argument, 0, "",
      "Disable virtual columns"),

  /* each thread to work on single table */
  OPT(THREAD_PER_TABLE, "thread-per-table", BOOL, "false", no_argument, 0, "",
      "Each thread to work on single table . If there is 10 threads "
      "and 30 tables then pstress will work on on 10 tables and each "
      "thread will work on one table"),

  /* disable blob columns*/
  OPT(NO_BLOB, "no-blob", BOOL, "false", no_argument, 0, "",
      "Disable blob columns"),

  /* disable json columns */
  OPT(NO_JSON, "no-json", BOOL, "false", no_argument, 0, "",
      "Disable json columns"),

  /* use session random timezone */
  OPT(RANDOM_TIMEZONE, "timezone-session", INT, "0", required_argument, OPT_SQL, "Timezone",
      "Use random timezone for each session"),

  /* disable all type of encrytion */
  OPT(NO_TABLESPACE, "no-tbs", BOOL, "false", no_argument, 0, "",
      "disable all type of tablespace including the general tablespace"),

  /* call SET SESSION wsrep_osu_method=NBO  befor drop table */
  OPT(DROP_WITH_NBO, "drop-with-nbo-prob", INT, "0", required_argument, 0, "",
      "call SET SESSION wsrep_osu_method=NBO  befor drop table"),

  /* Initial Table */
  OPT(TABLES, "tables", INT, "10", required_argument, 0, "",
      "Number of initial tables"),

  /* Number of indexes in a table */
  OPT(INDEXES, "indexes", INT, "1", required_argument, 0, "",
      "Maximum indexes in a table,default depends on page-size as well"),

  /* Process option prob file */
  OPT(OPTION_PROB_FILE, "option-prob-file", STRING, "", required_argument, 0, "",
      " option prob file, see File should contain lines "
      "like\n 20:option1=on|off\n, means 20% chances that it would be "
      "processed. For each seed it would pick one option from the "),

  OPT(NO_TIMESTAMP, "no-timestamp", BOOL, "false", no_argument, 0, "",
      "Disable timestamp"),

  OPT(COMPARE_CASE_INSENSTIVE, "compare-ci", BOOL, "false", no_argument, 0, "",
      "for compare use case insensitive"),

  OPT(NO_DATE, "no-date", BOOL, "false", no_argument, 0, "",
      "Disable date"),

  OPT(NO_BIT, "no-bit", BOOL, "false", no_argument, 0, "",
      "Disable bit"),

  OPT(NO_DATETIME, "no-datetime", BOOL, "false", no_argument, 0, "",
      "Disable time"),

  OPT(COMPARE_RESULT, "compare-result", BOOL, "false", no_argument, 0, "",
      "Compare result of SELECT query with and without secondary ENGINE. User "
      "is resposible to ensure no change in data during the test. "),

  OPT(SECONDARY_ENGINE, "secondary-engine", STRING, "", required_argument, 0, "",
      "Use secondary engine for some tables"),

  /* algorithm for alter */
  OPT(ALGORITHM, "alter-algorithm", STRING, "all", required_argument, 0, "",
      "algorithm used in alter table.\n"
      "--alter-algorithm INPLACE,COPY,DEFAULT\n --alter-algorithm all "
      "means randomly one of them will be picked. Pass options comma "
      "seperated without space"),

  OPT(COLUMN_TYPES, "column-types", STRING, "all", required_argument, 0, "",
      "Column types to be used in the table. Pass options comma "
      "seperated without space example --column-type=int,date. It will "
      "create table with only int and date"),

  /* lock for alter */
  OPT(LOCK, "alter-lock", STRING, "all", required_argument, 0, "",
      "lock mechanism used in alter table.\n "
      "--alter-lock DEFAULT,NONE,SHARED,EXCLUSIVE\n --alter-lock all "
      "means randomly one of them will be picked. Pass options comma "
      "seperated without space"),

  /* Number of columns in a table */
  OPT(COLUMNS, "columns", INT, "10", required_argument, 0, "",
      "maximum columns in a table, default depends on page-size, "
      "branch. for 8.0 it is 7 for 5.7 it 10"),

  /* Number of columns in an index of a table */
  OPT(INDEX_COLUMNS, "index-columns", INT, "10", required_argument, 0, "",
      "maximum columns in an index of a table, default depends on "
      "page-size as well"),

  /* autoinc column */
  OPT(NO_AUTO_INC, "no-auto-inc", BOOL, "false", no_argument, 0, "",
      "Disable auto inc columns in table, including pkey"),

  /* desc index support */
  OPT(NO_DESC_INDEX, "no-desc-index", BOOL, "false", no_argument, 0, "",
      "Disable index with desc on tables "),

  /* unique index probability out of thousand */
  OPT(UNIQUE_INDEX_PROB_K, "unique-key-prob-k", INT, "1", required_argument, 0, "",
      "probability of creating unique index on table out of 1000. "
      "Currently only supported if a table have int column"),

  /* Only Partition tables */
  OPT(ONLY_PARTITION, "only-partition-tables", BOOL, "false", no_argument, 0, "",
      "Work only on Partition tables"),

  /* Only Temporary tables */
  OPT(ONLY_TEMPORARY, "only-temp-tables", BOOL, "false", no_argument, 0, "",
      "Work only on temporary tables"),

  /* No FK tables */
  OPT(NO_FK, "no-fk-tables", BOOL, "false", no_argument, 0, "",
      "do not work on foriegn tables"),

  /* No Partition tables */
  OPT(NO_PARTITION, "no-partition-tables", BOOL, "false", no_argument, 0, "",
      "do not work on partition tables"),

  /* NO Temporary tables */
  OPT(NO_TEMPORARY, "no-temp-tables", BOOL, "false", no_argument, 0, "",
      "do not work on temporary tables"),

  OPT(FK_PROB, "fk-prob", INT, "50", required_argument, 0, "",
      R"(
    Probability of each normal table having the FK. Currently, FKs are only linked
    to the primary key of the parent table. So, even with 100% probability, a table
    will have an FK only if its parent has a primary key.
  )"),

  OPT(PARTITION_PROB, "partition-prob", INT, "10", required_argument, 0, "",
      "Probability of partition tables"),

  /* Ratio of temporary table to normal table */
  OPT(TEMPORARY_PROB, "temporary-prob", INT, "10", required_argument, 0, "",
      "Probability of temporary tables"),

  /* Initial Records in table */
  OPT(INITIAL_RECORDS_IN_TABLE, "records", STRING, "1K", required_argument, 0, "",
      "Number of initial records (N) in each table. The table will have random "
      "records in range of 0 to N. Also check --random-initial-records. You "
      "can "
      "also pass 10K, 1M  "),

  OPT(SECOND_LEVEL_MERGE, "second-level-merge", INT, "50", required_argument, 0, "",
      "Probability of executing second level merges"),

  OPT(LOG_PK_BULK_INSERT, "log-bulk-insert-pk", BOOL, "false", no_argument, 0, "",
      "Log primary key of table used during bulk insert"),

  /* Initial Records in table */
  OPT(RANDOM_INITIAL_RECORDS, "random-initial-records", BOOL, "false", no_argument, 0, "",
      " When passed with --records (N) option "
      " rand(N) records are inserted"),

  /* Execute workload for number of seconds */
  OPT(NUMBER_OF_SECONDS_WORKLOAD, "seconds", INT, "1000", required_argument, 0, "",
      "Number of seconds to execute workload"),

  /* number of queries N last queries */
  OPT(N_LAST_QUERIES, "log-last-queries", INT, "10", required_argument, 0, "",
      "Number of N queries logged in logdir /*sql last queries. F "
      "means query was FAIL and S mean it was success"),

  /* primary key probability */
  OPT(PRIMARY_KEY, "pk-prob", INT, "95", required_argument, 0, "",
      "Probability of adding primary key in a table"),

  /*Encrypt table */
  OPT(ALTER_TABLE_ENCRYPTION, "alter-table-encrypt", INT, "10", required_argument, OPT_SQL | OPT_DDL, "AlterEncrypt",
      "Alter table set Encryption"),

  OPT(BULK_INSERT_WIDTH, "bulk-insert-width", INT, "16777216", required_argument, 0, "",
      "The maximum width of each insert in bulk insert default is 16777216"),

  /* ENFORCE REWRITE  */
  OPT(ENFORCE_MERGE, "enforce-merge-prob", INT, "1", required_argument, OPT_SQL | OPT_DDL, "Rewrite",
      "ENFORCE MERGE in secondary by calling PRAGMA rewrite. It calls "
      "rewrite with partial merge"),

  OPT(REWRITE_ROW_GROUP_MIN_ROWS, "enforce-row-group-min-rows-prob", INT, "60", required_argument, 0, "",
      "row_group_min_rows prob in REWRITE TABLE"),

  OPT(REWRITE_ROW_GROUP_MAX_BYTES, "enfroce-row-group-max-bytes-prob", INT, "60", required_argument, 0, "",
      "row_group_max_bytes prob in REWRITE TABLE"),

  OPT(REWRITE_ROW_GROUP_MAX_ROWS, "enforce-row-group-max-rows-prob", INT, "60", required_argument, 0, "",
      "row_group_max_rows prob in REWRITE TABLE"),

  OPT(REWRITE_DELTA_NUM_ROWS, "enforce-delta-num-rows-prob", INT, "60", required_argument, 0, "",
      "delta_num_rows prob in REWRITE TABLE"),

  OPT(REWRITE_DELTA_NUM_UNDO, "enforce-delta-num-undo-prob", INT, "60", required_argument, 0, "",
      "delta_num_undo prob in REWRITE TABLE"),

  OPT(REWRITE_GC, "enforce-gc-prob", INT, "60", required_argument, 0, "",
      "gc prob in REWRITE TABLE"),

  OPT(REWRITE_BLOCKING, "enforce-blocking-prob", INT, "60", required_argument, 0, "",
      "blocking prob in REWRITE TABLE"),

  OPT(REWRITE_MAX_ROW_ID_HASH_MAP, "enforce-max-row-id-hash-map-prob", INT, "60", required_argument, 0, "",
      "max_row_id_hash_map prob in REWRITE TABLE"),

  OPT(REWRITE_FORCE, "enforce-force-prob", INT, "60", required_argument, 0, "",
      "force prob in REWRITE TABLE"),

  OPT(REWRITE_NO_RESIDUAL, "enforce-no-residual-prob", INT, "60", required_argument, 0, "",
      "no_residual prob in REWRITE TABLE"),

  OPT(REWRITE_MAX_INTERNAL_BLOB_SIZE, "enforce-max-internal-blob-size-prob", INT, "60", required_argument, 0, "",
      "max_internal_blob_size prob in REWRITE TABLE"),

  OPT(REWRITE_BLOCK_COOKER_ROW_GROUP_MAX_ROWS, "enforce-block-cooker-row-group-max-rows-prob", INT, "60", required_argument, 0, "",
      "block_cooker_row_group_max_rows prob in REWRITE TABLE"),

  OPT(REWRITE_PARTIAL, "enforce-partial-prob", INT, "60", required_argument, 0, "",
      "partial prob in REWRITE TABLE"),

  /* secondary GARBAGE COLLECT */
  OPT(SECONDARY_GC, "secondary-gc", INT, "1", required_argument, OPT_SQL | OPT_DDL, "GC",
      "Execute garbage collect in secondary"),

  /* probability of null value in a column */
  OPT(NULL_PROB, "null-prob", INT, "0", required_argument, 0, "",
      "Probability that a column would have null value. It is used for "
      "insert, update, delete and also in where clause"),

  OPT(UNIQUE_RANGE, "range", FLOAT, "100.0", required_argument, 0, "",
      "Sets the range for random number generation (integers, floats, "
      "doubles). "
      "A larger range increases randomness. For example, with 100 "
      "records and a range of 100, "
      "pstress generates values between -10,000 and +10,000 for int, "
      "float, and decimal columns. "
      "If the range is 0.1, values are generated between -10 and +10."),

  /* dictionary file */
  OPT(DICTIONARY_FILE, "dictionary-file", STRING, "english_dictionary.txt", required_argument, 0, "",
      "Dictionary file for random string"),

  /* table name prefix */
  OPT(TABLE_PREFIX, "table-prefix", STRING, "tt_", required_argument, 0, "",
      "Prefix of the generated table names, followed by the table "
      "number: tt_1, tt_2, ... Give two runs that share a database "
      "different prefixes so neither drops the other's tables."),

  /* total number of queries */
  OPT(TOTAL_QUERIES, "total-queries", INT, "0", required_argument, 0, "",
      "Total number of queries to be executed"),

  /* maximum width of varchar column */
  OPT(VARCHAR_COLUMN_MAX_WIDTH, "varchar-column-max-width", INT, "30", required_argument, 0, "",
      "Maximum width of varchar column"),

  /* disable text columns*/
  OPT(NO_TEXT, "no-text", BOOL, "false", no_argument, 0, "",
      "Disable text columns"),

  /* probability of creating not-secondary columns out of hundred */
  OPT(NOT_SECONDARY, "column-skip-to-secondary", INT, "0", required_argument, 0, "",
      "Probability of creating not secondary columns"),

  /* probability of modifying columns with/without NOT SECONDARY clause" */
  OPT(MODIFY_COLUMN_SECONDARY_ENGINE, "alter-column-secondary-engine", INT, "1", required_argument, OPT_SQL | OPT_DDL, "AlterColumnSecondary",
      "Probability of modifying existing column with NOT SECONDARY clause"),

  /* probability of modifying columns using primary key in where clause" */
  OPT(USING_PK_PROB, "using-pkey", INT, "90", required_argument, 0, "",
      "Probability of using pk column in where clause. if table does "
      "not pk column then first column of index. and if it does not "
      "have index then any random column"),

  OPT(PKEY_IN_SET, "pk-in-set", INT, "0", required_argument, 0, "",
      "USE PKEY IN SET tt_N set col="),

  /* disable char columns*/
  OPT(NO_CHAR, "no-char", BOOL, "false", no_argument, 0, "",
      "Disable char columns"),

  /* disable VARCHAR columns*/
  OPT(NO_VARCHAR, "no-varchar", BOOL, "false", no_argument, 0, "",
      "Disable varchar columns"),

  /* disable DEICMAL columns*/
  OPT(NO_DECIMAL, "no-decimal", BOOL, "false", no_argument, 0, "",
      "Disable decimal columns"),

  /* disable FLOAT columns*/
  OPT(NO_FLOAT, "no-float", BOOL, "false", no_argument, 0, "",
      "Disable float columns"),

  /* disable double columns*/
  OPT(NO_DOUBLE, "no-double", BOOL, "false", no_argument, 0, "",
      "Disable double columns"),

  /* disable bool columns*/
  OPT(NO_BOOL, "no-bool", BOOL, "false", no_argument, 0, "",
      "Disable bool columns"),

  /* disable integer columns*/
  OPT(NO_INTEGER, "no-integer", BOOL, "false", no_argument, 0, "",
      "Disable integer columns"),

  /* diable enum columns*/
  OPT(NO_ENUM, "no-enum", BOOL, "false", no_argument, 0, "",
      "Disable enum columns"),

  /* disable int columns*/
  OPT(NO_INT, "no-int", BOOL, "false", no_argument, 0, "",
      "Disable int  columns"),

  /* modify column */
  OPT(ALTER_COLUMN_MODIFY, "modify-column", INT, "10", required_argument, OPT_SQL | OPT_DDL, "ModifyColumn",
      "Alter table column modify"),

  /*compress table */
  OPT(ALTER_TABLE_COMPRESSION, "alter-table-compress", INT, "10", required_argument, OPT_SQL | OPT_DDL, "AlterCompress",
      "Alter table compression"),

  /* Row Format */
  OPT(ROW_FORMAT, "row-format", STRING, "none", required_argument, 0, "",
      "create table row format. it is the row format of table. a "
      "table can have compressed, dynamic, redundant row format.\n "
      "valid values are :\n all: use compressed, dynamic, redundant. all "
      "combination key block size will be used. \n uncompressed: do not use "
      "compressed row_format, i.e. key block size will not used. \n"
      "none: do not use any encryption"),

  /* MySQL server option */
  OPT(MYSQLD_SERVER_OPTION, "mso", STRING, "", required_argument, 0, "",
      "mysqld server options variables which are set during the load, see "
      "--set-variable. n:option=v1=v2 where n is probabality of picking "
      "option, v1 and v2 different value that is supported. "
      "for e.g. --md=20:innodb_temp_tablespace_encrypt=on=off"),

  OPT(SERVER_OPTION_FILE, "sof", STRING, "", required_argument, 0, "",
      "server options file, MySQL server options file, picks some of "
      "the mysqld options, "
      "and try to set them during the load , using set global and set "
      "session.\n see --set-variable.\n File should contain lines like\n "
      "20:innodb_temp_tablespace_encrypt=on=off\n, means 20% chances "
      "that it would be processed. "),

  /* Set Global */
  OPT(SET_GLOBAL_VARIABLE, "set-variable", INT, "3", required_argument, OPT_SQL | OPT_DDL, "SetVariable",
      "set mysqld variable during the load.(session|global)"),

  /* alter instance disable/enable redo logging */
  OPT(ALTER_REDO_LOGGING, "alter-redo-log", INT, "0", required_argument, OPT_SQL | OPT_DDL, "AlterRedo",
      "Alter instance enable/disable redo log"),

  /* alter instance rotate innodb master key */
  OPT(ALTER_MASTER_KEY, "rotate-master-key", INT, "1", required_argument, OPT_SQL | OPT_DDL, "RotateMaster",
      "Alter instance rotate innodb master key"),

  OPT(ALTER_SECONDARY_ENGINE, "alter-secondary-engine", INT, "1", required_argument, OPT_SQL | OPT_DDL, "AlterSecondary",
      "run secondary-engine sql"),

  /* alter instance rotate innodb system key */
  OPT(ALTER_ENCRYPTION_KEY, "rotate-encryption-key", INT, "1", required_argument, OPT_SQL | OPT_DDL, "AlterInstanceSystemKey",
      "Alter instance rotate innodb system key X"),

  /* alter instance rotate gcache master key */
  OPT(ALTER_GCACHE_MASTER_KEY, "rotate-gcache-key", INT, "1", required_argument, OPT_SQL | OPT_DDL, "AlterInstanceGcache",
      "Alter instance rotate gcache master key"),

  /* Reload keyring component configuration */
  OPT(ALTER_INSTANCE_RELOAD_KEYRING, "reload-keyring", INT, "1", required_argument, OPT_SQL | OPT_DDL, "AlterInstanceReloadKey",
      "Alter instance reload keyring"),

  /* rotate redo log key */
  OPT(ROTATE_REDO_LOG_KEY, "rotate-redo-log-key", INT, "1", required_argument, OPT_SQL | OPT_DDL, "RotateRedoLog",
      "Rotate redo log key"),

  /*Tablespace Encrytion */
  OPT(ALTER_TABLESPACE_ENCRYPTION, "alt-tbs-enc", INT, "1", required_argument, OPT_SQL | OPT_DDL, "AlterTablespaceEncrypt",
      "Alter tablespace set Encryption including the mysql tablespace"),

  /*Discard tablespace */
  OPT(ALTER_DISCARD_TABLESPACE, "alt-discard-tbs", INT, "1", required_argument, OPT_SQL | OPT_DDL, "AlterDiscardTablespace",
      "Alter table to discard file-per-tablespace"),

  /*Database Encryption */
  OPT(ALTER_DATABASE_ENCRYPTION, "alt-db-enc", INT, "1", required_argument, OPT_SQL | OPT_DDL, "AlterDatabaseEncrypt",
      "Alter Database Encryption mode to Y/N"),

  /*Database collation */
  OPT(ALTER_DATABASE_COLLATION, "alt-db-col", INT, "1", required_argument, OPT_SQL | OPT_DDL, "AlterDatabaseCollation",
      "Alter Database collation probability"),

  /* Tablespace Rename */
  OPT(ALTER_TABLESPACE_RENAME, "alt-tbs-rename", INT, "1", required_argument, OPT_SQL | OPT_DDL, "AlterTablespaceRename",
      "Alter tablespace rename"),

  /* SELECT */
  OPT(NO_SELECT, "no-select", BOOL, "false", no_argument, 0, "",
      "do not execute any type select on tables"),

  /* Add no cascade to FK */
  OPT(NO_FK_CASCADE, "no-cascade", BOOL, "false", no_argument, 0, "",
      "Disable to cacading of FK in pstress"),

  /* INSERT */
  OPT(NO_INSERT, "no-insert", BOOL, "false", no_argument, 0, "",
      "do not execute insert into tables"),

  OPT(NO_REPLACE, "no-replace", BOOL, "false", no_argument, 0, "",
      "do not execute REPLACE into tables"),

  /* UPDATE */
  OPT(NO_UPDATE, "no-update", BOOL, "false", no_argument, 0, "",
      "do not execute any type of update on tables"),

  /* DELETE */
  OPT(NO_DELETE, "no-delete", BOOL, "false", no_argument, 0, "",
      "do not execute any type of delete on tables"),

  /* PREPARE new metadata */
  OPT(PREPARE, "prepare", BOOL, "false", no_argument, 0, "",
      "create new random tables and insert initial records"),

  OPT(SELECT_ROW_USING_PKEY, "select-precise", INT, "800", required_argument, OPT_SQL, "SP",
      "Select table using single row"),

  OPT(THROTTLE_SLEEP, "throttle", INT, "0", required_argument, OPT_SQL, "",
      "slowing the thread"),

  OPT(SELECT_FOR_UPDATE, "select-precise-update", INT, "8", required_argument, OPT_SQL, "SPU",
      "Select table using single for update"),

  OPT(SELECT_ALL_ROW, "select-bulk", INT, "8", required_argument, OPT_SQL, "SB",
      "select all table data and in case of partition randomly pick "
      "some partition"),

  OPT(ONLY_SELECT, "only-select", BOOL, "false", no_argument, 0, "",
      "Disable all other SQL"),

  OPT(SELECT_FOR_UPDATE_BULK, "select-bulk-update", INT, "20", required_argument, OPT_SQL, "SBU",
      "SELECT bulk for update "),

  OPT(PREPARED_STMT_PROB, "ps-prob", INT, "0", required_argument, OPT_SQL, "PS",
      "Probability of using cached MySQL C API prepared statements for "
      "eligible generated SELECT statements"),

  OPT(NON_INT_PK, "non-int-pk-prob", INT, "25", required_argument, 0, "",
      "Probability of primary key column being non integer"),

  OPT(PK_COLUMN_AUTOINC, "pk-auto-inc-prob", INT, "10", required_argument, 0, "",
      "Probability of primary key column being auto increment"),

  /* Sequential primary keys. Deliberately left at the default
     required_argument: a no_argument BOOL can only ever be set to true, and
     this one has to be switchable off on ClickHouse, where it defaults on. */
  OPT(SEQ_PK, "seq-pk", BOOL, "false", required_argument, 0, "SeqPK",
      "INSERT takes primary key values from a per-table increasing "
      "counter instead of random ones, and UPDATE/DELETE/SELECT "
      "predicates aim inside the range of keys handed out so far. On "
      "ClickHouse the primary key is the table's ORDER BY prefix, so "
      "this makes each new part cover a narrow, increasing key range "
      "the way real append-only ingest does, instead of spanning the "
      "whole key space. Defaults ON for ClickHouse, OFF elsewhere; "
      "pass --seq-pk=false to turn it off."),

  OPT(SEQ_PK_RECENT_PROB, "seq-pk-recent-prob", INT, "30", required_argument, 0, "SeqPKRecent",
      "With --seq-pk, probability that a predicate aims only at the "
      "newest slice of the key range (see --seq-pk-recent-rows) "
      "instead of the whole range. The newest keys are the ones still "
      "in small unmerged parts, so this is where the interesting "
      "ClickHouse behaviour is."),

  OPT(SEQ_PK_RECENT_ROWS, "seq-pk-recent-rows", INT, "10000", required_argument, 0, "SeqPKRecentRows",
      "Width, in keys, of the newest slice --seq-pk-recent-prob aims "
      "at"),

  OPT(SEQ_PK_DUP_PROB, "seq-pk-dup-prob", INT, "5", required_argument, 0, "SeqPKDup",
      "With --seq-pk, probability that an inserted row reuses a key "
      "already handed out instead of taking a new one. Unique keys "
      "would otherwise retire the ReplacingMergeTree dedup path "
      "entirely, since colliding random keys are the only reason a "
      "row is ever collapsed today. A small rate keeps dedup "
      "exercised while leaving the append-only shape dominant."),

  OPT(INSERT_BULK_COUNT, "insert-bulk-count", INT, "1000", required_argument, 0, "",
      "Number of rows to insert in a single insert statement"),

  OPT(INSERT_BULK, "insert-bulk", INT, "1", required_argument, OPT_SQL, "IB",
      "insert random row"),

  OPT(INSERT_RANDOM_ROW, "insert", INT, "600", required_argument, OPT_SQL, "I",
      "insert random row"),

  /* Update row using pkey */
  OPT(UPDATE_ROW_USING_PKEY, "update-precise", INT, "200", required_argument, OPT_SQL, "UP",
      "Update using where clause"),

  OPT(REPLACE_ROW, "replace-row", INT, "10", required_argument, OPT_SQL, "Replace INTO ",
      "execute REPLACE INTO TABLE "),

  OPT(UPDATE_ALL_ROWS, "update-bulk", INT, "10", required_argument, OPT_SQL, "UB",
      "Update bulk of a table"),

  /* Delete row using pkey */
  OPT(DELETE_ROW_USING_PKEY, "delete-precise", INT, "100", required_argument, OPT_SQL, "DP",
      "delete where condition"),

  /* Delete all rows */
  OPT(DELETE_ALL_ROW, "delete-bulk", INT, "8", required_argument, OPT_SQL, "DB",
      "delete bulk rows of table"),

  /* ClickHouse ALTER TABLE UPDATE mutation */
  OPT(CH_ALTER_UPDATE, "ch-alter-update", INT, "0", required_argument, OPT_SQL | OPT_DDL, "CHAlterUpdate",
      "ClickHouse ALTER TABLE t UPDATE col=val WHERE ... SETTINGS mutations_sync=2"),

  /* ClickHouse ALTER TABLE DELETE mutation */
  OPT(CH_ALTER_DELETE, "ch-alter-delete", INT, "0", required_argument, OPT_SQL | OPT_DDL, "CHAlterDelete",
      "ClickHouse ALTER TABLE t DELETE WHERE ... SETTINGS mutations_sync=2"),

  /* ClickHouse: which implementation a DELETE FROM asks for. The server
     default, lightweight_delete_mode = alter_update, rewrites every matched
     part as a heavyweight mutation; lightweight_update writes a patch part
     instead. Rolled per statement rather than set once, so a single run covers
     both code paths. */
  OPT(CH_LIGHTWEIGHT_DELETE, "ch-lightweight-delete", INT, "100", required_argument, 0, "CHLightweightDelete",
      "Probability that a DELETE FROM appends SETTINGS lightweight_delete_mode "
      "= 'lightweight_update', so the delete writes a patch part inline "
      "instead of scheduling a heavyweight mutation. The mutation path is "
      "gated by number_of_free_entries_in_pool_to_execute_mutation, so a "
      "merge-saturated pool can leave the statement waiting minutes under "
      "lightweight_deletes_sync = 2; the patch-part path never touches that "
      "pool. Not a workload action of its own -- it only modifies the DELETE "
      "FROM that --delete-precise/--delete-bulk already roll. Lowering this "
      "below 100 also puts mutations back in reach of --ch-kill-mutation, "
      "which cannot target a patch part. Mode 'lightweight_update' falls back "
      "to alter_update when a patch part is impossible, so this never fails a "
      "statement on its own."),

  OPT(INSERT_COLUMN_SUBSET_PROB, "insert-column-subset-prob", INT, "10", required_argument, 0, "InsertColSubset",
      "Probability that a runtime INSERT names only a random subset of the "
      "table's columns, so the server fills the rest with their defaults. Key "
      "columns are always written, and so is any NOT NULL column without a "
      "default outside ClickHouse, where omitting it would fail the statement. "
      "The initial load always writes every column."),

  OPT(NO_VERSION_COLUMN, "no-version-column", BOOL, "false", no_argument, 0, "",
      "ClickHouse: do not add the _pstress_ver UInt64 column to tables or "
      "write it on INSERT. ReplacingMergeTree is then created without a "
      "version argument, so of the rows sharing a key the one a merge keeps "
      "is the last it reads rather than the newest by version."),

  OPT(SEQ_PK_TRUST_METADATA, "seq-pk-trust-metadata", BOOL, "false", no_argument, 0, "",
      "With --seq-pk, at step >= 2 take each table's next key from the step "
      "file as written instead of raising it to SELECT max(pk) from the "
      "server. Skips one full-column scan per table at startup, which is slow "
      "with many tables or many parts. Only safe when the previous step wrote "
      "its step file: if that step died, keys already in the table get "
      "reissued and, under ReplacingMergeTree, silently replace older rows."),

  OPT(CH_MUTATIONS_SYNC, "ch-mutations-sync", BOOL, "false", no_argument, 0, "CHMutationsSync",
      "Append SETTINGS mutations_sync=2 to ClickHouse ALTER mutations "
      "(ADD/DROP COLUMN, ALTER UPDATE/DELETE). Pass flag to enable."),

  /* ClickHouse KILL MUTATION. A mutation is a background part rewrite, so
     cancelling one mid-flight leaves the table half-mutated and any ALTER that
     was waiting on it (--ch-mutations-sync) failing — both states the server
     has to survive. */
  OPT(CH_KILL_MUTATION, "ch-kill-mutation", INT, "0", required_argument, OPT_SQL | OPT_DDL, "CHKillMutation",
      "Probability weight for KILL MUTATION: pick a random unfinished "
      "mutation of this run's database out of system.mutations and "
      "cancel it. Rolls blind and does nothing when there is no "
      "unfinished mutation."),

  OPT(CH_ADD_COLUMN_BACKFILL, "ch-add-column-backfill", BOOL, "false", no_argument, 0, "CHBackfill",
      "After ADD COLUMN succeeds, fire an ALTER TABLE UPDATE to "
      "backfill existing rows with a random server-side value "
      "instead of leaving them at the ClickHouse zero-default."),

  /* ClickHouse materialized views. --create-mv fires CREATE MATERIALIZED VIEW
     ... POPULATE against a random table while the load keeps inserting into it,
     which is the race the atomicity of POPULATE has to survive. */
  OPT(CH_CREATE_MV, "create-mv", INT, "1", required_argument, OPT_SQL | OPT_DDL, "CreateMV",
      "Create a ClickHouse materialized view with POPULATE over a "
      "random table while the workload runs. The view mirrors the "
      "table, so once inserts stop it must hold exactly the table's "
      "rows; --verify-mv checks that at the end of the run. Atomic "
      "POPULATE is a per-server guarantee, so against a multi-replica "
      "ClickHouse Cloud endpoint the check reports a false MISMATCH: "
      "pass --create-mv=0 there, or use a single-replica service."),

  OPT(CH_DROP_MV, "drop-mv", INT, "1", required_argument, OPT_SQL | OPT_DDL, "DropMV",
      "Drop a random existing materialized view. With "
      "--verify-mv-before-drop (the default) the view is checked "
      "against its table first, which pauses all workers for the "
      "duration of the check, so keep this probability low."),

  OPT(CH_MAX_MV_PER_TABLE, "max-mv-per-table", INT, "2", required_argument, 0, "",
      "Maximum number of materialized views on a single table. Every "
      "view adds work to every insert into that table."),

  OPT(CH_MV_SNAPSHOT_SLEEP_MS, "mv-snapshot-sleep-ms", INT, "1000", required_argument, 0, "",
      "Appended as SETTINGS merge_tree_storage_snapshot_sleep_ms=N to "
      "CREATE MATERIALIZED VIEW ... POPULATE, widening the window in "
      "which the source snapshot is taken so concurrent inserts "
      "actually collide with it. 0 disables. If a run with "
      "--mv-populate-atomically=off reports no mismatch at all, this "
      "is the first thing to raise."),

  OPT(CH_MV_POPULATE_ATOMICALLY, "mv-populate-atomically", STRING, "on", required_argument, 0, "",
      "on|off|random. Appended as SETTINGS "
      "materialized_views_populate_atomically=1/0 to each CREATE "
      "MATERIALIZED VIEW, and recorded per view. off forces the legacy "
      "non-atomic population, which is expected to lose or duplicate "
      "rows and so acts as a negative control proving the check "
      "actually detects the bug; mismatches on off views are reported "
      "but never fail the run. random rolls per view."),

  OPT(CH_MV_TO_PROB, "mv-to-prob", INT, "30", required_argument, 0, "",
      "Percent chance that a materialized view writes into a freshly "
      "created TO target table instead of its own inner table. "
      "POPULATE together with TO was a syntax error before ClickHouse "
      "PR 108715."),

  OPT(CH_VERIFY_MV_BEFORE_DROP, "verify-mv-before-drop", BOOL, "true", required_argument, 0, "",
      "Check a materialized view against its table before --drop-mv "
      "drops it, so every view that ever existed is checked instead of "
      "only the ones alive at the end of the run. Pauses all workers "
      "for the check. Pass off for stall free create/drop churn."),

  OPT(CH_SKIP_SCHEMA_VERIFY, "skip-schema-verify", BOOL, "false", no_argument, 0, "",
      "Skip comparing pstress metadata against the server's columns, "
      "both at startup (where a mismatch aborts the run) and at the "
      "end of the run. Saves a system.columns read per table, which "
      "adds up with many tables."),

  OPT(CH_VERIFY_MV, "verify-mv", BOOL, "false", no_argument, 0, "",
      "At the end of the run, compare every materialized view against "
      "its source table and fail the run on a mismatch."),

  /* ClickHouse projections. PR 113343 added ALTER TABLE ... MODIFY PROJECTION,
     a metadata only alter whose new settings apply lazily through later inserts
     and merges. Exercising it needs a projection to exist first, so --add- and
     --drop-projection are here too. */
  OPT(CH_ADD_PROJECTION, "add-projection", INT, "2", required_argument, OPT_SQL | OPT_DDL, "AddProjection",
      "ALTER TABLE t ADD PROJECTION p (SELECT ... ORDER BY ...) WITH "
      "SETTINGS (index_granularity = N) on a random table. Up to 5 per "
      "table. Also switches on the MergeTree settings a projection "
      "needs: without deduplicate_merge_projection_mode = rebuild a "
      "ReplacingMergeTree refuses projections outright, and without "
      "lightweight_mutation_projection_mode = rebuild a lightweight "
      "DELETE on a projected table fails."),

  OPT(CH_DROP_PROJECTION, "drop-projection", INT, "1", required_argument, OPT_SQL | OPT_DDL, "DropProjection",
      "ALTER TABLE t DROP PROJECTION p. Keep this below "
      "--modify-projection or most MODIFYs will find their target "
      "already gone."),

  OPT(CH_MODIFY_PROJECTION, "modify-projection", INT, "2", required_argument, OPT_SQL | OPT_DDL, "ModifyProjection",
      "ALTER TABLE t MODIFY PROJECTION p (<the same body>) WITH "
      "SETTINGS (index_granularity = N) — ClickHouse PR 113343. "
      "Restates the stored definition verbatim and changes only the "
      "settings, which is the one thing the statement allows. Needs "
      "--add-projection to have something to act on."),

  OPT(CH_MATERIALIZE_PROJECTION, "materialize-projection", INT, "1", required_argument, OPT_SQL | OPT_DDL, "MaterializeProjection",
      "ALTER TABLE t MATERIALIZE PROJECTION p — rebuilds the "
      "projection on parts that do not have it yet. Unlike MODIFY this "
      "is a mutation, so it honours --ch-mutations-sync and can be "
      "cancelled mid-flight by --ch-kill-mutation."),

  /* ClickHouse text indexes. A column takes at most one text index, and only a
     String (VARCHAR, CHAR, TEXT, BLOB) column can take one. */
  OPT(CH_TEXT_INDEX_PROB, "text-index-prob", INT, "0", required_argument, 0, "",
      "Probability out of 100 that a String column gets a text index "
      "in CREATE TABLE, with a random tokenizer and random optional "
      "parameters. Use with --text-words so the values have more than "
      "one token."),

  OPT(CH_TEXT_INDEX_PREPROCESSOR_PROB, "text-index-preprocessor-prob", INT, "0", required_argument, 0, "",
      "Probability out of 100 that a text index gets preprocessor = "
      "lower(col). The preprocessor is only applied on the index path, "
      "so hasToken and friends legitimately return different rows with "
      "use_skip_indexes = 0. Keep it at 0 when comparing against "
      "use_skip_indexes = 0; it is fine against "
      "query_plan_direct_read_from_text_index = 0."),

  OPT(CH_TEXT_INDEX_PHRASE_SEARCH_PROB, "text-index-phrase-search-prob", INT, "0", required_argument, 0, "",
      "Probability out of 100 that a text index gets "
      "support_phrase_search = 1. Any value above 0 also sets the "
      "MergeTree setting allow_experimental_text_index_phrase_search "
      "= 1 on every table, which the argument requires."),

  OPT(CH_ADD_TEXT_INDEX, "add-text-index", INT, "0", required_argument, OPT_SQL | OPT_DDL, "AddTextIndex",
      "ALTER TABLE t ADD INDEX ... TYPE text(...) on a String column "
      "that has no text index yet. Existing parts stay unindexed until "
      "--materialize-text-index reaches them."),

  OPT(CH_DROP_TEXT_INDEX, "drop-text-index", INT, "0", required_argument, OPT_SQL | OPT_DDL, "DropTextIndex",
      "ALTER TABLE t DROP INDEX on one of the table's text indexes."),

  OPT(CH_MATERIALIZE_TEXT_INDEX, "materialize-text-index", INT, "0", required_argument, OPT_SQL | OPT_DDL, "MaterializeTextIndex",
      "ALTER TABLE t MATERIALIZE INDEX on one of the table's text "
      "indexes, or a quarter of the time CLEAR INDEX, which removes "
      "the index from every part and leaves the table with a mix of "
      "indexed and unindexed parts again. Both are mutations and honour "
      "--ch-mutations-sync."),

  /* ClickHouse skipping indexes on int columns: minmax, set(N) or
     bloom_filter(p). They go on the first int column that is not the primary
     key, one per column, and the primary key is left alone because the ORDER
     BY already indexes it. An index can only change how many granules are
     read, never which rows come back, so compare against use_skip_indexes = 0
     with --compare-result-with-setting. */
  OPT(CH_INT_INDEX_PROB, "int-index-prob", INT, "50", required_argument, 0, "",
      "Probability out of 100 that CREATE TABLE gives the table's "
      "first non primary key int column a skipping index of a random "
      "type: minmax, set(N) or bloom_filter(p), with a random "
      "GRANULARITY."),

  OPT(CH_ADD_INT_INDEX, "add-int-index", INT, "2", required_argument, OPT_SQL | OPT_DDL, "AddIntIndex",
      "ALTER TABLE t ADD INDEX ... TYPE minmax|set|bloom_filter on the "
      "first int column that has no such index yet. Existing parts "
      "stay unindexed until --materialize-int-index reaches them."),

  OPT(CH_DROP_INT_INDEX, "drop-int-index", INT, "1", required_argument, OPT_SQL | OPT_DDL, "DropIntIndex",
      "ALTER TABLE t DROP INDEX on one of the table's int indexes."),

  OPT(CH_MATERIALIZE_INT_INDEX, "materialize-int-index", INT, "1", required_argument, OPT_SQL | OPT_DDL, "MaterializeIntIndex",
      "ALTER TABLE t MATERIALIZE INDEX on one of the table's int "
      "indexes, or a quarter of the time CLEAR INDEX. Both are "
      "mutations and honour --ch-mutations-sync."),

  OPT(TEXT_WORDS, "text-words", INT, "0", required_argument, 0, "",
      "Generate String values as 1 to N dictionary words joined by "
      "random separators, with an occasional capitalized word, instead "
      "of the usual single word. 0 keeps the usual values. Ignores the "
      "VARCHAR width, which ClickHouse does not enforce."),

  /* ClickHouse table settings changed while the run is going. The pool file
     decides what may be altered: only its alter: lines are candidates, because
     a MergeTree setting can be read only after create and nothing in
     system.merge_tree_settings says which ones are. */
  OPT(CH_MODIFY_TABLE_SETTING, "modify-table-setting", INT, "0", required_argument, OPT_SQL | OPT_DDL, "ModifySetting",
      "ALTER TABLE t MODIFY SETTING <name> = <value> on a random "
      "table, with the name and the value taken from the alter: lines "
      "of --table-settings-file. The new value replaces the one the "
      "table was created with, so a later DROP/CREATE and the next "
      "step recreate the table with it. Needs at least one alter: "
      "line in the pool."),

  OPT(CH_RESET_TABLE_SETTING, "reset-table-setting", INT, "0", required_argument, OPT_SQL | OPT_DDL, "ResetSetting",
      "ALTER TABLE t RESET SETTING <name> — take away an override the "
      "table currently carries and go back to the server default. "
      "Only names from the alter: lines of --table-settings-file are "
      "reset, and only when the table has one set, so this does "
      "nothing until --modify-table-setting or a roll at CREATE TABLE "
      "has put one there."),

  /* Per-table ClickHouse MergeTree settings */
  OPT(CH_TABLE_SETTINGS_FILE, "table-settings-file", STRING, "clickhouse_table_settings.txt", required_argument, 0, "",
      "File with the pool of ClickHouse table settings, one per line as "
      "[session:|alter:][<prob>:]<name> = <v1>|<v2> or int:<lo>..<hi>. "
      "Each table rolls every line against its probability, so prob "
      "100 puts a setting on every table and prob 5 on about one in "
      "twenty. session: lines are applied as SET on each connection. "
      "alter: lines are also rolled onto CREATE TABLE and, on top of "
      "that, are the pool --modify-table-setting and "
      "--reset-table-setting pick from at run time. "
      "Looked up next to the pstress binary. Missing default file is "
      "not an error; a missing file named on the command line is."),

  OPT(CH_TABLE_SETTINGS, "table-settings", STRING, "", required_argument, 0, "",
      "Fixed ClickHouse table settings applied verbatim to every "
      "table, e.g. --table-settings=\"index_granularity = 4096, "
      "compress_marks = 1\". Nothing is randomized and the settings "
      "file is not read."),

  OPT(NO_TABLE_SETTINGS, "no-table-settings", BOOL, "false", no_argument, 0, "",
      "Do not add any table or session settings, ignoring both "
      "--table-settings-file and --table-settings. Only the settings "
      "pstress requires internally are emitted. Pass flag to enable."),

  /* Compare a grammar SQL run with and without a query setting */
  OPT(COMPARE_RESULT_WITH_SETTING, "compare-result-with-setting", BOOL, "false", no_argument, 0, "",
      "Run every grammar SQL twice, once as it is and once with "
      "SETTINGS <--run-query-setting> appended, and compare the two "
      "result sets. Aborts on a mismatch, dumping both sets to "
      "default_result.csv and with_setting_result.csv in the log dir. "
      "Requires --run-query-setting. The comparison is order "
      "sensitive and takes no locks, so the workload has to be "
      "deterministic: use a single thread or an insert only workload, "
      "and give every grammar line an ORDER BY. Pass flag to enable."),

  OPT(RUN_QUERY_SETTING, "run-query-setting", STRING, "", required_argument, 0, "",
      "Setting clause appended as SETTINGS <clause> to the second run "
      "of each query compared by --compare-result-with-setting, e.g. "
      "--run-query-setting=\"join_algorithm='grace_hash'\". "
      "Comma separate to set more than one. Only use settings that "
      "are not supposed to change query results."),

  /* TLS for the ClickHouse native protocol (required by ClickHouse Cloud) */
  OPT(SECURE, "secure", BOOL, "false", no_argument, 0, "",
      "Connect over TLS using the ClickHouse native protocol. "
      "Required for ClickHouse Cloud. Changes the default port from "
      "9000 to 9440."),

  OPT(CH_SOCKET_TIMEOUT, "ch-socket-timeout", INT, "600", required_argument, 0, "",
      "Seconds a worker waits for a reply on an established connection "
      "before giving up on it (SO_RCVTIMEO/SO_SNDTIMEO). Only catches a "
      "server that holds the connection open and stops answering — a "
      "dropped connection is caught by TCP keepalive instead. Must stay "
      "clear of legitimately slow queries: a synchronous mutation on a "
      "busy service can take 20+ minutes. 0 waits forever, which is how "
      "a finished run can hang until it is killed."),

  OPT(CH_GRAMMAR_QUERY_SETTINGS, "ch-grammar-query-settings", STRING, "max_execution_time=120", required_argument, 0, "",
      "Setting clause appended as SETTINGS <clause> to every grammar "
      "SQL query (--grammar-sql). The grammar joins are unbounded and "
      "a join key that is mostly one value (0 from ADD COLUMN or a "
      "column left out of INSERT) turns a self-join into billions of "
      "rows, so the default cuts such a query off instead of letting "
      "it pin the server for hours. Comma separate to set more than "
      "one, e.g. \"max_execution_time=60, max_rows_in_join=100000000\". "
      "Merged into a SETTINGS clause the grammar line already has. "
      "Empty string sends the queries as written."),

  /* Drop column */
  OPT(DROP_COLUMN, "drop-column", INT, "1", required_argument, OPT_SQL | OPT_DDL, "DropColumn",
      "alter table drop some random column"),

  /* Add column */
  OPT(ADD_COLUMN, "add-column", INT, "1", required_argument, OPT_SQL | OPT_DDL, "AddColumn",
      "alter table add some random column"),

  /* Drop index */
  OPT(DROP_INDEX, "drop-index", INT, "1", required_argument, OPT_SQL | OPT_DDL, "DropIndex",
      "alter table drop random index"),

  /* Add column */
  OPT(ADD_INDEX, "add-index", INT, "1", required_argument, OPT_SQL | OPT_DDL, "AddIndex",
      "alter table add random index"),

  /* Rename Index */
  OPT(RENAME_INDEX, "rename-index", INT, "1", required_argument, OPT_SQL | OPT_DDL, "RenameIndex",
      "alter table rename index"),

  /* Rename Column */
  OPT(RENAME_COLUMN, "rename-column", INT, "1", required_argument, OPT_SQL | OPT_DDL, "RenameColumn",
      "alter table rename column"),

  /* Analyze Table */
  OPT(ANALYZE, "analyze", INT, "1", required_argument, OPT_SQL | OPT_DDL, "Analyze",
      "analyze table, for partition table randomly analyze either "
      "partition or full table"),

  /* Check Table */
  OPT(CHECK_TABLE, "check", INT, "0", required_argument, OPT_SQL | OPT_DDL, "Check",
      "check table, for partition table randomly check either "
      "partition or full table"),

  /* Check Table Pre-load */
  OPT(CHECK_TABLE_PRELOAD, "check-preload", BOOL, "false", no_argument, 0, "",
      "check table, for partition table randomly check either "
      "partition or full table before the load is started"),

  /* Add drop Partition */
  OPT(ADD_DROP_PARTITION, "add-drop-partition", INT, "3", required_argument, OPT_SQL | OPT_DDL, "AddDropPartition",
      "randomly add drop new partitions"),

  /* maximum number Partition */
  OPT(MAX_PARTITIONS, "max-partitions", INT, "25", required_argument, 0, "",
      "maximum number of partitions in table. choose between 1 and 8192"),

  /* Total number of partition supported */
  OPT(PARTITION_SUPPORTED, "partition-types", STRING, "all", required_argument, 0, "",
      "total partition supported, all for LIST, HASH, KEY, RANGE or to provide "
      "or pass for example --partition-types LIST,HASH,RANGE without space"),

  /* Optimize Table */
  OPT(OPTIMIZE, "optimize", INT, "3", required_argument, OPT_SQL | OPT_DDL, "Optimize",
      "optimize table, for paritition table randomly optimize either "
      "partition or full table "),

  /* Truncate table */
  OPT(TRUNCATE, "truncate", INT, "1", required_argument, OPT_SQL | OPT_DDL, "Truncate",
      "truncate table or in case of partition truncate partition"),

  /* Drop and recreate table */
  OPT(DROP_CREATE, "recreate-table", INT, "1", required_argument, OPT_SQL | OPT_DDL, "RecreateTable",
      "drop and recreate table"),

  /* DATABASE */
  OPT(DATABASE, "database", STRING, "test", required_argument, 0, "",
      "The database to connect to"),

  /* Address */
  OPT(ADDRESS, "address", STRING, "", required_argument, 0, "",
      "Hostname or IP address to connect to. Same as --host."),

  /* Host — alias for --address, matching clickhouse-client's flag name.
     Remapped to ADDRESS in the getopt switch in pstress.cpp, so this option
     never holds a value of its own and the two are genuinely last-one-wins. */
  OPT(HOST, "host", STRING, "", required_argument, 0, "",
      "Hostname or IP address to connect to. Alias for --address."),

  /* Infile */
  OPT(INFILE, "infile", STRING, "pquery.sql", required_argument, 0, "",
      "The SQL input file"),

  /* Logdir */
  OPT(LOGDIR, "logdir", STRING, "/tmp", required_argument, 0, "",
      "Log directory"),

  /* Socket */
  OPT(SOCKET, "socket", STRING, socket_default(), required_argument, 0, "",
      "Socket file to use"),

  /*config file */
  OPT(CONFIGFILE, "config-file", STRING, "", required_argument, 0, "",
      "Config file to use for test"),

  /*Port */
  OPT(PORT, "port", STRING, "3306", required_argument, 0, "",
      "Ports to use, It value is a LIST like 3108,3403,3408, Then it "
      "would create 3 nodes to connect and run load on each node. Also to "
      "connect using port, pass --address for example --address=127.0.0.1 "),

  /* Password*/
  OPT(PASSWORD, "password", STRING, "", required_argument, 0, "",
      "The MySQL user's password"),

  /* HELP */
  OPT(HELP, "help", BOOL, "false", optional_argument, 0, "",
      "user asked for help"),

  OPT(VERBOSE, "verbose", BOOL, "false", no_argument, 0, "",
      "verbose"),

  /* Threads */
  OPT(THREADS, "threads", INT, "1", required_argument, 0, "",
      "The number of threads to use"),

  /* User*/
  OPT(USER, "user", STRING, "root", required_argument, 0, "",
      "The MySQL userID to be used"),

  /* log all queries */
  OPT(LOG_ALL_QUERIES, "log-all-queries", BOOL, "false", no_argument, 0, "",
      "Log all queries (succeeded and failed)"),

  /* execute sql sequentially */
  OPT(NO_SHUFFLE, "no-shuffle", BOOL, "false", no_argument, 0, "",
      "execute SQL sequentially | randomly\n"),

  /* log query statistics */
  OPT(LOG_QUERY_STATISTICS, "log-query-statistics", BOOL, "false", no_argument, 0, "",
      "extended output of query result"),

  /* log client output*/
  OPT(LOG_CLIENT_OUTPUT, "log-client-output", BOOL, "false", no_argument, 0, "",
      "Log query output to separate file"),

  /* log query number*/
  OPT(LOG_QUERY_NUMBERS, "log-query-numbers", BOOL, "false", no_argument, 0, "",
      "write query # to logs"),

  /* log query duration */
  OPT(LOG_QUERY_DURATION, "log-query-duration", BOOL, "false", no_argument, 0, "",
      "Log query duration in milliseconds"),

  /* log failed queries */
  OPT(LOG_FAILED_QUERIES, "log-failed-queries", BOOL, "true", no_argument, 0, "",
      "Log all failed queries"),

  /* log success queries */
  OPT(LOG_SUCCEDED_QUERIES, "log-succeeded-queries", BOOL, "false", no_argument, 0, "",
      "Log succeeded queries"),

  /* queries per thread */
  OPT(QUERIES_PER_THREAD, "queries-per-thread", INT, "1", required_argument, 0, "",
      "The number of queries per thread"),

  /* test connection */
  OPT(TEST_CONNECTION, "test-connection", BOOL, "false", no_argument, 0, "",
      "Test connection to server and exit"),

  OPT(PRINT_TRANSACTION_RATE, "print-rate", INT, "0", required_argument, 0, "",
      "Print transaction rate per N second"),

  /* transaction probability */
  OPT(TRANSATION_PRB_K, "trx-prob-k", INT, "0", required_argument, 0, "",
      "probability(out of 1000) of combining sql as single trx"),

  /* XA TRansaction */
  OPT(XA_TRANSACTION, "xa-trx-prob-k", INT, "0", required_argument, 0, "",
      "Probablity of running XA transaction. Trx size option control "
      "the size of XA transaction"),

  /* Probablity of killing running transaction */
  OPT(KILL_TRANSACTION, "kill-trx-prob-k", INT, "1", required_argument, OPT_SQL | OPT_DDL, "KillQuery",
      "Probablity of killing running transaction"),

  /* tranasaction size */
  OPT(TRANSACTIONS_SIZE, "trx-size", INT, "10", required_argument, 0, "",
      "average size of each trx"),

  OPT(EXTACT_TRANSACTION_SIZE, "exact-trx-size", BOOL, "false", no_argument, 0, "",
      "exact transaction size"),

  /* probability of executing commit */
  OPT(COMMIT_PROB, "commit-prob", INT, "95", required_argument, 0, "",
      "probability of executing commit after a transaction. Else it "
      "would be rollback "),

  /* number of savepoints in trxs */
  OPT(SAVEPOINT_PRB_K, "savepoint-prob-k", INT, "10", required_argument, 0, "",
      "probability of using savepoint in a transaction.\n Also 10% "
      "such transaction will be rollback to some savepoint"),

  /* steps */
  OPT(STEP, "step", INT, "1", required_argument, 0, "",
      "current step in pstress script"),

  /* metadata file path */
  OPT(METADATA_PATH, "metadata-path", STRING, "", required_argument, 0, "",
      "path of metadata file"),

  /* sql format for */
  OPT(GRAMMAR_SQL, "grammar-sql", INT, "10", required_argument, OPT_SQL, "Grammar",
      "grammar sql"),

  /* file name of special sql */
  OPT(GRAMMAR_FILE, "grammar-file", STRING, "grammar.sql", required_argument, 0, "",
      "file to be used  for grammar sql\nT1_INT_1, T1_INT_2 will be replaced "
      "with int columns of some table\n in database T1_VARCHAR_1, T1_VARCHAR_2 "
      "will be replaced with varchar columns of some table in database"),

  OPT(DUMP_OPTIONS_JSON, "dump-options-json", BOOL, "false", no_argument, 0, "",
      "Print every option (name, type, default, current value, flags, "
      "help) as a JSON array on stdout and exit. Values reflect the "
      "other command line options given alongside it."),
};

void register_option(const OptionSpec &s) {
  if (options->at(s.opt) != nullptr) {
    std::cerr << "option table has two rows for Option::" << s.enum_name
              << " (--" << options->at(s.opt)->name << " and --" << s.name
              << ")" << std::endl;
    abort();
  }
  auto *opt = new Option(s.type, s.opt, s.name);
  opt->enum_name = s.enum_name;
  opt->default_value = s.def;
  opt->setArgs(s.args);
  switch (s.type) {
  case Option::BOOL:
    opt->setBool(std::string(s.def));
    break;
  case Option::INT:
    opt->setInt(std::string(s.def));
    break;
  case Option::STRING:
    opt->setString(s.def);
    break;
  case Option::FLOAT:
    opt->setFloat(std::string(s.def));
    break;
  }
  if (s.flags & OPT_SQL)
    opt->setSQL();
  if (s.flags & OPT_DDL)
    opt->setDDL();
  opt->short_help = s.short_help;
  opt->help = s.help;
  options->at(s.opt) = opt;
}

} // namespace

void add_options() {
  options->resize(Option::MAX);
  for (const auto &spec : kOptionSpecs)
    register_option(spec);
}
