#ifndef __COMMON_HPP__
#define __COMMON_HPP__

#ifndef PQVERSION
#define PQVERSION "1"
#endif

#ifdef MAXPACKET
#ifndef MAX_PACKET_DEFAULT
#define MAX_PACKET_DEFAULT 4194304
#endif
#endif

#ifndef PQREVISION
#define PQREVISION "unknown"
#endif

#ifdef __APPLE__
#define PLATFORM_ID "Darwin"
#else
#define PLATFORM_ID "Linux"
#endif

#include <algorithm>
#include <atomic>
#include <getopt.h>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <variant>
#include <vector>

struct Option {
  enum Type { BOOL, INT, STRING, FLOAT } type;
  enum Opt {
    INITIAL_SEED,
    NUMBER_OF_GENERAL_TABLESPACE,
    NUMBER_OF_UNDO_TABLESPACE,
    UNDO_SQL,
    ENGINE,
    JUST_LOAD_DDL,
    NO_DDL,
    ONLY_CL_DDL,
    ONLY_CL_SQL,
    USE_ENCRYPTION,
    ENCRYPTION_TYPE,
    NO_COLUMN_COMPRESSION,
    NO_TABLE_COMPRESSION,
    NO_TABLESPACE,
    NO_BLOB,
    NO_VIRTUAL_COLUMNS,
    TABLES,
    INDEXES,
    UNIQUE_INDEX_PROB_K,
    UNIQUE_RANGE,
    ALGORITHM,
    LOCK,
    COLUMNS,
    INDEX_COLUMNS,
    NO_AUTO_INC,
    NO_DESC_INDEX,
    ONLY_TEMPORARY,
    ONLY_PARTITION,
    INITIAL_RECORDS_IN_TABLE,
    NUMBER_OF_SECONDS_WORKLOAD,
    ALTER_TABLE_ENCRYPTION,
    ALTER_DISCARD_TABLESPACE,
    ALTER_TABLE_COMPRESSION,
    ALTER_COLUMN_MODIFY,
    ALTER_INSTANCE_RELOAD_KEYRING,
    PRIMARY_KEY,
    ROW_FORMAT,
    SERVER_OPTION_FILE,
    SET_GLOBAL_VARIABLE,
    ALTER_MASTER_KEY,
    ALTER_ENCRYPTION_KEY,
    ALTER_GCACHE_MASTER_KEY,
    ALTER_REDO_LOGGING,
    ROTATE_REDO_LOG_KEY,
    ALTER_TABLESPACE_ENCRYPTION,
    ALTER_TABLESPACE_RENAME,
    ALTER_DATABASE_ENCRYPTION,
    NO_SELECT,
    NO_INSERT,
    NO_UPDATE,
    NO_REPLACE,
    NO_DELETE,
    ONLY_SELECT,
    SELECT_ALL_ROW,
    SELECT_ROW_USING_PKEY,
    INSERT_RANDOM_ROW,
    UPDATE_ROW_USING_PKEY,
    REPLACE_ROW,
    UPDATE_ALL_ROWS,
    DELETE_ALL_ROW,
    DELETE_ROW_USING_PKEY,
    INVALID_OPTION = 63,
    LOG_ALL_QUERIES = 'A',
    DATABASE = 'd',
    ADDRESS = 'a',
    INFILE = 'i',
    LOGDIR = 'l',
    SOCKET = 's',
    CONFIGFILE = 'c',
    PORT = 'p',
    PASSWORD = 'P',
    NO_SHUFFLE = 'n',
    THREADS = 't',
    LOG_FAILED_QUERIES = 'F',
    LOG_SUCCEDED_QUERIES = 'S',
    LOG_QUERY_STATISTICS = 'L',
    LOG_QUERY_DURATION = 'D',
    LOG_QUERY_NUMBERS = 'N',
    LOG_CLIENT_OUTPUT = 'O',
    TEST_CONNECTION = 'T',
    QUERIES_PER_THREAD = 'q',
    USER = 'u',
    HELP = 'h',
    VERBOSE = 'v',
    MYSQLD_SERVER_OPTION = 'z',
    TRANSATION_PRB_K,
    TRANSACTIONS_SIZE,
    EXTACT_TRANSACTION_SIZE,
    COMMIT_PROB,
    SAVEPOINT_PRB_K,
    CHECK_TABLE,
    CHECK_TABLE_PRELOAD,
    PARTITION_SUPPORTED,
    ADD_DROP_PARTITION,
    MAX_PARTITIONS,
    STEP,
    METADATA_PATH,
    GRAMMAR_SQL,
    GRAMMAR_FILE,
    DROP_COLUMN,
    ADD_COLUMN,
    DROP_INDEX,
    ADD_INDEX,
    RENAME_COLUMN,
    RENAME_INDEX,
    OPTIMIZE,
    ANALYZE,
    TRUNCATE,
    DROP_CREATE,
    RANDOM_INITIAL_RECORDS,
    PREPARE,
    NO_TEMPORARY,
    NO_PARTITION,
    NO_FK,
    FK_PROB,
    PARTITION_PROB,
    TEMPORARY_PROB,
    IGNORE_ERRORS,
    IGNORE_DML_CLAUSE,
    DROP_WITH_NBO,
    THREAD_PER_TABLE,
    CALL_FUNCTION,
    FUNCTION_CONTAINS_DML,
    OPTION_PROB_FILE,
    NO_TIMESTAMP,
    COMPARE_RESULT,
    COMPARE_CASE_INSENSTIVE,
    SECONDARY_ENGINE,
    WAIT_FOR_SYNC,
    ALTER_SECONDARY_ENGINE,
    NO_FK_CASCADE,
    ENFORCE_MERGE,
    REWRITE_ROW_GROUP_MIN_ROWS,
    REWRITE_ROW_GROUP_MAX_BYTES,
    REWRITE_ROW_GROUP_MAX_ROWS,
    REWRITE_DELTA_NUM_ROWS,
    REWRITE_DELTA_NUM_UNDO,
    REWRITE_GC,
    REWRITE_BLOCKING,
    REWRITE_MAX_ROW_ID_HASH_MAP,
    REWRITE_FORCE,
    REWRITE_NO_RESIDUAL,
    REWRITE_MAX_INTERNAL_BLOB_SIZE,
    REWRITE_BLOCK_COOKER_ROW_GROUP_MAX_ROWS,
    REWRITE_PARTIAL,
    NOT_SECONDARY,
    MODIFY_COLUMN_SECONDARY_ENGINE,
    NO_DATE,
    NO_DATETIME,
    NO_TEXT,
    NO_CHAR,
    NO_VARCHAR,
    NO_FLOAT,
    NO_DECIMAL,
    NO_DOUBLE,
    NO_BOOL,
    NO_INTEGER,
    NO_INT,
    NO_ENUM,
    NO_BIT,
    NULL_PROB,
    SECONDARY_AFTER_CREATE,
    DELAY_IN_SECONDARY,
    SECONDARY_GC,
    SELECT_IN_SECONDARY,
    RANDOM_COLUMNS,
    ADD_NEW_TABLE,
    SINGLE_THREAD_DDL,
    INDEXES_PROB,
    POSITIVE_INT_PROB,
    SECOND_LEVEL_MERGE,
    USING_PK_PROB,
    SELECT_FOR_UPDATE,
    SELECT_FOR_UPDATE_BULK,
    PK_COLUMN_AUTOINC,
    THROTTLE_SLEEP,
    COLUMN_TYPES,
    ALTER_DATABASE_COLLATION,
    XA_TRANSACTION,
    KILL_TRANSACTION,
    THREAD_DOING_ONLY_SELECT,
    NO_JSON,
    RANDOM_TIMEZONE,
    PKEY_IN_SET,
    LOG_PK_BULK_INSERT,
    BULK_INSERT_WIDTH,
    N_LAST_QUERIES,
    PRINT_TRANSACTION_RATE,
    INSERT_BULK,
    INSERT_BULK_COUNT,
    NON_INT_PK,
    DICTIONARY_FILE,
    TOTAL_QUERIES,
    VARCHAR_COLUMN_MAX_WIDTH,
    COMPOSITE_KEY_PROB,
    CH_VERIFY_INTERVAL,
    CH_ALTER_UPDATE,
    CH_ALTER_DELETE,
    CH_MUTATIONS_SYNC,
    MAX
  } option;

  using ValueType = std::variant<bool, long int, std::string, float>;
  ValueType value;

  Option(Type t, Opt o, std::string n)
      : type(t), option(o), name(n), sql(false), ddl(false), total_queries(0),
        success_queries(0), query_in_timespan(0){};
  ~Option();

  void print_pretty();
  Type getType() { return type; };
  Opt getOption() { return option; };
  const char *getName() { return name.c_str(); };
  // Getters for specific types
  bool getBool() {
    if (std::holds_alternative<bool>(value)) {
      return std::get<bool>(value);
    }
    throw std::runtime_error("Option " + name + " is not a BOOL");
  }

  long int getInt() {
    if (std::holds_alternative<long int>(value)) {
      return std::get<long int>(value);
    }
    throw std::runtime_error("Option " + name + " is not an INT");
  }
  short getArgs() { return args; }
  void setArgs(short s) { args = s; };

  std::string getString() {
    if (std::holds_alternative<std::string>(value)) {
      return std::get<std::string>(value);
    }
    throw std::runtime_error("Option " + name + " is not a STRING");
  }

  float getFloat() {
    if (std::holds_alternative<float>(value)) {
      return std::get<float>(value);
    }
    throw std::runtime_error("Option " + name + " is not a FLOAT");
  }

  // Setters for specific types
  void setBool(std::string s) {
    if (type != BOOL) {
      throw std::runtime_error("Cannot set BOOL for non-BOOL option " + name);
    }
    std::transform(s.begin(), s.end(), s.begin(), ::toupper);
    if (s == "ON" || s == "TRUE" || s == "1") {
      value = true;
    } else if (s == "OFF" || s == "FALSE" || s == "0") {
      value = false;
    } else {
      throw std::runtime_error("Invalid boolean value for option " + name +
                               ": " + s);
    }
  }

  void setBool(bool b) {
    if (type != BOOL) {
      throw std::runtime_error("Cannot set BOOL for non-BOOL option " + name);
    }
    value = b;
  }

  void setInt(std::string n) {
    if (type != INT) {
      throw std::runtime_error("Cannot set INT for non-INT option " + name);
    }
    try {
      value = std::stol(n);
    } catch (const std::exception &e) {
      throw std::runtime_error("Invalid integer value for option " + name +
                               ": " + n);
    }
  }

  void setInt(long int n) {
    if (type != INT) {
      throw std::runtime_error("Cannot set INT for non-INT option " + name);
    }
    value = n;
  }

  void setString(std::string n) {
    if (type != STRING) {
      throw std::runtime_error("Cannot set STRING for non-STRING option " +
                               name);
    }
    value = n;
  }

  void setFloat(std::string n) {
    if (type != FLOAT) {
      throw std::runtime_error("Cannot set FLOAT for non-FLOAT option " + name);
    }
    try {
      value = std::stof(n);
    } catch (const std::exception &e) {
      throw std::runtime_error("Invalid float value for option " + name + ": " +
                               n);
    }
  }

  void setFloat(float f) {
    if (type != FLOAT) {
      throw std::runtime_error("Cannot set FLOAT for non-FLOAT option " + name);
    }
    value = f;
  }
  void setSQL() { sql = true; };
  void setDDL() { ddl = true; };
  void set_cl() { cl = true; }

  std::string name;
  std::string help;
  std::string short_help;
  std::string default_value = "";
  long int default_int = 0;       // if default value is integer
  bool default_bool = false;      // if default value is bool
  bool sql;                       // true if option is SQL, False if others
  bool ddl;                       // If SQL is DDL, or false if it is not
  bool cl = false;                // set if it was pass trough command line
  short args = required_argument; // default is required argument
  std::atomic<unsigned long int> total_queries;   // totatl times executed
  std::atomic<unsigned long int> success_queries; // successful count
  std::atomic<unsigned long int> query_in_timespan;
};

struct Server_Option { // Server_options
  Server_Option(std::string n) : name(n){};
  int prob;
  std::string name;
  std::vector<std::string> values;
};

/* delete options and server_options*/
void delete_options();
typedef std::vector<Option *> Opx;
typedef std::vector<Server_Option *> Ser_Opx;
extern Opx *options;
extern Ser_Opx *server_options;
void add_options();
void add_server_options(std::string str);
void add_server_options_file(std::string file_name);
void read_option_prob_file(const std::string &prob_file);
Option *newOption(Option::Type t, Option::Opt o, std::string s);
std::set<int> splitStringToIntSet(const std::string &input);
/* convert string to array of ints */
template <typename T>
std::vector<T> splitStringToArray(const std::string &input,
                                  char delimiter = ',') {

  std::vector<T> result;
  std::string tempValue;
  std::istringstream iss(input);
  while (std::getline(iss, tempValue, delimiter)) {
    T value;
    std::istringstream tempStream(tempValue);
    tempStream >> value;
    if (tempStream.fail()) {
      std::cerr << "Error converting string to type T" << std::endl;
      exit(EXIT_FAILURE);
    }
    result.push_back(value);
  }
  return result;
}

#endif
