/*
 =========================================================
 #     Created by Rahul Malik, Percona LLC             #
 =========================================================
*/
#include "random_test.hpp"
#include "common.hpp"
#include "json.hpp"
#include "node.hpp"
#include <array>
#include <document.h>
#include <filesystem>
#include <iomanip>
#include <libgen.h>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>

#define CR_SERVER_GONE_ERROR 2006
#define CR_SERVER_LOST 2013
#define CR_WSREP_NOT_PREPARED 1047
#define CR_SECONDARY_NOT_READY 6000
extern thread_local std::mt19937 rng;

size_t number_of_records;

bool encrypted_temp_tables = false;
bool encrypted_sys_tablelspaces = false;
bool keyring_comp_status = false;
std::vector<Table *> *all_tables = new std::vector<Table *>;
std::vector<std::string> g_undo_tablespace;
std::vector<std::string> g_encryption;
std::vector<std::string> g_compression = {"none", "zlib", "lz4"};
std::vector<std::string> g_row_format;
std::vector<std::string> g_tablespace;
std::vector<std::string> locks;
std::vector<std::string> algorithms;
std::vector<int> g_key_block_size;
std::vector<std::string> random_strs;
int g_innodb_page_size;
int sum_of_all_opts = 0; // sum of all probablility
std::mutex ddl_logs_write;
std::mutex all_table_mutex;

std::chrono::system_clock::time_point start_time =
    std::chrono::system_clock::now();

std::map<int, Option *> opt_range_map;
std::atomic_flag lock_stream = ATOMIC_FLAG_INIT;
std::atomic<bool> run_query_failed(false);

/* partition type supported by system */
std::vector<Partition::PART_TYPE> Partition::supported;

std::string lower_case_secondary() {
  static auto secondary = []() {
    std::string secondary = options->at(Option::SECONDARY_ENGINE)->getString();
    std::transform(secondary.begin(), secondary.end(), secondary.begin(),
                   ::tolower);
    return secondary;
  };
  return secondary();
}

/* print_error print mysql error to console */
void print_and_log(std::string &&str, Thd1 *thd, bool print_error,
                   bool count_to_console) {
  const int max_print = 300;
  static std::atomic<int> print_so_far = 0;
  if (count_to_console)
    print_so_far++;
  std::stringstream ss;

  // Get current time
  auto now = std::chrono::system_clock::now();
  auto time = std::chrono::system_clock::to_time_t(now);
  ss << "[" << std::put_time(std::localtime(&time), "%Y-%m-%d %H:%M:%S")
     << "] ";

  ss << "Thread " << (thd ? std::to_string(thd->thread_id) : "#") << " : "
     << str;
  if (print_error) {
    ss << "error: " << thd->db->get_error();
  }
  std::lock_guard<std::mutex> lock(ddl_logs_write);
  std::cout << ss.str() << std::endl;
  if (print_so_far > max_print) {
    std::cout << "more than " << max_print << " error on console Exiting"
              << std::endl;
    exit(EXIT_FAILURE);
  }
  if (thd == nullptr)
    return;
  thd->thread_log << ss.str() << std::endl;
}

static bool save_query_result_in_file(const query_result &result,
                                      const std::string &file_name) {
  auto complete_file_name =
      options->at(Option::LOGDIR)->getString() + "/" + file_name;
  std::ofstream file(complete_file_name);
  if (!file.is_open()) {
    std::cerr << "Failed to open file " << file_name << std::endl;
    return false;
  }
  for (auto &row : result) {
    for (auto &col : row) {
      file << col << ",";
    }
    file << std::endl;
  }
  return true;
}

/* compare the result set of two queries and return true if successsful else
 * false and also print the result of queries to afile */
static bool compare_query_result(const query_result &r1, const query_result &r2,
                                 Thd1 *thd, bool case_insensitive = false) {
  auto print_query_result = [r1, r2]() {
    save_query_result_in_file(r1, "secondary_result.csv");
    save_query_result_in_file(r2, "mysql_result.csv");
    return false;
  };
  if (r1.size() != r2.size()) {
    print_and_log("Number of rows in result set do not match", thd);
    return print_query_result();
  }
  for (size_t i = 0; i < r1.size(); i++) {
    if (r1[i].size() != r2[i].size()) {
      print_and_log("Number of columns in result set do not match", thd);
      return print_query_result();
    }
    for (size_t j = 0; j < r1[i].size(); j++) {
      if (case_insensitive) {
        // Perform case-insensitive comparison
        std::string s1 = r1[i][j];
        std::string s2 = r2[i][j];
        // Convert both strings to lowercase for comparison
        std::transform(s1.begin(), s1.end(), s1.begin(), ::tolower);
        std::transform(s2.begin(), s2.end(), s2.begin(), ::tolower);
        if (s1 != s2) {
          print_and_log("Result set do not match", thd);
          return print_query_result();
        }
      } else {
        // Perform case-sensitive comparison
        if (r1[i][j].compare(r2[i][j]) != 0) {
          print_and_log("Result set do not match", thd);
          return print_query_result();
        }
      }
    }
  }
  return true;
}

static size_t convert_to_number(const std::string &str) {
  size_t number = std::stol(str);
  char unit = '\0';

  // Find the first non-digit character for the unit
  for (char c : str) {
    if (!std::isdigit(c)) {
      unit = c;
      break;
    }
  }

  // Adjust number based on unit
  if (unit == 'k' || unit == 'K')
    number *= 1000;
  else if (unit == 'm' || unit == 'M')
    number *= 1000000;

  return number;
}
/* generate random numbers to populate system with unique values
@param[in] number_of_records
@param[out] vector containing unique elements */
std::vector<long int> generateUniqueRandomNumbers(long int number_of_records) {

  std::unordered_set<long int> unique_keys_set(number_of_records);

  long int max_size =
      options->at(Option::UNIQUE_RANGE)->getFloat() * number_of_records;

  /* return sequence */
  if ((options->at(Option::UNIQUE_RANGE)->getFloat() == 1 &&
       options->at(Option::POSITIVE_INT_PROB)->getInt() >= 100)) {
    std::vector<long int> vec(number_of_records);
    std::iota(vec.begin(), vec.end(), 1);
    return vec;
  }

  while (unique_keys_set.size() < static_cast<size_t>(number_of_records)) {
    unique_keys_set.insert(try_negative(rand_int(max_size, 1)));
  }

  std::vector<long int> unique_keys(unique_keys_set.begin(),
                                    unique_keys_set.end());
  return unique_keys;
}

/* run check table */
static bool get_check_result(const std::string &sql, Thd1 *thd) {

  auto result = thd->db->get_query_result(sql);
  if (result.empty() || result.size() < 1 || result[0].size() < 4) {
    print_and_log("CHECK TABLE FAILED" + sql + thd->db->get_error(), thd);
    return false;
  }

  if (result[0][3].compare("OK") != 0) {
    /* print all values */
    for (auto &row : result) {
      for (auto &col : row) {
        std::cout << col << " ";
      }
      std::cout << std::endl;
    }
    print_and_log("Check table failed" + sql + thd->db->get_error(), thd);
    return false;
  }
  return true;
}

/* return server version in number format
 Example 8.0.26 -> 80026
 Example 5.7.35 -> 50735
*/

/* return server version in number format
 Example 8.0.26 -> 80026
 Example 5.7.35 -> 50735
*/

std::string add_ignore_clause() {
  int prob = rand_int(100, 1);
  if (prob <= options->at(Option::IGNORE_DML_CLAUSE)->getInt())
    return " IGNORE ";
  else
    return "";
}

/* return probabality of all options and disable some feature based on user
 * request/ branch/ fork */
int sum_of_all_options(Thd1 *thd) {
  number_of_records = convert_to_number(
      options->at(Option::INITIAL_RECORDS_IN_TABLE)->getString());

  /* find out innodb page_size */
  if (options->at(Option::ENGINE)->getString().compare("INNODB") == 0) {
    g_innodb_page_size =
        std::stoi(thd->db->get_single_value("select @@innodb_page_size"));
    assert(g_innodb_page_size % 1024 == 0);
    g_innodb_page_size /= 1024;
  }

  if (options->at(Option::COLUMN_TYPES)->getString() != "all") {
    auto types = options->at(Option::COLUMN_TYPES)->getString();
    std::transform(types.begin(), types.end(), types.begin(), ::toupper);
    std::vector<std::string> column_types;
    std::stringstream ss(types);
    std::string token;
    while (std::getline(ss, token, ',')) {
      column_types.push_back(token);
    }
    if (std::find(column_types.begin(), column_types.end(), "INTEGER") ==
        column_types.end())
      options->at(Option::NO_INTEGER)->setBool(true);

    if (std::find(column_types.begin(), column_types.end(), "ENUM") ==
        column_types.end())
      options->at(Option::NO_ENUM)->setBool(true);

    if (std::find(column_types.begin(), column_types.end(), "INT") ==
        column_types.end())
      options->at(Option::NO_INT)->setBool(true);

    if (std::find(column_types.begin(), column_types.end(), "DECIMAL") ==
        column_types.end())
      options->at(Option::NO_DECIMAL)->setBool(true);

    if (std::find(column_types.begin(), column_types.end(), "FLOAT") ==
        column_types.end())
      options->at(Option::NO_FLOAT)->setBool(true);

    if (std::find(column_types.begin(), column_types.end(), "DOUBLE") ==
        column_types.end())
      options->at(Option::NO_DOUBLE)->setBool(true);

    if (std::find(column_types.begin(), column_types.end(), "BOOL") ==
        column_types.end())
      options->at(Option::NO_BOOL)->setBool(true);

    if (std::find(column_types.begin(), column_types.end(), "DATE") ==
        column_types.end())
      options->at(Option::NO_DATE)->setBool(true);

    if (std::find(column_types.begin(), column_types.end(), "DATETIME") ==
        column_types.end())
      options->at(Option::NO_DATETIME)->setBool(true);

    if (std::find(column_types.begin(), column_types.end(), "TIMESTAMP") ==
        column_types.end())
      options->at(Option::NO_TIMESTAMP)->setBool(true);

    if (std::find(column_types.begin(), column_types.end(), "BIT") ==
        column_types.end())
      options->at(Option::NO_BIT)->setBool(true);

    if (std::find(column_types.begin(), column_types.end(), "BLOB") ==
        column_types.end())
      options->at(Option::NO_BLOB)->setBool(true);

    if (std::find(column_types.begin(), column_types.end(), "JSON") ==
        column_types.end())
      options->at(Option::NO_JSON)->setBool(true);

    if (std::find(column_types.begin(), column_types.end(), "CHAR") ==
        column_types.end())
      options->at(Option::NO_CHAR)->setBool(true);

    if (std::find(column_types.begin(), column_types.end(), "VARCHAR") ==
        column_types.end())
      options->at(Option::NO_VARCHAR)->setBool(true);

    if (std::find(column_types.begin(), column_types.end(), "TEXT") ==
        column_types.end())
      options->at(Option::NO_TEXT)->setBool(true);

    if (std::find(column_types.begin(), column_types.end(), "VIRTUAL") ==
        column_types.end())
      options->at(Option::NO_VIRTUAL_COLUMNS)->setBool(true);
  }
  /* abort if all the column are set to false */
  if (options->at(Option::NO_INTEGER)->getBool() &&
      options->at(Option::NO_INT)->getBool() &&
      options->at(Option::NO_FLOAT)->getBool() &&
      options->at(Option::NO_DECIMAL)->getBool() &&
      options->at(Option::NO_DOUBLE)->getBool() &&
      options->at(Option::NO_BOOL)->getBool() &&
      options->at(Option::NO_DATE)->getBool() &&
      options->at(Option::NO_DATETIME)->getBool() &&
      options->at(Option::NO_TIMESTAMP)->getBool() &&
      options->at(Option::NO_BIT)->getBool() &&
      options->at(Option::NO_BLOB)->getBool() &&
      options->at(Option::NO_JSON)->getBool() &&
      options->at(Option::NO_ENUM)->getBool() &&
      options->at(Option::NO_CHAR)->getBool() &&
      options->at(Option::NO_VARCHAR)->getBool() &&
      options->at(Option::NO_TEXT)->getBool() &&
      options->at(Option::NO_VIRTUAL_COLUMNS)->getBool()) {
    print_and_log("No column type selected", thd);
    exit(EXIT_FAILURE);
  }

  /*check which all partition type supported */
  auto part_supp = opt_string(PARTITION_SUPPORTED);
  Partition::supported.reserve(4);
  if (part_supp.compare("all") == 0) {
    Partition::supported.push_back(Partition::KEY);
    Partition::supported.push_back(Partition::LIST);
    Partition::supported.push_back(Partition::HASH);
    Partition::supported.push_back(Partition::RANGE);
  } else {
    std::transform(part_supp.begin(), part_supp.end(), part_supp.begin(),
                   ::toupper);
    if (part_supp.find("HASH") != std::string::npos)
      Partition::supported.push_back(Partition::HASH);
    if (part_supp.find("KEY") != std::string::npos)
      Partition::supported.push_back(Partition::KEY);
    if (part_supp.find("LIST") != std::string::npos)
      Partition::supported.push_back(Partition::LIST);
    if (part_supp.find("RANGE") != std::string::npos)
      Partition::supported.push_back(Partition::RANGE);
  }

  if (options->at(Option::MAX_PARTITIONS)->getInt() < 1 ||
      options->at(Option::MAX_PARTITIONS)->getInt() > 8192) {
    print_and_log(
        "invalid range for --max-partition. Choose between 1 and 8192");
    exit(EXIT_FAILURE);
  };

  /* for 5.7 disable some features */
  if (thd->db->get_server_version() < 80000) {
    opt_int_set(ALTER_TABLESPACE_RENAME, 0);
    opt_int_set(RENAME_COLUMN, 0);
    opt_int_set(UNDO_SQL, 0);
    opt_int_set(ALTER_REDO_LOGGING, 0);
  }

  /* check if keyring component is installed */
  if (strcmp(FORK, "DuckDB") != 0 &&
      thd->db->get_single_value(
          "SELECT status_value FROM performance_schema.keyring_component_status WHERE \
    status_key='component_status'") == "Active")
    keyring_comp_status = true;

  locks.reserve(4);
  auto lock = opt_string(LOCK);
  if (lock.compare("all") == 0) {
    locks.push_back("DEFAULT");
    locks.push_back("EXCLUSIVE");
    locks.push_back("SHARED");
    locks.push_back("NONE");
  } else {
    std::transform(lock.begin(), lock.end(), lock.begin(), ::toupper);
    if (lock.find("EXCLUSIVE") != std::string::npos)
      locks.push_back("EXCLUSIVE");
    if (lock.find("SHARED") != std::string::npos)
      locks.push_back("SHARED");
    if (lock.find("NONE") != std::string::npos)
      locks.push_back("NONE");
    if (lock.find("DEFAULT") != std::string::npos)
      locks.push_back("DEFAULT");
  }
  auto algorithm = opt_string(ALGORITHM);
  algorithms.reserve(4);
  if (algorithm.compare("all") == 0) {
    algorithms.push_back("INPLACE");
    algorithms.push_back("COPY");
    algorithms.push_back("INSTANT");
    algorithms.push_back("DEFAULT");
  } else {
    std::transform(algorithm.begin(), algorithm.end(), algorithm.begin(),
                   ::toupper);
    if (algorithm.find("INPLACE") != std::string::npos)
      algorithms.push_back("INPLACE");
    if (algorithm.find("COPY") != std::string::npos)
      algorithms.push_back("COPY");
    if (algorithm.find("INSTANT") != std::string::npos)
      algorithms.push_back("INSTANT");
    if (algorithm.find("DEFAULT") != std::string::npos)
      algorithms.push_back("DEFAULT");
  }

  if (strcmp(FORK, "DuckDB") == 0) {
    options->at(Option::NO_PARTITION)->setBool(true);
    options->at(Option::NO_TEMPORARY)->setBool(true);
    options->at(Option::NO_TABLESPACE)->setBool(true);
    options->at(Option::NO_COLUMN_COMPRESSION)->setBool(true);
    options->at(Option::NO_DESC_INDEX)->setBool(true);
    options->at(Option::INDEXES)->setInt(0);
    options->at(Option::NO_TABLE_COMPRESSION)->setBool(true);
    options->at(Option::NO_AUTO_INC)->setBool(true);
    options->at(Option::NO_FK)->setBool(true);
    options->at(Option::PK_COLUMN_AUTOINC)->setInt(0);
    algorithms.clear();
    locks.clear();
  }

  if (strcmp(FORK, "ClickHouse") == 0) {
    options->at(Option::NO_FK)->setBool(true);
    options->at(Option::NO_AUTO_INC)->setBool(true);
    options->at(Option::PK_COLUMN_AUTOINC)->setInt(0);
    options->at(Option::NO_TABLESPACE)->setBool(true);
    options->at(Option::NO_TEMPORARY)->setBool(true);
    options->at(Option::NO_COLUMN_COMPRESSION)->setBool(true);
    options->at(Option::NO_TABLE_COMPRESSION)->setBool(true);
    options->at(Option::XA_TRANSACTION)->setInt(0);
    options->at(Option::SAVEPOINT_PRB_K)->setInt(0);
    options->at(Option::NO_VIRTUAL_COLUMNS)->setBool(true);
    options->at(Option::NO_ENUM)->setBool(true);
    options->at(Option::NO_DESC_INDEX)->setBool(true);
    options->at(Option::INDEXES)->setInt(0);
    options->at(Option::NO_PARTITION)->setBool(true);
    options->at(Option::IGNORE_DML_CLAUSE)->setInt(0);
    options->at(Option::REPLACE_ROW)->setInt(0);
    options->at(Option::SELECT_FOR_UPDATE)->setInt(0);
    options->at(Option::SELECT_FOR_UPDATE_BULK)->setInt(0);
    options->at(Option::CALL_FUNCTION)->setInt(0);
    options->at(Option::ADD_DROP_PARTITION)->setInt(0);
    options->at(Option::RENAME_INDEX)->setInt(0);
    options->at(Option::ALTER_DISCARD_TABLESPACE)->setInt(0);
    options->at(Option::ALTER_DATABASE_COLLATION)->setInt(0);
    options->at(Option::NO_BIT)->setBool(true);
    /* Composite key columns not in primary_key flag would break ORDER BY prefix */
    options->at(Option::COMPOSITE_KEY_PROB)->setInt(0);
    algorithms.clear();
    locks.clear();
  }

  /* Disabling alter discard tablespace until 8.0.30
   * Bug: https://jira.percona.com/browse/PS-7865 is fixed by upstream in
   * MySQL 8.0.31 */
  if (thd->db->get_server_version() >= 80000 &&
      thd->db->get_server_version() <= 80030) {
    opt_int_set(ALTER_DISCARD_TABLESPACE, 0);
  }

  auto enc_type = options->at(Option::ENCRYPTION_TYPE)->getString();

  /* for percona-server we have additional encryption type keyring */
  if (enc_type.compare("all") == 0) {
    g_encryption = {"Y", "N"};
    if (strcmp(FORK, "Percona-Server") == 0) {
      g_encryption.push_back("KEYRING");
    }
  } else if (enc_type.compare("oracle") == 0) {
    g_encryption = {"Y", "N"};
    options->at(Option::ALTER_ENCRYPTION_KEY)->setInt(0);
  } else
    g_encryption = {enc_type};

  /* feature not supported by oracle */
  if (strcmp(FORK, "MySQL") == 0) {
    options->at(Option::ALTER_DATABASE_ENCRYPTION)->setInt(0);
    options->at(Option::NO_COLUMN_COMPRESSION)->setBool("true");
    options->at(Option::ALTER_ENCRYPTION_KEY)->setInt(0);
  }

  if (options->at(Option::PK_COLUMN_AUTOINC)->getInt() == 100) {
    options->at(Option::NON_INT_PK)->setInt(0);
  }

  if (options->at(Option::SECONDARY_ENGINE)->getString() == "") {
    options->at(Option::ALTER_SECONDARY_ENGINE)->setInt(0);
    options->at(Option::ENFORCE_MERGE)->setInt(0);
    options->at(Option::REWRITE_ROW_GROUP_MIN_ROWS)->setInt(0);
    options->at(Option::REWRITE_ROW_GROUP_MAX_BYTES)->setInt(0);
    options->at(Option::REWRITE_ROW_GROUP_MAX_ROWS)->setInt(0);
    options->at(Option::REWRITE_DELTA_NUM_ROWS)->setInt(0);
    options->at(Option::REWRITE_DELTA_NUM_UNDO)->setInt(0);
    options->at(Option::REWRITE_GC)->setInt(0);
    options->at(Option::REWRITE_BLOCKING)->setInt(0);
    options->at(Option::REWRITE_MAX_ROW_ID_HASH_MAP)->setInt(0);
    options->at(Option::REWRITE_FORCE)->setInt(0);
    options->at(Option::REWRITE_NO_RESIDUAL)->setInt(0);
    options->at(Option::REWRITE_MAX_INTERNAL_BLOB_SIZE)->setInt(0);
    options->at(Option::REWRITE_BLOCK_COOKER_ROW_GROUP_MAX_ROWS)->setInt(0);
    options->at(Option::REWRITE_PARTIAL)->setInt(0);
    options->at(Option::SECONDARY_GC)->setInt(0);
    options->at(Option::ALTER_SECONDARY_ENGINE)->setInt(0);
    options->at(Option::MODIFY_COLUMN_SECONDARY_ENGINE)->setInt(0);
    options->at(Option::WAIT_FOR_SYNC)->setBool(false);
    options->at(Option::SECONDARY_AFTER_CREATE)->setBool(false);
    options->at(Option::NOT_SECONDARY)->setInt(0);
  } else {
    /* disable some of options for secondary engine */
    options->at(Option::PRIMARY_KEY)->setInt(100);
    options->at(Option::NO_TEMPORARY)->setBool(true);
    options->at(Option::NO_TABLESPACE)->setBool(true);
    options->at(Option::COMMIT_PROB)->setInt(100);
    options->at(Option::SELECT_FOR_UPDATE)->setInt(0);
    options->at(Option::SELECT_FOR_UPDATE_BULK)->setInt(0);
    opt_int_set(UNDO_SQL, 0);
    opt_int_set(ALTER_REDO_LOGGING, 0);
    /* disable FK Columns */
    options->at(Option::NO_FK)->setBool(true);
  }


  if (options->at(Option::ONLY_PARTITION)->getBool() &&
      options->at(Option::ONLY_TEMPORARY)->getBool()) {
    print_and_log("choose either only partition or only temporary ");
    exit(EXIT_FAILURE);
  }

  if (options->at(Option::ONLY_PARTITION)->getBool() &&
      options->at(Option::NO_PARTITION)->getBool()) {
    print_and_log("choose either only partition or no partition");
    exit(EXIT_FAILURE);
  }

  if (options->at(Option::NO_PARTITION)->getBool()) {
    options->at(Option::PARTITION_PROB)->setInt(0);
  }

  if (options->at(Option::NO_FK)->getBool()) {
    options->at(Option::FK_PROB)->setInt(0);
  }

  if (options->at(Option::ONLY_PARTITION)->getBool())
    options->at(Option::NO_TEMPORARY)->setBool("true");

  if (options->at(Option::ONLY_SELECT)->getBool()) {
    options->at(Option::NO_UPDATE)->setBool(true);
    options->at(Option::NO_REPLACE)->setBool(true);
    options->at(Option::NO_DELETE)->setBool(true);
    options->at(Option::NO_INSERT)->setBool(true);
    options->at(Option::NO_DDL)->setBool(true);
  } else if (options->at(Option::NO_SELECT)->getBool()) {
    /* if select is set as zero, disable all type of selects */
    options->at(Option::SELECT_ALL_ROW)->setInt(0);
    options->at(Option::SELECT_ROW_USING_PKEY)->setInt(0);
    options->at(Option::SELECT_FOR_UPDATE)->setInt(0);
    options->at(Option::SELECT_FOR_UPDATE_BULK)->setInt(0);
    options->at(Option::GRAMMAR_SQL)->setInt(0);
  }

  /* if delete is set as zero, disable all type of deletes */
  if (options->at(Option::NO_DELETE)->getBool()) {
    options->at(Option::DELETE_ALL_ROW)->setInt(0);
    options->at(Option::DELETE_ROW_USING_PKEY)->setInt(0);
  }
  /* If update is disable, set all update probability to zero */
  if (options->at(Option::NO_UPDATE)->getBool()) {
    options->at(Option::UPDATE_ROW_USING_PKEY)->setInt(0);
    options->at(Option::UPDATE_ALL_ROWS)->setInt(0);
    options->at(Option::REPLACE_ROW)->setInt(0);
  }
  /* if insert is disable, set all insert probability to zero */
  if (options->at(Option::NO_INSERT)->getBool()) {
    opt_int_set(INSERT_RANDOM_ROW, 0);
    opt_int_set(INSERT_BULK, 0);
  }

  /* disable call  function if no insert update and delete */
  if (options->at(Option::NO_UPDATE)->getBool() &&
      options->at(Option::NO_DELETE)->getBool() &&
      options->at(Option::NO_INSERT)->getBool()) {
    options->at(Option::CALL_FUNCTION)->setInt(0);
  }

  /* if no-tbs, do not execute tablespace related sql */
  if (options->at(Option::NO_TABLESPACE)->getBool()) {
    opt_int_set(ALTER_TABLESPACE_RENAME, 0);
    opt_int_set(ALTER_TABLESPACE_ENCRYPTION, 0);
  }

  /* options to disable if engine is not INNODB */
  std::string engine = options->at(Option::ENGINE)->getString();
  std::transform(engine.begin(), engine.end(), engine.begin(), ::toupper);
  if (engine.compare("ROCKSDB") == 0) {
    options->at(Option::NO_TEMPORARY)->setBool("true");
    options->at(Option::NO_COLUMN_COMPRESSION)->setBool("true");
    options->at(Option::NO_DESC_INDEX)->setBool(true);
    options->at(Option::NO_TABLE_COMPRESSION)->setBool(true);
  }

  /* If no-encryption is set, disable all encryption options */
  if (!options->at(Option::USE_ENCRYPTION)->getBool()) {
    opt_int_set(ALTER_TABLE_ENCRYPTION, 0);
    opt_int_set(ALTER_TABLESPACE_ENCRYPTION, 0);
    opt_int_set(ALTER_MASTER_KEY, 0);
    opt_int_set(ALTER_ENCRYPTION_KEY, 0);
    opt_int_set(ALTER_GCACHE_MASTER_KEY, 0);
    opt_int_set(ROTATE_REDO_LOG_KEY, 0);
    opt_int_set(ALTER_DATABASE_ENCRYPTION, 0);
    opt_int_set(ALTER_INSTANCE_RELOAD_KEYRING, 0);
  }

  if (thd->db->get_single_value("select @@innodb_temp_tablespace_encrypt") ==
      "1")
    encrypted_temp_tables = true;

  if (strcmp(FORK, "Percona-Server") == 0 &&
      thd->db->get_single_value("select @@innodb_sys_tablespace_encrypt") ==
          "1")
    encrypted_sys_tablelspaces = true;

  /* Disable GCache encryption for MS or PS, only supported in PXC-8.0 */
  if (strcmp(FORK, "Percona-XtraDB-Cluster") != 0 ||
      (strcmp(FORK, "Percona-XtraDB-Cluster") == 0 &&
       thd->db->get_server_version() < 80000))
    opt_int_set(ALTER_GCACHE_MASTER_KEY, 0);

  /* If OS is Mac, disable table compression as hole punching is not supported
   * on OSX */
  if (strcmp(PLATFORM_ID, "Darwin") == 0)
    options->at(Option::NO_TABLE_COMPRESSION)->setBool(true);

  /* If no-table-compression is set, disable all compression */
  if (options->at(Option::NO_TABLE_COMPRESSION)->getBool()) {
    opt_int_set(ALTER_TABLE_COMPRESSION, 0);
    g_compression.clear();
  }

  /* if no dynamic variables is passed set-global to zero */
  if (server_options->empty())
    opt_int_set(SET_GLOBAL_VARIABLE, 0);

  auto only_cl_ddl = opt_bool(ONLY_CL_DDL);
  auto only_cl_sql = opt_bool(ONLY_CL_SQL);
  auto no_ddl = opt_bool(NO_DDL);

  /* if set, then disable all other SQL*/
  if (only_cl_sql) {
    for (auto &opt : *options) {
      if (opt != nullptr && opt->sql && !opt->cl)
        opt->setInt(0);
    }
  }

  /* only-cl-ddl, if set then disable all other DDL */
  if (only_cl_ddl) {
    for (auto &opt : *options) {
      if (opt != nullptr && opt->ddl && !opt->cl)
        opt->setInt(0);
    }
  }

  if (only_cl_ddl && no_ddl) {
    print_and_log("noddl && only-cl-ddl are passed together", thd);
  }

  /* if no ddl is set disable all ddl */
  if (no_ddl) {
    for (auto &opt : *options) {
      if (opt != nullptr && opt->sql && opt->ddl)
        opt->setInt(0);
    }
  }

  /* if kill query retry on error tododb only accepted error */
  if (options->at(Option::KILL_TRANSACTION)->getInt() > 0) {
    options->at(Option::IGNORE_ERRORS)->setString("all");
  }

  int total = 0;
  bool is_ddl = false;
  for (auto &opt : *options) {
    if (opt == nullptr)
      continue;
    if (opt->getType() == Option::INT)
      thd->thread_log << opt->getName() << "=>" << opt->getInt() << std::endl;
    else if (opt->getType() == Option::BOOL)
      thd->thread_log << opt->getName() << "=>" << opt->getBool() << std::endl;
    if (!opt->sql || opt->getInt() == 0)
      continue;
    if (opt->ddl) {
      is_ddl = true;
    }
    total += opt->getInt();
    opt_range_map[total] = opt;
  }
  if (!is_ddl) {
    options->at(Option::NO_DDL)->setBool(true);
  }

  if (total == 0) {
    print_and_log("no option selected", nullptr);
    exit(EXIT_FAILURE);
  }
  return total;
}

/* return some options */
Option::Opt pick_some_option() {
  int rd = rand_int(sum_of_all_opts);
  auto it = opt_range_map.lower_bound(rd);
  assert(it != opt_range_map.end());
  return it->second->getOption();
}

int sum_of_all_server_options() {
  int total = 0;
  for (auto &opt : *server_options) {
    total += opt->prob;
  }
  return total;
}

/* pick some algorithm. and if caller pass value of algo & lock set it */
inline static std::string
pick_algorithm_lock(std::string *const algo = nullptr,
                    std::string *const lock = nullptr) {

  std::string current_lock;
  std::string current_algo;

  /* algorithm is empty for duckdb */
  if ((algorithms.empty() && locks.empty()) || rand_int(100) == 1) {
    if (algo != nullptr)
      *algo = "";
    if (lock != nullptr)
      *lock = "";

    return "";
  }

  current_algo = algorithms[rand_int(algorithms.size() - 1)];

  /*
    Support Matrix	LOCK=DEFAULT	LOCK=EXCLUSIVE	 LOCK=NONE LOCK=SHARED
    ALGORITHM=INPLACE	Supported	Supported	 Supported Supported
    ALGORITHM=COPY	Supported	Supported	 Not Supported Supported
    ALGORITHM=INSTANT	Supported	Not Supported	 Not Supported  Not
    Supported ALGORITHM=DEFAULT	Supported	Supported        Supported
    Supported
  */

  /* If current_algo=INSTANT, we can set current_lock=DEFAULT directly as it is
   * the only supported option */
  if (current_algo == "INSTANT")
    current_lock = "DEFAULT";
  /* If current_algo=COPY; MySQL supported LOCK values are
   * DEFAULT,EXCLUSIVE,SHARED. At this point, it may pick LOCK=NONE as well, but
   * we will handle it later in the code. If current_algo=INPLACE|DEFAULT;
   * randomly pick any value, since all lock types are supported.*/
  else
    current_lock = locks[rand_int(locks.size() - 1)];

  /* Handling the incompatible combination at the end.
   * A user may see a deviation if he has opted for --alter-lock to NOT
   * run with DEFAULT. But this is an exceptional case.
   */
  if (current_algo == "COPY" && current_lock == "NONE")
    current_lock = "DEFAULT";

  if (algo != nullptr)
    *algo = current_algo;
  if (lock != nullptr)
    *lock = current_lock;

  return ", LOCK=" + current_lock + ", ALGORITHM=" + current_algo;
}

/* set seed of current thread */
int set_seed(Thd1 *thd) {
  int thread_id = 0;
  if (thd != nullptr) {
    thread_id = thd->thread_id;
  }

  return options->at(Option::INITIAL_SEED)->getInt() + thread_id * 1000 +
         options->at(Option::STEP)->getInt();
}

/* generate random strings of size N_STR */
std::vector<std::string> random_strs_generator() {

  auto exePath = getExecutablePath();
  std::filesystem::path exeDir = std::filesystem::path(exePath).parent_path();
  std::ifstream file(exeDir /
                     options->at(Option::DICTIONARY_FILE)->getString());
  if (!file) {
    throw std::runtime_error("Failed to open file: " +
                             options->at(Option::DICTIONARY_FILE)->getString());
  }
  size_t line_count = std::count(std::istreambuf_iterator<char>(file),
                                 std::istreambuf_iterator<char>(), '\n');
  std::vector<std::string> strs;
  file.clear(); // Clear EOF flag
  file.seekg(0, std::ios::beg);
  strs.reserve(line_count);
  std::string str;
  while (std::getline(file, str)) {
    strs.push_back(str);
  }
  file.close();
  return strs;
}

long int rand_int(long int upper, long int lower) {
  assert(upper >= lower);
  std::uniform_int_distribution<std::mt19937::result_type> dist(lower, upper);
  return dist(rng);
}

/* return random float number in the range of upper and lower */
std::string rand_float(float upper, float lower) {
  assert(upper >= lower);
  std::uniform_real_distribution<> dis(lower, upper);
  std::ostringstream out;
  out << std::fixed;
  out << std::setprecision(2) << try_negative(dis(rng));
  return out.str();
}

std::string rand_double(double upper, double lower) {
  assert(upper >= lower);
  std::uniform_real_distribution<> dis(lower, upper);
  std::ostringstream out;
  out << std::fixed;
  out << std::setprecision(5) << try_negative(dis(rng));
  return out.str();
}

static std::string rand_bit(int length) {
  std::string bit = "b\'";
  for (int i = 0; i < length; i++) {
    bit += std::to_string(rand_int(1));
  }
  bit += "\'";
  return bit;
}

static std::string generateRandomString(int n) {
  const std::string alphabet =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

  std::string result;
  result.reserve(n);
  for (int i = 0; i < n; ++i) {
    result += alphabet[rand_int(alphabet.size() - 1)];
  }
  return result;
}


/* return random string in range of upper and lower. If it can't find return
 * random generated string*/
std::string rand_string(size_t size) {
  /* if bigger string is request then ensure minimum 10 */
  if (size > 10) {
    size = rand_int(size, 10);
  }
  std::string rs;
  while (size > 0 && !random_strs.empty()) {
    const auto &str = random_strs.at(rand_int(random_strs.size() - 1));
    if (size > str.size()) {
      if (size == str.size()) {
        return rs + str;
      }
      rs += str + " ";
      size -= str.size() + 1;
    } else {
      break;
    }
  }
  if (rs.empty())
    return generateRandomString(size);
#ifdef USE_DUCKDB
      /* if string has ' replace it with '' */
      rs = std::regex_replace(rs, std::regex("'"), "''");
#endif
      return rs;
}

/* return column type from a string */
Column::COLUMN_TYPES Column::col_type(std::string type) {
  if (type.compare("INTEGER") == 0)
    return INTEGER;
  else if (type.compare("INT") == 0)
    return INT;
  else if (type.compare("CHAR") == 0)
    return CHAR;
  else if (type.compare("VARCHAR") == 0)
    return VARCHAR;
  else if (type.compare("BOOL") == 0)
    return BOOL;
  else if (type.compare("GENERATED") == 0)
    return GENERATED;
  else if (type.compare("BLOB") == 0)
    return BLOB;
  else if (type.compare("JSON") == 0)
    return JSON;
  else if (type.compare("DECIMAL") == 0)
    return DECIMAL;
  else if (type.compare("FLOAT") == 0)
    return FLOAT;
  else if (type.compare("DOUBLE") == 0)
    return DOUBLE;
  else if (type.compare("DATE") == 0)
    return DATE;
  else if (type.compare("DATETIME") == 0)
    return DATETIME;
  else if (type.compare("TIMESTAMP") == 0)
    return TIMESTAMP;
  else if (type.compare("TEXT") == 0)
    return TEXT;
  else if (type.compare("BIT") == 0)
    return BIT;
  else if (type.compare("ENUM") == 0)
    return ENUM;
  else {
    print_and_log("unhandled " + col_type_to_string(type_) + " at line " +
                  std::to_string(__LINE__));
    exit(EXIT_FAILURE);
  }
}

/* return string from a column type */
const std::string Column::col_type_to_string(COLUMN_TYPES type) {
  switch (type) {
  case ENUM:
    return "ENUM";
  case INTEGER:
    return "INTEGER";
  case INT:
    return "INT";
  case CHAR:
    return "CHAR";
  case DOUBLE:
    return "DOUBLE";
  case DECIMAL:
    return "DECIMAL";
  case FLOAT:
    return "FLOAT";
  case VARCHAR:
    return "VARCHAR";
  case BOOL:
    return "BOOL";
  case BLOB:
    return "BLOB";
  case JSON:
    return "JSON";
  case GENERATED:
    return "GENERATED";
  case DATE:
    return "DATE";
  case DATETIME:
    return "DATETIME";
  case TIMESTAMP:
    return "TIMESTAMP";
  case TEXT:
    return "TEXT";
  case COLUMN_MAX:
  case BIT:
    return "BIT";
    break;
  }
  return "FAIL";
}
// Helper function to generate a random time zone offset
static std::string rand_timezone() {
  // Common time zone offsets (e.g., -12:00 to +14:00)
  static const std::string offsets[] = {
      "+00:00", // UTC
      "+01:00", // CET
      "+02:00", // CEST
      "-05:00", // EST
      "-08:00", // PST
      "+09:00", // JST
      "+10:00", // AEST
      "-03:00"  // ART
  };
  int index = rand_int(7, 0); // Select from 8 common offsets
  return offsets[index];
}
static std::string rand_date() {
  std::ostringstream out;
  int year = rand_int(9999, 1000); // Year from 1000 to 9999
  int month = rand_int(12, 1);     // Month from 1 to 12
  int max_day = 28;                // Default to 28 for February
  if (month == 4 || month == 6 || month == 9 || month == 11) {
    max_day = 30; // April, June, September, November
  } else if (month != 2) {
    max_day = 31; // January, March, May, July, August, October, December
  } else if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
    max_day = 29; // Leap year February
  }
  int day = rand_int(max_day, 1); // Day based on month and leap year
  out << std::setfill('0') << std::setw(4) << year << "-" << std::setw(2)
      << month << "-" << std::setw(2) << day;
  return out.str();
}

static std::string rand_datetime() {
  std::ostringstream out;
  int year = rand_int(9999, 1000); // Year from 1000 to 9999
  int month = rand_int(12, 1);     // Month from 1 to 12
  int max_day = 28;                // Default to 28 for February
  if (month == 4 || month == 6 || month == 9 || month == 11) {
    max_day = 30; // April, June, September, November
  } else if (month != 2) {
    max_day = 31; // January, March, May, July, August, October, December
  } else if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
    max_day = 29; // Leap year February
  }
  int day = rand_int(max_day, 1); // Day based on month and leap year
  int hour = rand_int(23, 0);     // Hour from 0 to 23
  int minute = rand_int(59, 0);   // Minute from 0 to 59
  int second = rand_int(59, 0);   // Second from 0 to 59

  out << std::setfill('0') << std::setw(4) << year << "-" << std::setw(2)
      << month << "-" << std::setw(2) << day << " " << std::setw(2) << hour
      << ":" << std::setw(2) << minute << ":" << std::setw(2) << second;
  return out.str();
}
static std::string rand_timestamp(int precision = 0,
                                  bool include_timezone = true) {
  std::ostringstream out;
  // TIMESTAMP range: 1970-01-01 00:00:00 to 2038-01-19 03:14:07
  int year = rand_int(2037, 1971);
  int month = rand_int(12, 1);
  int max_day = 28;
  if (month == 4 || month == 6 || month == 9 || month == 11) {
    max_day = 30;
  } else if (month != 2) {
    max_day = 31;
  } else if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
    max_day = 29;
  }
  if (year == 2038 && month == 1) {
    max_day = 19;
  }
  int day = rand_int(max_day, 1);
  int hour = rand_int(23, 0);
  int minute = rand_int(59, 0);
  int second = rand_int(59, 0);
  if (year == 2038 && month == 1 && day == 19) {
    hour = rand_int(3, 0);
    if (hour == 3) {
      minute = rand_int(14, 0);
      if (minute == 14) {
        second = rand_int(7, 0);
      }
    }
  }

  // Avoid DST transition for March/April 2030
  if (year == 2030 && month == 3 && day >= 30 && day <= 31 && hour == 2) {
    hour = rand_int(1, 0) ? 1 : 3;
  }

  // Get time zone and ensure UTC equivalent is within TIMESTAMP range
  std::string tz = include_timezone ? rand_timezone() : "+00:00";

  // Format date and time
  out << std::setfill('0') << std::setw(4) << year << "-" << std::setw(2)
      << month << "-" << std::setw(2) << day << " " << std::setw(2) << hour
      << ":" << std::setw(2) << minute << ":" << std::setw(2) << second;

  // Add fractional seconds if precision > 0
  if (precision > 0) {
    out << ".";
    int max_fraction = 1;
    for (int i = 0; i < std::min(precision, 6); ++i) {
      max_fraction *= 10;
    }
    int fraction = rand_int(max_fraction - 1, 0);
    out << std::setfill('0') << std::setw(std::min(precision, 6)) << fraction;
  }

  // Append time zone if requested
  if (include_timezone) {
    out << tz;
  }

  return out.str();
}

Enum_Column::Enum_Column(std::string name, Table *table)
    : Column(table, Column::ENUM) {
  length = 0;
  name_ = "e" + name;
  size_t number_of_enum = rand_int(10, 3);
  enum_values.reserve(number_of_enum);
  for (size_t i = 0; i < number_of_enum; i++) {
    std::string value = random_strs.at(rand_int(random_strs.size() - 1));
    length = std::max(length, (int)value.size());
    if (std::find(enum_values.begin(), enum_values.end(), value) ==
        enum_values.end()) {
      enum_values.push_back(value);
    }
  }
}

static std::string list_partition_random_value(Partition *part_table) {
  auto rand_list = part_table->lists.at(rand_int(part_table->lists.size() - 1));
  auto value = rand_list.list.at(rand_int(rand_list.list.size() - 1));
  return std::to_string(value);
}
static std::string range_partition_random_value(Partition *part_table) {
  static long int minimum =
      -1 * (number_of_records * options->at(Option::UNIQUE_RANGE)->getFloat());

  /* try to return values less than lowest value */
  if (rand_int(1000) == 0) {
    return std::to_string(rand_int(0, minimum));
  }
  auto value = std::to_string(rand_int(
      part_table->positions.at(part_table->positions.size() - 1).range - 1,
      part_table->positions.at(0).range));

  return value;
}

std::string Column::rand_value() {

  /* if column is partition column  and list partition return a value */
  if (is_partition) {
    auto curr_part_type = static_cast<Partition *>(table_)->part_type;

    if (curr_part_type == Partition::LIST) {
      return list_partition_random_value(static_cast<Partition *>(table_));
    } else if (curr_part_type == Partition::RANGE) {
      return range_partition_random_value(static_cast<Partition *>(table_));
    }
  }

  bool should_return_null =
      (rand_int(100, 1) <= options->at(Option::NULL_PROB)->getInt()) &&
      null_val;

  if (should_return_null) {
    return "NULL";
  }

  auto current_type = type_;
  if (current_type == Column::COLUMN_TYPES::GENERATED) {
    current_type = static_cast<const Generated_Column *>(this)->generate_type();
  }
  /* if primary key we varchar */
  if (primary_key == true) {
    auto value = std::to_string(try_negative(rand_int(
        options->at(Option::UNIQUE_RANGE)->getFloat() * number_of_records)));
    if (current_type == Column::COLUMN_TYPES::VARCHAR) {
      std::string result;
      result.reserve(value.size() + 2);
#ifdef USE_MYSQL
      result += '"';
      result += value;
      result += '"';
#else
      result += '\'';
      result += value;
      result += '\'';
#endif
      return result;
    }
    return value;
  }

  switch (current_type) {
  case Column::COLUMN_TYPES::INTEGER:
    return std::to_string(try_negative(rand_int(number_of_records)));
  case Column::COLUMN_TYPES::INT:
    return std::to_string(try_negative(rand_int(
        options->at(Option::UNIQUE_RANGE)->getFloat() * number_of_records)));
  case Column::COLUMN_TYPES::FLOAT:
    return rand_float(number_of_records);
  case Column::COLUMN_TYPES::DOUBLE:
    return rand_double(1.0 / options->at(Option::UNIQUE_RANGE)->getFloat() *
                       number_of_records);
  case Column::COLUMN_TYPES::CHAR:
  case Column::COLUMN_TYPES::VARCHAR:
  case Column::COLUMN_TYPES::TEXT:
#ifdef USE_MYSQL
    return "\"" + rand_string(length) + "\"";
#else
    return "\'" + rand_string(length) + "\'";
#endif
  case Column::COLUMN_TYPES::BLOB:
#ifdef USE_MYSQL
    return "_binary\"" + rand_string(length) + "\"";
#else
    return "\'" + rand_string(length) + "\'";
#endif
  case Column::COLUMN_TYPES::JSON:
    return "\'" + json_rand_doc(this) + "\'";
  case Column::COLUMN_TYPES::BIT:
    return rand_bit(length);
    break;
  case Column::COLUMN_TYPES::BOOL:
    return (rand_int(1) == 1 ? "true" : "false");
    break;
  case Column::COLUMN_TYPES::DATE:
    return "\'" + rand_date() + "\'";
  case Column::COLUMN_TYPES::DATETIME:
    return "\'" + rand_datetime() + "\'";
  case Column::COLUMN_TYPES::TIMESTAMP:
#ifdef USE_MYSQL
    return "\'" + rand_timestamp() + "\'";
#else
    return "\'" + rand_timestamp(0, false) + "\'";
#endif
    break;
  case Column::COLUMN_TYPES::GENERATED:
  case Column::COLUMN_TYPES::ENUM:
  case Column::COLUMN_TYPES::DECIMAL:
  case Column::COLUMN_TYPES::COLUMN_MAX:
    print_and_log("unhandled " + Column::col_type_to_string(type_) +
                  " at line " + std::to_string(__LINE__));
    exit(EXIT_FAILURE);
  }
  return "";
}

/* return table definition */
std::string Column::definition() {
#ifdef USE_CLICKHOUSE
  /* ClickHouse columns are NOT NULL by default; use Nullable() for nullable */
  std::string type_str = null_val ? "Nullable(" + clause() + ")" : clause();
  return name_ + " " + type_str;
#else
  std::string def = name_ + " " + clause();
  if (!null_val)
    def += " NOT NULL";
  if (auto_increment)
    def += " AUTO_INCREMENT";
  if (compressed) {
    def += " COLUMN_FORMAT COMPRESSED";
  }
  if (not_secondary)
    def += " NOT SECONDARY";
  return def;
#endif
}

/* add new column, part of create table or Alter table */
Column::Column(std::string name, Table *table, COLUMN_TYPES type)
    : table_(table) {
  type_ = type;
  switch (type) {
  case CHAR:
    name_ = "c" + name;
    length =
        rand_int(options->at(Option::VARCHAR_COLUMN_MAX_WIDTH)->getInt(), 5);
    break;
  case VARCHAR:
    name_ = "v" + name;
    length =
        rand_int(options->at(Option::VARCHAR_COLUMN_MAX_WIDTH)->getInt(), 5);
    if (name == "pkey") {
      length = 50;
    }
    break;
  case INT:
  case INTEGER:
    name_ = "i" + name;
    break;
  case FLOAT:
    name_ = "f" + name;
    break;
  case DOUBLE:
    name_ = "d" + name;
    break;
  case BOOL:
    name_ = "t" + name;
    break;
  case DATE:
    name_ = "dt" + name;
    break;
  case DATETIME:
    name_ = "dtm" + name;
    break;
  case TIMESTAMP:
    name_ = "ts" + name;
    break;
  case JSON:
    name_ = "j" + name;
    break;
  case BIT:
    name_ = "bt" + name;
    length = rand_int(64, 5);
    break;
  case ENUM:
    name_ = name;
    break;
  case DECIMAL:
    name_ = name;
    break;
  default:
    print_and_log("unhandled " + col_type_to_string(type_) + " at line " +
                  std::to_string(__LINE__));
    exit(EXIT_FAILURE);
  }
}

/* add new blob column, part of create table or Alter table */
Blob_Column::Blob_Column(std::string name, Table *table)
    : Column(table, Column::BLOB) {

  if (options->at(Option::NO_COLUMN_COMPRESSION)->getBool() == false &&
      rand_int(1) == 1)
    compressed = true;

  switch (rand_int(4, 1)) {
  case 1:
    sub_type = "TINYBLOB";
    name_ = "tb" + name;
    length = rand_int(255, 100);
    break;
  case 2:
    sub_type = "BLOB";
    name_ = "b" + name;
    length = rand_int(1000, 100);
    break;
  case 3:
    sub_type = "MEDIUMBLOB";
    name_ = "mb" + name;
    length = rand_int(3000, 1000);
    break;
  case 4:
    sub_type = "LONGBLOB";
    name_ = "lb" + name;
    length = rand_int(4000, 100);
    break;
  }
}

Blob_Column::Blob_Column(std::string name, Table *table, std::string sub_type_)
    : Column(table, Column::BLOB) {
  name_ = name;
  sub_type = sub_type_;
}

/* add new TEXT column, part of create table or Alter table */
Text_Column::Text_Column(std::string name, Table *table)
    : Column(table, Column::TEXT) {

  if (options->at(Option::NO_COLUMN_COMPRESSION)->getBool() == false &&
      rand_int(1) == 1)
    compressed = true;

  switch (rand_int(4, 1)) {
  case 1:
    sub_type = "TINYTEXT";
    name_ = "t" + name;
    length = rand_int(255, 100);
    break;
  case 2:
    sub_type = "TEXT";
    name_ = "t" + name;
    length = rand_int(1000, 500);
    break;
  case 3:
    sub_type = "MEDIUMTEXT";
    name_ = "mt" + name;
    length = rand_int(3000, 1000);
    break;
  case 4:
    sub_type = "LONGTEXT";
    name_ = "lt" + name;
    length = rand_int(4000, 2000);
    break;
  }
}

Text_Column::Text_Column(std::string name, Table *table, std::string sub_type_)
    : Column(table, Column::TEXT) {
  name_ = name;
  sub_type = sub_type_;
}

/* Constructor used for load metadata */
Generated_Column::Generated_Column(std::string name, Table *table,
                                   std::string clause, std::string sub_type)
    : Column(table, Column::GENERATED) {
  name_ = name;
  str = clause;
  g_type = Column::col_type(sub_type);
}

/* Generated column constructor. lock table before calling */
Generated_Column::Generated_Column(std::string name, Table *table)
    : Column(table, Column::GENERATED) {
  name_ = "g" + name;
  g_type = COLUMN_MAX;
  /* Generated columns are 4:2:2:1 (INT:VARCHAR:CHAR:BLOB) */
  // todojson, add if a table has index
  while (g_type == COLUMN_MAX) {
    auto x = rand_int(9, 1);
    if (x <= 4 && !options->at(Option::NO_INT)->getBool())
      g_type = INT;
    else if (x <= 6 && !options->at(Option::NO_VARCHAR)->getBool())
      g_type = VARCHAR;
    else if (x <= 8 && !options->at(Option::NO_CHAR)->getBool())
      g_type = CHAR;
    else if (x == 9 && !options->at(Option::NO_BLOB)->getBool()) {
      g_type = BLOB;
    } else if (x == 10 && !options->at(Option::NO_TEXT)->getBool()) {
      g_type = TEXT;
    } else {
      g_type = (rand_int(1) == 0 ? INT : VARCHAR);
    }
  }

  if (options->at(Option::NO_COLUMN_COMPRESSION)->getBool() == false &&
      rand_int(1) == 1 && (g_type == BLOB || g_type == TEXT))
    compressed = true;

  /*number of columns in generated columns */
  size_t columns = rand_int(.6 * table->columns_->size()) + 1;
  if (columns > 4)
    columns = 2;

  std::vector<size_t> col_pos; // position of columns

  while (col_pos.size() < columns) {
    size_t col = rand_int(table->columns_->size() - 1);
    if (!table->columns_->at(col)->auto_increment &&
        table->columns_->at(col)->type_ != GENERATED)
      col_pos.push_back(col);
  }

  if (g_type == INT || g_type == INTEGER) {
    auto sql_g_type = col_type_to_string(g_type);
    /* some expression fails */
    if (sql_g_type == "INT") {
      sql_g_type = "BIGINT";
    }

    str = " " + sql_g_type + " GENERATED ALWAYS AS (";
    for (auto pos : col_pos) {
      auto col = table->columns_->at(pos);
      if (col->type_ == VARCHAR || col->type_ == CHAR || col->type_ == BLOB ||
          col->type_ == TEXT || col->type_ == BIT || col->type_ == ENUM) {
        if (rand_int(2) == 1) {
          str += " CHAR_LENGTH(" + col->name_ + ")+";
        } else {
          str += " LENGTH(REPLACE(" + col->name_ + ",'A',''))+";
        }
      } else if (col->type_ == INT || col->type_ == INTEGER ||
                 col->type_ == BOOL || col->type_ == FLOAT ||
                 col->type_ == DOUBLE || col->type_ == DECIMAL) {
        if (rand_int(2) == 1) {
          str += " (" + col->name_ + "-" + "100)" + "+";
        } else {
          str += " " + col->name_ + "+";
        }
      } else if (col->type_ == DATE || col->type_ == DATETIME ||
                 col->type_ == TIMESTAMP) {
        str += " DATEDIFF('" + rand_date() + "'," + col->name_ + ")+";
      } else if (col->type_ == JSON) {
        // todojson, better approach
        str += " JSON_LENGTH(" + col->name_ + ")+";
      } else {
        print_and_log("unhandled " + col_type_to_string(col->type_) +
                      " at line " + std::to_string(__LINE__));
        exit(EXIT_FAILURE);
      }
    }
    str.pop_back();
  } else if (g_type == VARCHAR || g_type == CHAR || g_type == BLOB ||
             g_type == TEXT) {
    size_t generated_column_length;
    if (g_type == BLOB || g_type == TEXT) {
      generated_column_length = size_t(rand_int(5000, 5));
    } else {
      generated_column_length = size_t(rand_int(
          options->at(Option::VARCHAR_COLUMN_MAX_WIDTH)->getInt(), 10));
    }
    int actual_size = 0;
    std::string gen_sql;

    /* we try to randomly distribute */
    int max_size = generated_column_length / col_pos.size() * 2;
    if (max_size < 2) {
      max_size = 2;
    }
    for (auto pos : col_pos) {
      auto col = table->columns_->at(pos);
      int column_size = 0;
      /* base column */
      switch (col->type_) {
      case INT:
      case INTEGER:
      case FLOAT:
      case DECIMAL:
      case DOUBLE:
        column_size = 15; // max size of int
        break;
      case DATE:
      case DATETIME:
      case TIMESTAMP:
        column_size = 19;
        break;
      case BOOL:
        column_size = 2;
        break;
      case JSON:
        // todojson
        column_size = 30;
        break;
      case VARCHAR:
      case CHAR:
      case ENUM:
      case BLOB:
      case TEXT:
      case BIT:
        column_size = col->length;
        break;
      case COLUMN_MAX:
      case GENERATED:
        print_and_log("unhandled " + col_type_to_string(col->type_) +
                      " at line " + std::to_string(__LINE__));
        exit(EXIT_FAILURE);
        break;
      }
      auto current_size = rand_int(max_size, 2);

      if (column_size > current_size) {
        actual_size += current_size;
        if (col->type_ == BIT) {
          gen_sql = "lpad(bin(" + col->name_ + " >> (" +
                    std::to_string(column_size) + " - " +
                    std::to_string(current_size) + "))," +
                    std::to_string(current_size) + ",'0'),";
        } else if (col->type_ == JSON || col->type_ == ENUM ||
                   col->type_ == BLOB || col->type_ == TIMESTAMP) {
          // todojson decide on table type document etc
          gen_sql += "SUBSTRING(CAST(" + col->name_ + " AS CHAR),1," +
                     std::to_string(current_size) + "),";
        } else {
          gen_sql += "SUBSTRING(" + col->name_ + ",1," +
                     std::to_string(current_size) + "),";
        }

      } else {
        actual_size += column_size;
        if (col->type_ == BIT) {
          gen_sql = "lpad(bin(" + col->name_ + ")," +
                    std::to_string(column_size) + ",'0'),";
        } else if (col->type_ == JSON || col->type_ == ENUM ||
                   col->type_ == TIMESTAMP) {
          // todojson fix a better way
          gen_sql += "SUBSTRING(CAST(" + col->name_ + " AS CHAR),1," +
                     std::to_string(column_size) + "),";

        } else {
          gen_sql += col->name_ + ",";
        }
      }
    }
    gen_sql.pop_back();
    str = " " + col_type_to_string(g_type);
    if (g_type == VARCHAR || g_type == CHAR)
      str += "(" + std::to_string(actual_size) + ")";
    str += " GENERATED ALWAYS AS (CONCAT(";
    str += gen_sql;
    str += ")";
    length = actual_size;
    assert(length >= 2);

  } else {
    print_and_log("unhandled " + col_type_to_string(g_type) + " at line " +
                  std::to_string(__LINE__));
    exit(EXIT_FAILURE);
  }
  str += ")";

  if (rand_int(2) == 1 || compressed ||
      options->at(Option::SECONDARY_ENGINE)->getString() != "")
    str += " STORED";
}


/* used by generated,blob, text column */
Column::Column(Table *table, COLUMN_TYPES type) : type_(type), table_(table) {}

Index::~Index() {
  for (auto id_col : *columns_) {
    delete id_col;
  }
  delete columns_;
}


Ind_col::Ind_col(Column *c, bool d) : column(c), desc(d) {}

Index::Index(std::string n, bool u) : name_(n), columns_(), unique(u) {
  columns_ = new std::vector<Ind_col *>;
}

void Index::AddInternalColumn(Ind_col *column) { columns_->push_back(column); }

/* index definition */
std::string Index::definition() {
  std::string def;
  if (unique)
    def += "UNIQUE ";
  def += "INDEX " + name_ + "(";
  for (auto idc : *columns_) {
    def += idc->column->name_;
    /* if column is json create dummy index. todojson */

    /* blob columns should have prefix length */
    if (idc->column->type_ == Column::BLOB ||
        idc->column->type_ == Column::TEXT ||
        (idc->column->type_ == Column::GENERATED &&
         (static_cast<const Generated_Column *>(idc->column)->generate_type() ==
              Column::BLOB ||
          static_cast<const Generated_Column *>(idc->column)->generate_type() ==
              Column::TEXT)))
      def += "(" +
             std::to_string(rand_int(
                 options->at(Option::VARCHAR_COLUMN_MAX_WIDTH)->getInt(), 1)) +
             ")";

    def += (idc->desc ? " DESC" : (rand_int(3) ? "" : " ASC"));
    def += ", ";
  }
  def.erase(def.length() - 2);
  def += ") ";
  return def;
}

static void validate_secondary_engine(Thd1 *thd) {
  if (strcmp(FORK, "Duckdb") == 0) {
    return;
  }
  if (options->at(Option::SELECT_IN_SECONDARY)->getBool()) {
    execute_sql("SET @@SESSION.USE_SECONDARY_ENGINE=OFF", thd);
  }
  std::string table_exists =
      "select count(1) from INFORMATION_SCHEMA.TABLES where "
      "table_schema=\"performance_schema\" and table_name=\"" +
      lower_case_secondary() + "_table_sync_status\"";
  if (thd->db->get_single_value(table_exists) != "1") {
    throw std::runtime_error("Failed to find performance_schema table " +
                             lower_case_secondary() + "_table_sync_status");
  }
  if (options->at(Option::SELECT_IN_SECONDARY)->getBool()) {
    execute_sql("SET @@SESSION.USE_SECONDARY_ENGINE=FORCED", thd);
  }
}

void wait_till_sync(const std::string &name, Thd1 *thd) {

  if (options->at(Option::SELECT_IN_SECONDARY)->getBool()) {
    execute_sql("SET @@SESSION.USE_SECONDARY_ENGINE=OFF", thd);
  }

  std::string sql = "select count(1) from performance_schema." +
                    lower_case_secondary() +
                    "_table_sync_status where "
                    "table_schema=\"";
  sql += options->at(Option::DATABASE)->getString() + "\"";
  sql += " and table_name =\"" + name +
         "\" and SYNC_STATUS=\"SYNCING WITH CHANGE-STREAM\"";

  const int max_wait = 120;
  int counter = 0;

  while (true) {
    if (thd->db->get_single_value(sql) == "1") {
      break;
    }
    std::this_thread::sleep_for(std::chrono::seconds(5));
    if (counter > max_wait) {
      print_and_log("Table " + name + " not synced to secondary in 600 seconds",
                    thd);
    }
    counter++;
  }
  if (options->at(Option::SELECT_IN_SECONDARY)->getBool()) {
    execute_sql("SET @@SESSION.USE_SECONDARY_ENGINE=FORCED", thd);
  }
}

Table::Table(std::string n) : name_(n), indexes_() {
  columns_ = new std::vector<Column *>;
  indexes_ = new std::vector<Index *>;
}

bool Table::load_secondary_indexes(Thd1 *thd) {

  if (indexes_->size() == 0)
    return true;

  for (auto id : *indexes_) {
    if (id == indexes_->at(auto_inc_index))
      continue;
    std::string sql = "ALTER TABLE " + name_ + " ADD " + id->definition();
    if (!execute_sql(sql, thd)) {
      print_and_log("Failed to add index " + id->name_ + " on " + name_, thd,
                    true);
      run_query_failed = true;
      return false;
    }
  }

  return true;
}


bool FK_table::load_fk_constrain(Thd1 *thd, bool set_run_query_failed) {
  std::string constraint = name_ + "_" + std::to_string(rand_int(100));
  std::string sql = "ALTER TABLE " + name_ + " ADD  CONSTRAINT " + constraint +
                    fk_constrain();

  if (!execute_sql(sql, thd)) {
    print_and_log("Failed to add fk constraint on " + name_, thd, true);
    if (set_run_query_failed)
      run_query_failed = true;
    return false;
  }
  return true;
}

/* Constructor used by load_metadata */
Partition::Partition(std::string n, std::string part_type_, int number_of_part_)
    : Table(n), number_of_part(number_of_part_) {
  set_part_type(part_type_);
}

/* Constructor used by new Partiton table */
Partition::Partition(std::string n) : Table(n) {

  part_type = supported[rand_int(supported.size() - 1)];

  number_of_part = rand_int(options->at(Option::MAX_PARTITIONS)->getInt(), 2);

  /* randomly pick ranges for partition */
  if (part_type == RANGE) {
    auto unique_values = generateUniqueRandomNumbers(number_of_part);
    std::sort(unique_values.begin(), unique_values.end());
    for (int i = 0; i < number_of_part; i++) {
      positions.emplace_back("p" + std::to_string(i), unique_values.at(i));
    }

  } else if (part_type == LIST) {
    /* temporary vector to store all number_of_records */
    auto unique_values = generateUniqueRandomNumbers(
        number_of_part * maximum_records_in_each_parititon_list);

    int curr_unique_index = 0;
    for (int i = 0; i < number_of_part; i++) {
      auto number_of_records =
          rand_int(maximum_records_in_each_parititon_list, 1);

      lists.emplace_back("p" + std::to_string(i));

      for (int j = 0; j < number_of_records; j++) {
        lists.at(i).list.push_back(unique_values.at(curr_unique_index++));
      }
    }
  }
}

std::string FK_table::fk_constrain(bool add_fk) {
  std::string parent = name_.substr(0, name_.find("_", name_.find("_") + 1));
  if (add_fk)
    parent += "_fk";

  std::string sql =
      " FOREIGN KEY (ifk_col) REFERENCES " + parent + " (" + "ipkey" + ")";
  sql += " ON UPDATE " + enumToString(on_update);
  sql += " ON DELETE  " + enumToString(on_delete);
  return sql;
}

void Table::DropCreate(Thd1 *thd) {
  int nbo_prob = options->at(Option::DROP_WITH_NBO)->getInt();
  bool set_session_nbo = false;
  if (rand_int(100) < nbo_prob) {
    execute_sql("SET SESSION wsrep_osu_method=NBO ", thd);
    set_session_nbo = true;
  }
  if (!execute_sql("DROP TABLE " + name_, thd)) {
    return;
  }

  if (set_session_nbo) {
    execute_sql("SET SESSION wsrep_osu_method=DEFAULT ", thd);
  }
  std::string def = definition(true, true, true);
  if (!execute_sql(def, thd) && tablespace.size() > 0) {
    std::string tbs = " TABLESPACE=" + tablespace + "_rename";

    auto use_encryption = opt_bool(USE_ENCRYPTION);

    std::string encrypt_sql = " ENCRYPTION = " + encryption;

    /* If tablespace is rename or encrypted, or tablespace rename/encrypted */
    if (!execute_sql(def + tbs, thd))
      if (use_encryption && (execute_sql(def + encrypt_sql, thd) ||
                             execute_sql(def + encrypt_sql + tbs, thd))) {
        lock_table_mutex(thd->ddl_query);
        if (encryption.compare("Y") == 0)
          encryption = 'N';
        else if (encryption.compare("N") == 0)
          encryption = 'Y';
        unlock_table_mutex();
      }
  }
}

void Table::Optimize(Thd1 *thd) {
  if (type == PARTITION && rand_int(4) == 1) {
    lock_table_mutex(thd->ddl_query);
    int partition =
        rand_int(static_cast<Partition *>(this)->number_of_part - 1);
    unlock_table_mutex();
    execute_sql("ALTER TABLE " + name_ + " OPTIMIZE PARTITION p" +
                    std::to_string(partition),
                thd);
  } else
    execute_sql("OPTIMIZE TABLE " + name_, thd);
}

void Table::Check(Thd1 *thd) {
  if (type == PARTITION && rand_int(4) == 1) {
    lock_table_mutex(thd->ddl_query);
    int partition =
        rand_int(static_cast<Partition *>(this)->number_of_part - 1);
    unlock_table_mutex();
    get_check_result("ALTER TABLE " + name_ + " CHECK PARTITION p" +
                         std::to_string(partition),
                     thd);
  } else
    get_check_result("CHECK TABLE " + name_, thd);
}

void Table::Analyze(Thd1 *thd) {
  if (type == PARTITION && rand_int(4) == 1) {
    lock_table_mutex(thd->ddl_query);
    int partition =
        rand_int(static_cast<Partition *>(this)->number_of_part - 1);
    unlock_table_mutex();
    execute_sql("ALTER TABLE " + name_ + " ANALYZE PARTITION p" +
                    std::to_string(partition),
                thd);
  } else
    execute_sql("ANALYZE TABLE " + name_, thd);
}

void Table::Truncate(Thd1 *thd) {
  /* 99% truncate the some partition */
  if (type == PARTITION && rand_int(100) > 1) {
    auto sql = "ALTER TABLE " + name_ + " TRUNCATE ";
    if (rand_int(1000) == 1) {
      sql += "ALL";
    } else {
      lock_table_mutex(thd->ddl_query);
      sql += GetRandomPartition();
      unlock_table_mutex();
    }
    execute_sql(sql, thd);
  } else {
    execute_sql("TRUNCATE TABLE " + name_, thd);
  }
}

/* add or drop average 10% of max partitions */
void Partition::AddDrop(Thd1 *thd) {
  if (part_type == KEY || part_type == HASH) {
    int new_partition =
        rand_int(options->at(Option::MAX_PARTITIONS)->getInt()) / 10;
    if (new_partition == 0)
      new_partition = 1;

    if (rand_int(1) == 0) {
      if (execute_sql("ALTER TABLE " + name_ + " ADD PARTITION PARTITIONS " +
                          std::to_string(new_partition),
                      thd)) {
        lock_table_mutex(thd->ddl_query);
        number_of_part += new_partition;
        unlock_table_mutex();
      }
    } else {
      if (execute_sql("ALTER TABLE " + name_ + pick_algorithm_lock() +
                          ", COALESCE PARTITION " +
                          std::to_string(new_partition),
                      thd)) {
        lock_table_mutex(thd->ddl_query);
        number_of_part -= new_partition;
        unlock_table_mutex();
      }
    }
  } else if (part_type == RANGE) {
    /* drop partition, else add partition */
    if (rand_int(1) == 1) {
      lock_table_mutex(thd->ddl_query);
      auto par = positions.at(rand_int(positions.size() - 1));
      auto part_name = par.name;
      unlock_table_mutex();

      if (execute_sql("ALTER TABLE " + name_ + " DROP PARTITION " + part_name,
                      thd)) {
        lock_table_mutex(thd->ddl_query);
        number_of_part--;
        for (auto i = positions.begin(); i != positions.end(); i++) {
          if (i->name.compare(part_name) == 0) {
            positions.erase(i);
            break;
          }
        }
        unlock_table_mutex();
      }
    } else {
      /* add partition */
      auto new_part_name = "p" + std::to_string(rand_int(1000, 50));
      auto new_range = try_negative(rand_int(
          options->at(Option::UNIQUE_RANGE)->getFloat() * number_of_records));

      std::string sql = "ALTER TABLE " + name_ + " ADD PARTITION (PARTITION " +
                        new_part_name + " VALUES LESS THAN (" +
                        std::to_string(new_range) + "))";

      if (execute_sql(sql, thd)) {
        lock_table_mutex(thd->ddl_query);
        positions.emplace_back(new_part_name, new_range);
        number_of_part++;
        unlock_table_mutex();
      }
    }
  } else if (part_type == LIST) {

    /* drop partition or add partition */
    if (rand_int(1) == 0) {
      lock_table_mutex(thd->ddl_query);
      assert(lists.size() > 0);
      auto par = lists.at(rand_int(lists.size() - 1));
      auto part_name = par.name;
      unlock_table_mutex();
      if (execute_sql("ALTER TABLE " + name_ + " DROP PARTITION " + part_name,
                      thd)) {
        lock_table_mutex(thd->ddl_query);
        number_of_part--;
        for (auto i = lists.begin(); i != lists.end(); i++) {
          if (i->name.compare(part_name) == 0) {
            lists.erase(i);
            break;
          }
        }
        unlock_table_mutex();
      }

    } else {
      /* add partition */
      size_t number_of_records_in_partition =
          rand_int(number_of_records) /
          rand_int(options->at(Option::MAX_PARTITIONS)->getInt(), 1);

      if (number_of_records_in_partition == 0)
        number_of_records_in_partition = 1;

      std::vector<long int> temp_list;
      int iteration = rand_int(7, 1);
      for (int i = 0; i < iteration; i++) {
        temp_list.push_back(try_negative(
            rand_int(options->at(Option::UNIQUE_RANGE)->getFloat() *
                     number_of_records)));
      }
      std::string new_part_name = "p" + std::to_string(rand_int(1000, 100));
      std::string sql = "ALTER TABLE " + name_ + " ADD PARTITION (PARTITION " +
                        new_part_name + " VALUES IN (";
      for (size_t i = 0; i < temp_list.size(); i++) {
        sql += " " + std::to_string(temp_list.at(i));
        if (i != temp_list.size() - 1)
          sql += ",";
      }
      sql += "))";
      if (execute_sql(sql, thd)) {
        lock_table_mutex(thd->ddl_query);
        number_of_part++;
        lists.emplace_back(new_part_name);
        for (size_t i = 0; i < temp_list.size(); i++) {
          lists.at(lists.size() - 1).list.push_back(temp_list.at(i));
        }
        unlock_table_mutex();
      }
    }
  }
}

Table::~Table() {
  for (auto ind : *indexes_)
    delete ind;
  for (auto col : *columns_) {
    col->mutex.lock();
    delete col;
  }
  delete columns_;
  delete indexes_;
}

/* create default column */
void Table::CreateDefaultColumn() {
  auto no_auto_inc = opt_bool(NO_AUTO_INC);
  bool has_auto_increment = false;
  bool found_partition = false;
  bool table_will_have_pk =
      (rand_int(100, 1) <= options->at(Option::PRIMARY_KEY)->getInt());
  bool table_will_have_int_pk =
      table_will_have_pk &&
      (rand_int(100) >= options->at(Option::NON_INT_PK)->getInt());

  /* first column for fk and partition */
  if (type == Table::FK) {
    AddInternalColumn(new Column{"fk_col", this, Column::INT});
  }

  if (type == Table::PARTITION) {
    std::string name = "p_col";
    Column *col;
    col = new Column{"p_col", this, Column::INT};
    auto curr_part_type = static_cast<Partition *>(this)->part_type;
    if (!table_will_have_int_pk or curr_part_type == Partition::LIST or
        curr_part_type == Partition::RANGE or rand_int(2) == 1) {
      col->is_partition = true;
      found_partition = true;
    }
    col->null_val = false;

    /* for list and range partition the column should not be auto inc  */
    /* if secondary_engine is not null we restrict to add auto inc column as
     * might be added by invisible unique key */
    if (curr_part_type != Partition::LIST and
        curr_part_type != Partition::RANGE and
        options->at(Option::SECONDARY_ENGINE)->getString() == "" and
        !options->at(Option::NO_AUTO_INC)->getBool() && rand_int(3) == 0) {
      col->auto_increment = true;
      has_auto_increment = true;
    }

    AddInternalColumn(col);
  }

  int max_columns = options->at(Option::RANDOM_COLUMNS)->getBool()
                        ? rand_int(options->at(Option::COLUMNS)->getInt(), 1)
                        : options->at(Option::COLUMNS)->getInt();

  if (type == Table::FK || type == Table::PARTITION) {
    max_columns -= 1;
  }
  if (max_columns == 0 && table_will_have_pk) {
    max_columns = 1;
  }

  for (int i = 0; i < max_columns; i++) {
    std::string name;
    Column *col;

    /* First try to create primary key */
    if (i == 0 && table_will_have_pk) {
      col = new Column{"pkey", this,
                       table_will_have_int_pk ? Column::INT : Column::VARCHAR};
      col->primary_key = true;
      col->null_val = false;

      /* in mysql for partition table there can be only one auto column and it
       * must be defined as a key */
      if (type != Table::PARTITION && col->type_ == Column::INT &&
          rand_int(100) < options->at(Option::PK_COLUMN_AUTOINC)->getInt() &&
          options->at(Option::NO_AUTO_INC)->getBool() == false) {
        col->auto_increment = true;
        col->null_val = true;
        has_auto_increment = true;
      }
      if (type == Table::PARTITION) {
        auto part_table = static_cast<Partition *>(this);
        if (!found_partition and part_table->part_type != Partition::LIST) {
          col->is_partition = true;
        } else {
        }
      }

    } else {
      name = std::to_string(i);
      Column::COLUMN_TYPES col_type = Column::COLUMN_MAX;

      /* loop until we select some column */
      while (col_type == Column::COLUMN_MAX) {

        auto prob = rand_int(26);

        /* intial columns can't be generated columns. also 50% of tables last
         * columns are virtuals */
        if (!options->at(Option::NO_VIRTUAL_COLUMNS)->getBool() &&
            i >= (.8 * max_columns) && rand_int(1) == 1)
          col_type = Column::GENERATED;
        else if ((!options->at(Option::NO_INT)->getBool() && prob < 5))
          col_type = Column::INT;
        else if (!options->at(Option::NO_INTEGER)->getBool() && prob < 6)
          col_type = Column::INTEGER;
        else if (!options->at(Option::NO_FLOAT)->getBool() && prob < 8)
          col_type = Column::FLOAT;
        else if (!options->at(Option::NO_DOUBLE)->getBool() && prob < 10)
          col_type = Column::DOUBLE;
        else if (!options->at(Option::NO_VARCHAR)->getBool() && prob >= 10 &&
                 prob < 14)
          col_type = Column::VARCHAR;
        else if (!options->at(Option::NO_CHAR)->getBool() && prob >= 14 &&
                 prob < 16)
          col_type = Column::CHAR;
        else if (!options->at(Option::NO_TEXT)->getBool() && prob == 17)
          col_type = Column::TEXT;
        else if (!options->at(Option::NO_BLOB)->getBool() && prob == 18)
          col_type = Column::BLOB;
        else if (!options->at(Option::NO_BOOL)->getBool() && prob == 19)
          col_type = Column::BOOL;
        else if (prob == 20 && !options->at(Option::NO_DATE)->getBool())
          col_type = Column::DATE;
        else if (prob == 21 && !options->at(Option::NO_DATETIME)->getBool())
          col_type = Column::DATETIME;
        else if (prob == 22 && !options->at(Option::NO_TIMESTAMP)->getBool())
          col_type = Column::TIMESTAMP;
        else if (prob == 23 && !options->at(Option::NO_BIT)->getBool())
          col_type = Column::BIT;
        else if (prob == 24 && !options->at(Option::NO_JSON)->getBool())
          col_type = Column::JSON;
        else if (prob == 25 && !options->at(Option::NO_ENUM)->getBool())
          col_type = Column::ENUM;
        else if (prob == 26 && !options->at(Option::NO_DECIMAL)->getBool())
          col_type = Column::DECIMAL;
      }

      if (col_type == Column::GENERATED)
        col = new Generated_Column(name, this);
      else if (col_type == Column::BLOB)
        col = new Blob_Column(name, this);
      else if (col_type == Column::TEXT)
        col = new Text_Column(name, this);
      else if (col_type == Column::ENUM)
        col = new Enum_Column(name, this);
      else if (col_type == Column::DECIMAL)
        col = new Decimal_Column(name, this);
      else
        col = new Column(name, this, col_type);

      /* set not secondary clause */
      if (options->at(Option::NOT_SECONDARY)->getInt() > rand_int(100)) {
        col->not_secondary = true;
      }

      /*For key partition we add few more columns */
      if (col->type_ != Column::JSON && col->type_ != Column::BLOB &&
          col->type_ != Column::GENERATED && col->type_ != Column::TEXT &&
          type == Table::PARTITION &&
          static_cast<Partition *>(this)->part_type == Partition::KEY &&
          rand_int(3) == 1) {
        col->is_partition = true;
        col->null_val = false;
        col->not_secondary = false;
      }

      /* 25% column can have auto_inc */
      if (options->at(Option::SECONDARY_ENGINE)->getString() == "" &&
          type != Table::PARTITION && col->type_ == Column::INT &&
          !no_auto_inc && has_auto_increment == false && rand_int(100) > 25) {
        col->auto_increment = true;
        has_auto_increment = true;
      }
      if (rand_int(100, 1) < 30 && col->type_ != Column::GENERATED &&
          this->type != TABLE_TYPES::FK) {
        col->null_val = false;
      }
    }
    AddInternalColumn(col);
  }
  }

/* create default indexes */
void Table::CreateDefaultIndex() {

  int number_of_compressed = 0;
  int number_of_json_columns = 0;

  for (auto column : *columns_) {
    if (column->compressed)
      number_of_compressed++;
    if (column->type_ == Column::JSON)
      number_of_json_columns++;
  }

  /* create a todojson index */
  size_t number_of_valid_columns =
      columns_->size() - number_of_compressed - number_of_json_columns;

  /* if no number of columns = 0 return */
  if (number_of_valid_columns == 0)
    return;

  /* if table have few column, decrease number of indexes */
  size_t indexes = 0;
  size_t max_indexes = opt_int(INDEXES);

  if (rand_int(100, 1) <= options->at(Option::INDEXES_PROB)->getInt())
    indexes =
        rand_int(number_of_valid_columns < max_indexes ? number_of_valid_columns
                                                       : max_indexes,
                 0);
  else
    indexes = 0;
  /* for auto-inc columns handling, we need to add auto_inc as first column */
  int auto_inc_pos = -1; // auto_inc_column_position
  for (size_t i = 0; i < columns_->size(); i++) {
    if (columns_->at(i)->auto_increment && columns_->at(i)->name_ != "ipkey" &&
        columns_->at(i)->name_ != "vpkey") {
      auto_inc_pos = i;
    }
  }

  if (indexes == 0) {
    if (auto_inc_pos != -1)
      indexes = 1;
    else
      return;
  }

  /*which index will have auto_inc column */
  auto_inc_index = rand_int(indexes - 1, 0);

  for (size_t i = 0; i < indexes; i++) {
    Index *id = new Index(name_ + "i" + std::to_string(i));

    static size_t max_columns = opt_int(INDEX_COLUMNS);

    size_t number_of_columns = rand_int((max_columns < number_of_valid_columns
                                             ? max_columns
                                             : number_of_valid_columns),
                                        1);

    std::vector<int> col_pos; // position of columns

    /* pick some columns */
    while (col_pos.size() < number_of_columns) {
      int current = rand_int(columns_->size() - 1);
      if (columns_->at(current)->compressed ||
          columns_->at(current)->type_ == Column::JSON)
        continue;
      /* auto-inc column should be first column in auto_inc_index */
      if (auto_inc_pos != -1 && i == auto_inc_index && col_pos.size() == 0)
        col_pos.push_back(auto_inc_pos);
      else {
        bool already_added = false;
        for (auto id : col_pos) {
          if (id == current)
            already_added = true;
        }
        if (!already_added)
          col_pos.push_back(current);
      }
    } // while

    auto index_has_int_col = [&col_pos, this]() {
      for (auto pos : col_pos) {
        if (columns_->at(pos)->type_ == Column::INT)
          return true;
      }
      return false;
    };

    if (index_has_int_col() &&
        rand_int(1000) < options->at(Option::UNIQUE_INDEX_PROB_K)->getInt()) {
      id->unique = true;
    }

    for (auto pos : col_pos) {
      auto col = columns_->at(pos);
      static bool no_desc_support = opt_bool(NO_DESC_INDEX);
      bool column_desc = false;
      if (!no_desc_support) {
        column_desc = rand_int(100) < DESC_INDEXES_IN_COLUMN
                          ? true
                          : false; // 33 % are desc //
      }
      id->AddInternalColumn(
          new Ind_col(col, column_desc)); // desc is set as true
    }
    AddInternalIndex(id);
  }
}

/* Create new table and pick some attributes */
Table *Table::table_id(TABLE_TYPES type, int id, bool suffix) {
  Table *table;
  std::string name = TABLE_PREFIX + std::to_string(id);
  if (suffix) {
    name += "_" + std::to_string(rand_int(1000000));
  }

  switch (type) {
  case PARTITION:
    table = new Partition(name + PARTITION_SUFFIX);
    break;
  case NORMAL:
    table = new Table(name);
    break;
  case TEMPORARY:
    table = new Temporary_table(name + TEMP_SUFFIX);
    break;
  case FK:
    table = new FK_table(name + FK_SUFFIX);
    break;
  default:
    print_and_log("Invalid table type");
    exit(EXIT_FAILURE);
  }

  table->type = type;

  table->number_of_initial_records =
      options->at(Option::RANDOM_INITIAL_RECORDS)->getBool()
          ? rand_int(number_of_records)
          : number_of_records;
  static auto use_encryption = opt_bool(USE_ENCRYPTION);

  /* temporary table on 8.0 can't have key block size */
  if (type != TEMPORARY) {
    if (g_key_block_size.size() > 0)
      table->key_block_size =
          g_key_block_size[rand_int(g_key_block_size.size() - 1)];

    if (table->key_block_size > 0 && rand_int(2) == 0) {
      table->row_format = "COMPRESSED";
    }

    if (table->key_block_size == 0 && g_row_format.size() > 0)
      table->row_format = g_row_format[rand_int(g_row_format.size() - 1)];
  }

  /* with more number of tablespace there are more chances to have table in
   * tablespaces */
  static int tbs_count = opt_int(NUMBER_OF_GENERAL_TABLESPACE);

  /* partition and temporary tables don't have tablespaces */
  if (table->type == PARTITION && use_encryption) {
    table->encryption = g_encryption[rand_int(g_encryption.size() - 1)];
  } else if (table->type != TEMPORARY && use_encryption) {
    int rand_index = rand_int(g_encryption.size() - 1);
    if (g_encryption.at(rand_index) == "Y" ||
        g_encryption.at(rand_index) == "N") {
      if (g_tablespace.size() > 0 && rand_int(tbs_count) != 0) {
        table->tablespace = g_tablespace[rand_int(g_tablespace.size() - 1)];
        if (table->tablespace.substr(table->tablespace.size() - 2, 2)
                .compare("_e") == 0)
          table->encryption = "Y";
        table->row_format.clear();
        if (g_innodb_page_size > INNODB_16K_PAGE_SIZE ||
            table->tablespace.compare("innodb_system") == 0 ||
            stoi(table->tablespace.substr(3, 2)) == g_innodb_page_size)
          table->key_block_size = 0;
        else
          table->key_block_size = std::stoi(table->tablespace.substr(3, 2));
      }
    } else
      table->encryption = g_encryption.at(rand_index);
  }

  if (encrypted_temp_tables && table->type == TEMPORARY)
    table->encryption = 'Y';

  if (encrypted_sys_tablelspaces &&
      table->tablespace.compare("innodb_system") == 0) {
    table->encryption = 'Y';
  }

  /* 25 % tables are compress */
  if (table->type != TEMPORARY && table->tablespace.empty() and
      rand_int(3) == 1 && g_compression.size() > 0) {
    table->compression = g_compression[rand_int(g_compression.size() - 1)];
    table->row_format.clear();
    table->key_block_size = 0;
  }

  static auto engine = options->at(Option::ENGINE)->getString();
  table->engine = engine;

  table->CreateDefaultColumn();
  table->CreateDefaultIndex();
  if (type == FK) {
    static_cast<FK_table *>(table)->pickRefrence(table);
  }

  return table;
}

/* check if table has a primary key */
bool Table::has_int_pk() const {
  for (const auto &col : *columns_) {
    if (col->primary_key && col->type_ == Column::INT)
      return true;
  }
  return false;
}
bool Table::has_pk() const {
  for (const auto &col : *columns_) {
    if (col->primary_key)
      return true;
  }
  return false;
}

bool Table::has_auto_inc_col() const {
  for (const auto &col : *columns_) {
    if (col->auto_increment)
      return true;
  }
  return false;
}

/* prepare table definition */
std::string Table::definition(bool with_index, bool with_fk,
                              bool with_forced_secondary) {
  std::string def = "CREATE";
  if (type == TEMPORARY)
    def += " TEMPORARY";
  def += " TABLE " + name_ + " (";

  assert(columns_->size() > 0);

  /* add columns */
  for (auto col : *columns_) {
    def += col->definition() + ", ";
  }


  /* if column has primary key */
  if (has_pk()) {
    def += " PRIMARY KEY(";
    auto table_has_auto_inc = has_auto_inc_col();
    for (auto col : *columns_) {
      if (col->primary_key)
        def += col->name_ + ", ";
    }
    for (auto col : *columns_) {
      if (col->primary_key)
        continue;
      else if (col->primary_key || col->is_partition ||
               ((options->at(Option::SECONDARY_ENGINE)->getString() == "" ||
                 table_has_auto_inc == false) &&
                col->type_ != Column::BLOB && col->type_ != Column::JSON &&
                col->type_ != Column::TEXT &&
                rand_int(100) <=
                    options->at(Option::COMPOSITE_KEY_PROB)->getInt() &&
                col->null_val == false)) {
        def += col->name_ + ", ";
      }
    }
    def.erase(def.length() - 2);
    def += "), ";
  }

  if (with_index) {
    if (indexes_->size() > 0) {
      for (auto id : *indexes_) {
        def += id->definition() + ", ";
      }
    }
  } else {
    /* only load autoinc */
    if (indexes_->size() > 0) {
      def += indexes_->at(auto_inc_index)->definition() + ", ";
    }
  }

  if (with_fk) {
    if (type == FK) {
      auto fk = static_cast<FK_table *>(this);
      def += fk->fk_constrain(true) + ", ";
    }
  }

  def.erase(def.length() - 2);

  def += ")";
  static auto use_encryption = opt_bool(USE_ENCRYPTION);
  bool keyring_key_encrypt_flag = 0;

  if (use_encryption && type != TEMPORARY) {
    if (encryption == "Y" || encryption == "N")
      def += " ENCRYPTION='" + encryption + "'";
    else if (encryption == "KEYRING") {
      keyring_key_encrypt_flag = 1;
      switch (rand_int(2)) {
      case 0:
        def += " ENCRYPTION='KEYRING'";
        break;
      case 1:
        def += " ENCRYPTION_KEY_ID=" + std::to_string(rand_int(9));
        break;
      case 2:
        def += " ENCRYPTION='KEYRING' ENCRYPTION_KEY_ID=" +
               std::to_string(rand_int(9));
        break;
      }
    }
  }

  if (!compression.empty())
    def += " COMPRESSION='" + compression + "'";

  if (!tablespace.empty() && !keyring_key_encrypt_flag)
    def += " TABLESPACE=" + tablespace;

  if (key_block_size > 1)
    def += " KEY_BLOCK_SIZE=" + std::to_string(key_block_size);

  if (row_format.size() > 0)
    def += " ROW_FORMAT=" + row_format;

  if (!engine.empty())
    def += " ENGINE=" + engine;

#ifdef USE_CLICKHOUSE
  if (!engine.empty() && !columns_->empty()) {
    /* ORDER BY must include all primary key columns (PK must be a prefix) */
    std::string order_cols;
    for (const auto &col : *columns_) {
      if (col->primary_key) {
        if (!order_cols.empty()) order_cols += ", ";
        order_cols += col->name_;
      }
    }
    if (order_cols.empty())
      order_cols = columns_->at(0)->name_;
    def += " ORDER BY (" + order_cols + ")"
           " SETTINGS enable_block_number_column = 1,"
           " enable_block_offset_column = 1";
  }
#endif

  if (options->at(Option::SECONDARY_ENGINE)->getString().size() > 0 &&
      (!options->at(Option::SECONDARY_AFTER_CREATE)->getBool() ||
       with_forced_secondary)) {
    def += ", SECONDARY_ENGINE=" +
           options->at(Option::SECONDARY_ENGINE)->getString();
  }

  if (type == PARTITION) {
    auto par = static_cast<Partition *>(this);
    def += " PARTITION BY " + par->get_part_type() + " (" +
           par->get_part_column() + ")";
    switch (par->part_type) {
    case Partition::HASH:
    case Partition::KEY:
      def += " PARTITIONS " + std::to_string(par->number_of_part);
      break;
    case Partition::RANGE:
      def += "(";
      for (size_t i = 0; i < par->positions.size(); i++) {
        std::string range = std::to_string(par->positions[i].range);
        def += "PARTITION p" + std::to_string(i) + " VALUES LESS THAN (" +
               range + "), ";
      }
      def.pop_back();
      def.pop_back();
      def += ")";
      break;
    case Partition::LIST:
      def += "(";
      for (size_t i = 0; i < par->lists.size(); i++) {
        def += " PARTITION " + par->lists.at(i).name + " VALUES IN (";
        auto list = par->lists.at(i).list;
        for (size_t j = 0; j < list.size(); j++) {
          def += std::to_string(list.at(j));
          if (j == list.size() - 1)
            def += ")";
          else
            def += ",";
        }
        if (i == par->lists.size() - 1)
          def += ")";
        else
          def += ",";
      }
      break;
    }
  }
  return def;
}

/* create default table includes all tables*/
void generate_metadata_for_tables() {

  if (options->at(Option::ONLY_TEMPORARY)->getBool())
    return;

  for (int i = 1; i <= options->at(Option::TABLES)->getInt(); i++) {

    if (options->at(Option::ONLY_PARTITION)->getBool() ||
        options->at(Option::PARTITION_PROB)->getInt() > rand_int(100))
      all_tables->push_back(Table::table_id(Table::PARTITION, i));

    if (options->at(Option::ONLY_PARTITION)->getBool())
      continue;

    auto parent_table = Table::table_id(Table::NORMAL, i);
    all_tables->push_back(parent_table);

    /* Create FK table */
    if (options->at(Option::FK_PROB)->getInt() > rand_int(100) &&
        parent_table->has_int_pk()) {
      auto child_table = Table::table_id(Table::FK, i);
      all_tables->push_back(child_table);
    }
  }
}

void Table::Compare_between_engine(const std::string &sql, Thd1 *thd) {

  /* Lock the mutex if other thread is doing DML on this table */
  auto lock = [this]() {
    if (options->at(Option::ONLY_SELECT)->getBool())
      return;
    dml_mutex.lock();
  };
  auto unlock = [this]() {
    if (options->at(Option::ONLY_SELECT)->getBool())
      return;
    dml_mutex.unlock();
  };

  auto set_default = [thd]() {
    if (options->at(Option::SECONDARY_ENGINE)->getString() == "")
      return;
    if (options->at(Option::SELECT_IN_SECONDARY)->getBool()) {
      execute_sql("SET @@SESSION.USE_SECONDARY_ENGINE=FORCED", thd);
    } else {
      execute_sql("SET @@SESSION.USE_SECONDARY_ENGINE=DEFAULT ", thd);
    }
    if (options->at(Option::DELAY_IN_SECONDARY)->getInt() > 0) {
      execute_sql("SET @@SESSION." + lower_case_secondary() +
                      "_sleep_after_gtid_lookup_ms=DEFAULT",
                  thd);
    }
  };

  lock();

  /* Get result without forced */
  if (options->at(Option::SECONDARY_ENGINE)->getString() != "") {
    execute_sql("COMMIT", thd);
    thd->trx_left = 0;
    execute_sql("SET @@SESSION.USE_SECONDARY_ENGINE=OFF", thd);
  }

  {
    execute_sql("SET @@SESSION.USE_SECONDARY_ENGINE=FORCED ", thd);
    execute_sql(sql, thd);
    execute_sql("SET @@SESSION.USE_SECONDARY_ENGINE=OFF ", thd);
  }

  if (!execute_sql(sql, thd)) {
    print_and_log("Failed in MySQL:" + sql, thd, true);
    unlock();
    return set_default();
  }
  auto res_without_forced = thd->db->get_result();

  if (options->at(Option::SECONDARY_ENGINE)->getString() != "")
    execute_sql("SET @@SESSION.USE_SECONDARY_ENGINE=FORCED ", thd);


  /* unlock the table so other thread can execute the DML */
  if (options->at(Option::DELAY_IN_SECONDARY)->getInt() > 0) {
    int delay = rand_int(options->at(Option::DELAY_IN_SECONDARY)->getInt());
    execute_sql("SET @@SESSION." + lower_case_secondary() +
                    "_sleep_after_gtid_lookup_ms=" + std::to_string(delay),
                thd);
  }
  query_result res_with_forced;
  bool failed_in_secondary = true;
  unlock();
  if (!execute_sql(sql, thd)) {
    print_and_log("Failed in Secondary:" + sql, thd, true);
    return set_default();
  } else {
    failed_in_secondary = false;
    res_with_forced = thd->db->get_result();
  }

  /*
  auto run_sql = [sql, &thd, &res_with_forced, set_default,
                  &failed_in_secondary]() {
    if (!execute_sql(sql, thd)) {
      print_and_log("Failed in Secondary:" + sql, thd, true);
    } else {
      failed_in_secondary = false;
      res_with_forced = thd->db->get_result();
    }
  };
  std::thread run(run_sql);
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  unlock();
  run.join();
  */

  if (failed_in_secondary) {
    return set_default();
  }
  if (!compare_query_result(
          res_with_forced, res_without_forced, thd,
          options->at(Option::COMPARE_CASE_INSENSTIVE)->getBool())) {
    print_and_log("result set mismatch for " + sql, thd);
    exit(EXIT_FAILURE);
  }

  set_default();
}

bool execute_sql(const std::string &sql, Thd1 *thd, bool force_sql_log_query) {
  static auto log_all = opt_bool(LOG_ALL_QUERIES);
  static auto log_success = opt_bool(LOG_SUCCEDED_QUERIES);
  static auto log_query_duration = opt_bool(LOG_QUERY_DURATION);
  /*tododb
  static auto log_client_output = opt_bool(LOG_CLIENT_OUTPUT);
  */
  std::chrono::system_clock::time_point begin, end;
  if (log_query_duration) {
    begin = std::chrono::system_clock::now();
  }

  auto res = thd->db->execute_query(sql);

  if (log_query_duration) {
    end = std::chrono::system_clock::now();

    /* elpased time in micro-seconds */
    auto te_start = std::chrono::duration_cast<std::chrono::microseconds>(
        begin - start_time);
    auto te_query =
        std::chrono::duration_cast<std::chrono::microseconds>(end - begin);
    auto in_time_t = std::chrono::system_clock::to_time_t(begin);

    std::stringstream ss;
    ss << std::put_time(std::localtime(&in_time_t), "%Y-%m-%dT%X");

    thd->thread_log << ss.str() << " " << te_start.count() << "=>"
                    << te_query.count() << "ms ";
  }
  thd->performed_queries_total++;

  if (!res) {
    thd->success = false;
    thd->failed_queries_total++;
    thd->max_con_fail_count++;
    thd->thread_log << "F " << sql << std::endl;
    thd->thread_log << "Error " << thd->db->get_error() << std::endl;
    thd->query_buffer.push("F " + sql);

    static std::set<int> mysql_ignore_error =
        splitStringToIntSet(options->at(Option::IGNORE_ERRORS)->getString());

    auto error_number = thd->db->get_error_number();

    if (options->at(Option::IGNORE_ERRORS)->getString() == "all" ||
        mysql_ignore_error.count(error_number)) {
      if (error_number == CR_SERVER_GONE_ERROR ||
          error_number == CR_SERVER_LOST) {
        thd->thread_log << "Ignoring error " << thd->db->get_error()
                        << std::endl;
        if (!thd->tryreconnet()) {
          run_query_failed = true;
        }
      }
    }
    if (error_number == CR_SERVER_LOST ||
        error_number == CR_WSREP_NOT_PREPARED ||
        error_number == CR_SERVER_GONE_ERROR ||
        error_number == CR_SECONDARY_NOT_READY) {
      print_and_log("Fatal: " + sql.substr(0, 200), thd, true);
      run_query_failed = true;
    }
  } else {
    thd->max_con_fail_count = 0;
    thd->success = true;

    /*tododb
    if (log_client_output && log_result_client) {
      if (thd->result != nullptr) {
        unsigned int i, num_fields;

        num_fields = mysql_num_fields(thd->result.get());
        while (auto row = mysql_fetch_row_safe(thd)) {
          for (i = 0; i < num_fields; i++) {
            if (row[i]) {
              if (strlen(row[i]) == 0) {
                thd->client_log << "EMPTY"
                                << "#";
              } else {
                thd->client_log << row[i] << "#";
              }
            } else {
              thd->client_log << "#NO DATA"
                              << "#";
            }
          }
          if (log_query_numbers) {
            thd->client_log << ++thd->query_number;
          }
          thd->client_log << '\n';
        }
      }
    } */
    if ((force_sql_log_query && log_all) || log_success) {
      thd->thread_log << "S " << sql
                      << ", rows:" << thd->db->get_affected_rows() << std::endl;
    } else {
      /*we push to buffer only if all queries are not recorded */
      thd->query_buffer.push("S " + sql);
    }
  }

  if (thd->ddl_query) {
    std::lock_guard<std::mutex> lock(ddl_logs_write);
    thd->ddl_logs << thd->thread_id << " " << sql << " " << thd->db->get_error()
                  << std::endl;
  }

  return res;
}


void Table::EnforceRebuildInSecondary(Thd1 *thd) {
  std::string sql = " SET GLOBAL " +
                    options->at(Option::SECONDARY_ENGINE)->getString() +
                    " PRAGMA = \"rewrite_table(" +
                    options->at(Option::DATABASE)->getString() + "." + name_;

  if (rand_int(100) < options->at(Option::SECOND_LEVEL_MERGE)->getInt()) {
    sql += ",second_level_merge='true'";
  }
  sql += ")\"";
  execute_sql(sql, thd);
}

void Table::SetEncryption(Thd1 *thd) {
  std::string sql = "ALTER TABLE " + name_ + " ENCRYPTION = '";
  if (g_encryption.size() == 0)
    return;
  std::string enc = g_encryption[rand_int(g_encryption.size() - 1)];
  sql += enc + "'";
  if (execute_sql(sql, thd)) {
    lock_table_mutex(thd->ddl_query);
    encryption = enc;
    unlock_table_mutex();
  }
}

void Table::SetTableCompression(Thd1 *thd) {
  std::string sql = "ALTER TABLE " + name_ + " COMPRESSION= '";
  if (g_compression.size() == 0)
    return;
  std::string comp = g_compression[rand_int(g_compression.size() - 1)];
  sql += comp + "'";
  if (execute_sql(sql, thd)) {
    lock_table_mutex(thd->ddl_query);
    compression = comp;
    unlock_table_mutex();
  }
}

void Table::ModifyColumn(Thd1 *thd) {
  std::string sql = "ALTER TABLE " + name_ + " MODIFY COLUMN ";
  Column *col = nullptr;
  /* store old value */
  int length = 0;
  std::string default_value;
  bool auto_increment = false;
  bool compressed = false; // percona type compressed

  // try maximum 50 times to get a valid column
  int i = 0;
  while (i < 50 && col == nullptr) {
    auto col1 = columns_->at(rand_int(columns_->size() - 1));
    switch (col1->type_) {
    case Column::BLOB:
    case Column::GENERATED:
    case Column::VARCHAR:
    case Column::CHAR:
    case Column::FLOAT:
    case Column::DOUBLE:
    case Column::INT:
    case Column::INTEGER:
    case Column::DATE:
    case Column::DATETIME:
    case Column::TIMESTAMP:
    case Column::BIT:
    case Column::ENUM:
    case Column::TEXT:
    case Column::DECIMAL:
      col = col1;
      length = col->length;
      auto_increment = col->auto_increment;
      compressed = col->compressed;
      col->mutex.lock(); // lock column so no one can modify it //
      break;
    /* nothing can be done for json and bool */
    case Column::BOOL:
    case Column::JSON:
    case Column::COLUMN_MAX:
      break;
    }
    i++;
  }

  /* could not find a valid column to process */
  if (col == nullptr)
    return;

  if (col->length != 0) {
    if (col->type_ != Column::DECIMAL)
      col->length =
          rand_int(options->at(Option::VARCHAR_COLUMN_MAX_WIDTH)->getInt(), 5);
    else {
      col->length = rand_int(10, 4);
    }
  }

  if (col->auto_increment == true and rand_int(5) == 0)
    col->auto_increment = false;

  if (col->compressed == true and rand_int(4) == 0)
    col->compressed = false;
  else if (options->at(Option::NO_COLUMN_COMPRESSION)->getBool() == false &&
           (col->type_ == Column::BLOB || col->type_ == Column::GENERATED ||
            col->type_ == Column::VARCHAR || col->type_ == Column::TEXT))
    col->compressed = true;
  else if (col->not_secondary == true and rand_int(3) == 0)
    col->not_secondary = false;

#ifdef USE_CLICKHOUSE
  /* Use Nullable() wrapper for nullable columns, matching CREATE TABLE DDL */
  std::string ch_type = col->null_val ? "Nullable(" + col->clause() + ")" : col->clause();
  sql += " " + col->name_ + " " + ch_type;
#else
  sql += " " + col->definition() + pick_algorithm_lock();
#endif

  /* if not successful rollback */
  if (!execute_sql(sql, thd)) {
    col->length = length;
    col->auto_increment = auto_increment;
    col->compressed = compressed;
  } else {
    if (col->type_ == Column::DECIMAL) {
      static_cast<Decimal_Column *>(col)->update_max_precision();
    }
  }

  col->mutex.unlock();
}

/* alter table drop column */
void Table::DropColumn(Thd1 *thd) {
  lock_table_mutex(thd->ddl_query);

  /* do not drop last column */
  if (columns_->size() == 1) {
    unlock_table_mutex();
    return;
  }
  auto ps = rand_int(columns_->size() - 1); // position

  auto name = columns_->at(ps)->name_;

  if (rand_int(100, 1) <= options->at(Option::PRIMARY_KEY)->getInt() &&
      name.find("pkey") != std::string::npos) {
    unlock_table_mutex();
    return;
  }

  std::string sql = "ALTER TABLE " + name_ + " DROP COLUMN " + name;

  sql += pick_algorithm_lock();
  unlock_table_mutex();

  if (execute_sql(sql, thd)) {
    lock_table_mutex(thd->ddl_query);

    std::vector<int> indexes_to_drop;
    for (auto id = indexes_->begin(); id != indexes_->end(); id++) {
      auto index = *id;

      for (auto id_col = index->columns_->begin();
           id_col != index->columns_->end(); id_col++) {
        auto ic = *id_col;
        if (ic->column->name_.compare(name) == 0) {
          if (index->columns_->size() == 1) {
            delete index;
            indexes_to_drop.push_back(id - indexes_->begin());
          } else {
            delete ic;
            index->columns_->erase(id_col);
          }
          break;
        }
      }
    }
    std::sort(indexes_to_drop.begin(), indexes_to_drop.end(),
              std::greater<int>());

    for (auto &i : indexes_to_drop) {
      indexes_->at(i) = indexes_->back();
      indexes_->pop_back();
    }
    // table->indexes_->erase(id);

    for (auto pos = columns_->begin(); pos != columns_->end(); pos++) {
      auto col = *pos;
      if (col->name_.compare(name) == 0) {
        col->mutex.lock();
        delete col;
        columns_->erase(pos);
        break;
      }
    }
    unlock_table_mutex();
  }
}

/* alter table add random column */
void Table::AddColumn(Thd1 *thd) {

  std::string sql = "ALTER TABLE " + name_ + " ADD COLUMN ";

  Column::COLUMN_TYPES col_type = Column::COLUMN_MAX;

  auto use_virtual = true;

  // lock table to create definition
  lock_table_mutex(thd->ddl_query);

  if (options->at(Option::NO_VIRTUAL_COLUMNS)->getBool() ||
      (columns_->size() == 1 && columns_->at(0)->auto_increment == true)) {
    use_virtual = false;
  }
  while (col_type == Column::COLUMN_MAX) {
    auto prob = rand_int(26);

    if (use_virtual && prob == 1)
      col_type = Column::GENERATED;
    else if ((!options->at(Option::NO_INT)->getBool() && prob < 5))
      col_type = Column::INT;
    else if (!options->at(Option::NO_INTEGER)->getBool() && prob < 6)
      col_type = Column::INTEGER;
    else if (!options->at(Option::NO_FLOAT)->getBool() && prob < 8)
      col_type = Column::FLOAT;
    else if (!options->at(Option::NO_DOUBLE)->getBool() && prob < 10)
      col_type = Column::DOUBLE;
    else if (!options->at(Option::NO_VARCHAR)->getBool() && prob < 14 &&
             prob > 10)
      col_type = Column::VARCHAR;
    else if (!options->at(Option::NO_CHAR)->getBool() && prob < 16 && prob > 14)
      col_type = Column::CHAR;
    else if (!options->at(Option::NO_TEXT)->getBool() && prob == 17)
      col_type = Column::TEXT;
    else if (!options->at(Option::NO_BLOB)->getBool() && prob == 18)
      col_type = Column::BLOB;
    else if (!options->at(Option::NO_BOOL)->getBool() && prob == 19)
      col_type = Column::BOOL;
    else if (prob == 20 && !options->at(Option::NO_DATE)->getBool())
      col_type = Column::DATE;
    else if (prob == 21 && !options->at(Option::NO_DATETIME)->getBool())
      col_type = Column::DATETIME;
    else if (prob == 22 && !options->at(Option::NO_TIMESTAMP)->getBool())
      col_type = Column::TIMESTAMP;
    else if (prob == 23 && !options->at(Option::NO_BIT)->getBool())
      col_type = Column::BIT;
    else if (prob == 24 && !options->at(Option::NO_JSON)->getBool())
      col_type = Column::JSON;
    else if (prob == 25 && !options->at(Option::NO_ENUM)->getBool())
      col_type = Column::ENUM;
    else if (prob == 26 && !options->at(Option::NO_DECIMAL)->getBool())
      col_type = Column::DECIMAL;
  }

  Column *tc;

  std::string name = "N" + std::to_string(rand_int(300));

  if (col_type == Column::GENERATED)
    tc = new Generated_Column(name, this);
  else if (col_type == Column::BLOB)
    tc = new Blob_Column(name, this);
  else if (col_type == Column::TEXT)
    tc = new Text_Column(name, this);
  else if (col_type == Column::ENUM)
    tc = new Enum_Column(name, this);
  else if (col_type == Column::DECIMAL)
    tc = new Decimal_Column(name, this);
  else
    tc = new Column(name, this, col_type);

  sql += tc->definition();

  std::string algo;
  std::string algorithm_lock = pick_algorithm_lock(&algo);

  bool has_virtual_column = false;
  /* if a table has virtual column, We can not add AFTER */
  for (auto col : *columns_) {
    if (col->type_ == Column::GENERATED) {
      has_virtual_column = true;
      break;
    }
  }
  if (col_type == Column::GENERATED)
    has_virtual_column = true;

  if ((((algo == "INSTANT" || algo == "INPLACE") &&
        has_virtual_column == false && key_block_size == 1) ||
       (algo != "INSTANT" && algo != "INPLACE" && algo != "")) &&
      rand_int(10, 1) <= 7) {
    sql += " AFTER " + columns_->at(rand_int(columns_->size() - 1))->name_;
  }

  sql += algorithm_lock;

  unlock_table_mutex();

  if (execute_sql(sql, thd)) {
    lock_table_mutex(thd->ddl_query);
    auto add_new_column =
        true; // check if there is already a column with this name
    for (auto col : *columns_) {
      if (col->name_.compare(tc->name_) == 0)
        add_new_column = false;
    }

    if (add_new_column)
      AddInternalColumn(tc);
    else
      delete tc;

    unlock_table_mutex();
  } else
    delete tc;
}

void Table::ModifyColumnSecondaryEngine(Thd1 *thd) {
  std::string sql = "ALTER TABLE " + name_ + " MODIFY COLUMN ";
  lock_table_mutex(thd->ddl_query);
  auto col = columns_->at(rand_int(columns_->size() - 1));
  auto col_name = col->name_;
  auto old_value = col->not_secondary;
  col->not_secondary = !col->not_secondary;
  sql += " " + col->definition() + pick_algorithm_lock();
  unlock_table_mutex();
  if (!execute_sql(sql, thd)) {
    /* find the existing column with name and revert the changes */
    lock_table_mutex(thd->ddl_query);
    for (auto col : *columns_) {
      if (col->name_.compare(col_name) == 0) {
        col->not_secondary = old_value;
        break;
      }
    }
    unlock_table_mutex();
  }
}

/* randomly drop some index of table */
void Table::DropIndex(Thd1 *thd) {
  lock_table_mutex(thd->ddl_query);
  if (indexes_ != nullptr && indexes_->size() > 0) {
    auto index = indexes_->at(rand_int(indexes_->size() - 1));
    auto name = index->name_;
    std::string sql = "ALTER TABLE " + name_ + " DROP INDEX " + name;
    sql += pick_algorithm_lock();
    unlock_table_mutex();
    if (execute_sql(sql, thd)) {
      lock_table_mutex(thd->ddl_query);
      for (size_t i = 0; i < indexes_->size(); i++) {
        auto ix = indexes_->at(i);
        if (ix->name_.compare(name) == 0) {
          delete ix;
          indexes_->at(i) = indexes_->back();
          indexes_->pop_back();
          break;
        }
      }
      unlock_table_mutex();
    }
  } else {
    unlock_table_mutex();
    thd->thread_log << "no index to drop " + name_ << std::endl;
  }
}

/*randomly add some index on the table */
void Table::AddIndex(Thd1 *thd) {
  auto i = rand_int(1000);
  Index *id = new Index(name_ + std::to_string(i));

  static size_t max_columns = opt_int(INDEX_COLUMNS);
  lock_table_mutex(thd->ddl_query);

  /* number of columns to be added */
  int no_of_columns = rand_int(
      (max_columns < columns_->size() ? max_columns : columns_->size()), 1);

  std::vector<int> col_pos; // position of columns

  /* pick some columns */
  while (col_pos.size() < (size_t)no_of_columns) {
    int current = rand_int(columns_->size() - 1);
    /* auto-inc column should be first column in auto_inc_index */
    bool already_added = false;
    for (auto id : col_pos) {
      if (id == current)
        already_added = true;
    }
    if (!already_added)
      col_pos.push_back(current);
  } // while

  for (auto pos : col_pos) {
    auto col = columns_->at(pos);
    static bool no_desc_support = opt_bool(NO_DESC_INDEX);
    bool column_desc = false;
    if (!no_desc_support) {
      column_desc = rand_int(100) < DESC_INDEXES_IN_COLUMN
                        ? true
                        : false; // 33 % are desc //
    }
    id->AddInternalColumn(new Ind_col(col, column_desc)); // desc is set as true
  }

  if (rand_int(1000) <= options->at(Option::UNIQUE_INDEX_PROB_K)->getInt()) {
    id->unique = true;
  }

  std::string sql = "ALTER TABLE " + name_ + " ADD " + id->definition();
  sql += pick_algorithm_lock();
  unlock_table_mutex();

  if (execute_sql(sql, thd)) {
    lock_table_mutex(thd->ddl_query);
    auto do_not_add = false; // check if there is already a index with this name
    for (auto ind : *indexes_) {
      if (ind->name_.compare(id->name_) == 0)
        do_not_add = true;
    }
    if (!do_not_add)
      AddInternalIndex(id);
    else
      delete id;

    unlock_table_mutex();
  } else {
    delete id;
  }
}

std::string Table::SelectColumn() {
  /* Table is already locked no need to take lock */
  std::string select;
  auto col = columns_->at(rand_int(columns_->size() - 1));
  select = (col->type_ == Column::JSON ? json_select(col) : col->name_);

  /*todojson enable multiple select. previous value was 20 */
  if (rand_int(100) < 20) {
    for (const auto &col1 : *columns_) {
      /* we do not select non_secondary as select can be in secondary engine */
      if (col1->not_secondary)
        continue;
      if (rand_int(100) < 50)
        select += ", " + (col1->type_ == Column::JSON ? json_select(col1)
                                                      : col1->name_);
    }
  }
  return select;
}

void Table::SetSecondaryEngine(Thd1 *thd) {
  lock_table_mutex(thd->ddl_query);
  std::string second_engine = "NULL";
  if (rand_int(1) == 0) {
    second_engine = options->at(Option::SECONDARY_ENGINE)->getString();
  }
  execute_sql("COMMIT", thd);
  thd->trx_left = 0;
  std::string sql =
      "ALTER TABLE " + name_ + " SECONDARY_ENGINE=" + second_engine;
  execute_sql(sql, thd);
  if (second_engine == options->at(Option::SECONDARY_ENGINE)->getString() &&
      options->at(Option::WAIT_FOR_SYNC)->getBool()) {
    wait_till_sync(name_, thd);
  }
  unlock_table_mutex();
}

void Table::IndexRename(Thd1 *thd) {
  lock_table_mutex(thd->ddl_query);
  if (indexes_->size() == 0)
    unlock_table_mutex();
  else {
    auto ps = rand_int(indexes_->size() - 1);
    auto name = indexes_->at(ps)->name_;
    /* ALTER index to _rename or back to orignal_name */
    std::string new_name = "_rename";
    static auto s = new_name.size();
    if (name.size() > s &&
        name.substr(name.length() - s).compare("_rename") == 0)
      new_name = name.substr(0, name.length() - s);
    else
      new_name = name + new_name;
    std::string sql =
        "ALTER TABLE " + name_ + " RENAME INDEX " + name + " To " + new_name;
    sql += pick_algorithm_lock();
    unlock_table_mutex();
    if (execute_sql(sql, thd)) {
      lock_table_mutex(thd->ddl_query);
      for (auto &ind : *indexes_) {
        if (ind->name_.compare(name) == 0)
          ind->name_ = new_name;
      }
      unlock_table_mutex();
    }
  }
}

void Table::ColumnRename(Thd1 *thd) {
  lock_table_mutex(thd->ddl_query);
  auto ps = rand_int(columns_->size() - 1);
  auto name = columns_->at(ps)->name_;
  /* ALTER column to _rename or back to orignal_name */
  std::string new_name = "_rename";
  static auto s = new_name.size();
  if (name.size() > s && name.substr(name.length() - s).compare("_rename") == 0)
    new_name = name.substr(0, name.length() - s);
  else
    new_name = name + new_name;
  std::string sql =
      "ALTER TABLE " + name_ + " RENAME COLUMN " + name + " To " + new_name;
  sql += pick_algorithm_lock();
  unlock_table_mutex();
  if (execute_sql(sql, thd)) {
    lock_table_mutex(thd->ddl_query);
    for (auto &col : *columns_) {
      if (col->name_.compare(name) == 0)
        col->name_ = new_name;
    }
    unlock_table_mutex();
  }
}

static bool only_bool(std::vector<Column *> *columns) {
  for (auto c : *columns) {
    if (c->type_ != Column::BOOL)
      return false;
  }
  return true;
}

Column *Table::GetRandomColumn() {

  Column *col = nullptr;
  if (rand_int(100) <= options->at(Option::USING_PK_PROB)->getInt()) {
    for (auto c : *columns_) {
      if (c->primary_key) {
        col = c;
        return col;
      }
    }
  }

  if (indexes_->size() > 0) {
    auto indx = indexes_->at(rand_int(indexes_->size() - 1));
    if (rand_int(100) > options->at(Option::USING_PK_PROB)->getInt() &&
        indx->columns_->size() > 0) {
      auto first_col = indx->columns_->at(0)->column;
      if (first_col->type_ != Column::BOOL && first_col->type_ != Column::FLOAT)
        col = first_col;
    }
  }

  int max_tries = 0;
  if (columns_->size() == 1) {
    return columns_->at(0);
  }
  while (col == nullptr) {
    int col_pos = rand_int(columns_->size() - 1);
    if (options->at(Option::USING_PK_PROB)->getInt() == 0 &&
        columns_->at(col_pos)->primary_key) {
      continue;
    }
    switch (columns_->at(col_pos)->type_) {
    case Column::BOOL:
      if (rand_int(10000) == 1 || only_bool(columns_))
        col = columns_->at(col_pos);
      break;
    case Column::INT:
    case Column::VARCHAR:
    case Column::CHAR:
    case Column::GENERATED:
    case Column::DATE:
    case Column::DATETIME:
    case Column::TIMESTAMP:
    case Column::DECIMAL:
      col = columns_->at(col_pos);
      break;
    case Column::INTEGER:
      if (rand_int(1000) < 10)
        col = columns_->at(col_pos);
      break;
    case Column::COLUMN_MAX:
      break;
      /* these columns are used very less */
    case Column::FLOAT:
    case Column::DOUBLE:
    case Column::ENUM:
    case Column::BLOB:
    case Column::TEXT:
    case Column::JSON:
    case Column::BIT:
      if (max_tries == 100) {
        col = columns_->at(col_pos);
        break;
      }
      max_tries++;
      break;
    }
  }

  return col;
}

std::string Table::GetRandomPartition() {
  std::string sql = "";
  if (type == PARTITION && rand_int(10) < 2) {
    sql += " PARTITION (";
    auto part = static_cast<Partition *>(this);
    assert(part->number_of_part > 0);
    if (part->part_type == Partition::RANGE) {
      sql += part->positions.at(rand_int(part->positions.size() - 1)).name;
      for (int i = 0; i < rand_int(part->positions.size()); i++) {
        if (rand_int(5) == 1)
          sql += "," +
                 part->positions.at(rand_int(part->positions.size() - 1)).name;
      }
    } else if (part->part_type == Partition::KEY ||
               part->part_type == Partition::HASH) {
      sql += "p" + std::to_string(rand_int(part->number_of_part - 1));
      for (int i = 0; i < rand_int(part->number_of_part); i++) {
        if (rand_int(2) == 1)
          sql += ", p" + std::to_string(rand_int(part->number_of_part - 1));
      }
    } else if (part->part_type == Partition::LIST) {
      sql += part->lists.at(rand_int(part->lists.size() - 1)).name;
      for (int i = 0; i < rand_int(part->lists.size()); i++) {
        if (rand_int(5) == 1)
          sql += "," + part->lists.at(rand_int(part->lists.size() - 1)).name;
      }
    }

    sql += ")";
  }
  return sql;
}

std::string Table::GetWherePrecise() {
  auto col = GetRandomColumn();
  std::string where = " WHERE ";
  std::string rand_value;

  if (col->type_ == Column::JSON) {
    where += json_where(col);
    rand_value = json_value(col);
  } else {
    where += col->name_;
    rand_value = col->rand_value();
  }

  if (rand_int(100, 1) <= options->at(Option::USING_PK_PROB)->getInt() ||
      rand_int(100) > 1) {
    return where + " = " + rand_value;
  }

  std::string second_rand_value;
  if (col->type_ == Column::JSON) {
    second_rand_value = json_value(col);
  }else {
    second_rand_value = col->rand_value();
  }

    return where + " IN (" + rand_value + ", " + second_rand_value + ")";
}

std::string Table::GetWhereBulk() {
  auto col = GetRandomColumn();
  std::string where = " WHERE " + col->name_;
  std::string rand_value = col->rand_value();

  if (rand_value == "NULL") {
    return where + " IS " + (rand_int(1000) == 1 ? "NOT NULL" : "NULL");
  }

  if (col->is_col_number() && rand_int(100) < 40) {
    auto lower_value = std::to_string(std::stol(rand_value) - rand_int(100, 3));
    return where + " BETWEEN " + lower_value + " AND " + rand_value;
  }

  if (col->type_ == Column::BLOB && rand_int(1000) == 1) {
#ifdef USE_MYSQL
    return " WHERE instr( " + col->name_ + ",_binary\"" + rand_string(20) +
           "%\")";
#else
    return " WHERE position(" + col->name_ + ", '" + rand_string(20) + "') > 0";
#endif
  }

  if (col->is_col_can_be_compared()) {
    if (rand_int(100) == 1) {
      return where + " >= " + rand_value;
    }
    if (rand_int(100) == 1) {
      return where + " <= " + rand_value;
    }

    auto second_rand_value = col->rand_value();

    if (second_rand_value == "NULL") {
      return where + " >= " + rand_value + " AND " + col->name_ +
             " IS NOT NULL";
    }

    if (rand_int(100) < 20) {
      return where + " >= " + rand_value + " AND " + col->name_ +
             " <= " + second_rand_value;
    }
    if (rand_int(100) < 10) {
      return where + " <= " + rand_value + " AND " + col->name_ +
             " >= " + second_rand_value;
    }
  }

  if (col->is_col_string() && rand_int(100) < 20) {
#ifdef USE_MYSQL
    return where + " LIKE " + "\"" + rand_string(20) + "%\"";
#else
    return where + " LIKE '" + rand_string(20) + "%'";
#endif
  }

  if (col->is_col_string() && rand_int(100) < 90) {
    auto second_rand_value = col->rand_value();
    if (second_rand_value == "NULL") {
      return where + " = " + rand_value + " OR " + col->name_ + " IS NULL";
    }
    if (rand_int(100) < 80) {
      return where + " BETWEEN " + rand_value + " AND " + second_rand_value;
    } else {
      return where + " NOT BETWEEN " + col->rand_value() + " and " +
             col->rand_value();
    }
  }

  if (rand_int(100) == 1) {
#ifdef USE_CLICKHOUSE
    return " WHERE 1=1";
#else
    return "";
#endif
  }

  return where + " = " + col->rand_value();
}

void Table::SelectRandomRow(Thd1 *thd, bool select_for_update) {
  lock_table_mutex(thd->ddl_query);
  std::string where = GetWherePrecise();
  assert(where.size() > 4);
  auto select_column = SelectColumn();
  std::string sql = "SELECT " + select_column + " FROM " + name_ +
                    GetRandomPartition() + where;

  if (options->at(Option::COMPARE_RESULT)->getBool()) {
    sql += " order by " + select_column;
  }
  if (select_for_update &&
      options->at(Option::SECONDARY_ENGINE)->getString() == "")
    sql += " FOR UPDATE SKIP LOCKED";

  unlock_table_mutex();
  if (options->at(Option::COMPARE_RESULT)->getBool()) {
    Compare_between_engine(sql, thd);
  } else {
    if (options->at(Option::SELECT_IN_SECONDARY)->getBool()) {
      execute_sql("COMMIT", thd);
      thd->trx_left = 0;
    }
    execute_sql(sql, thd);
  }
}

void Table::CreateFunction(Thd1 *thd) {
  static std::vector<std::string> function_dmls = []() {
    std::vector<std::string> v;
    std::string option_func = opt_string(FUNCTION_CONTAINS_DML);
    std::transform(option_func.begin(), option_func.end(), option_func.begin(),
                   ::tolower);
    std::istringstream iss(option_func);
    std::string token;
    while (std::getline(iss, token, ',')) {
      if (token == "update" &&
          options->at(Option::NO_UPDATE)->getBool() == false)
        v.push_back("UPDATE");
      else if (token == "insert" &&
               options->at(Option::NO_INSERT)->getBool() == false)
        v.push_back("INSERT");
      else if (token == "delete" &&
               options->at(Option::NO_DELETE)->getBool() == false)
        v.push_back("DELETE");
      else
        std::runtime_error("invalid function dml option");
    }
    return v;
  }();
  // todo limit insert update delete
  std::string sql = "DROP FUNCTION IF EXISTS f" + name_;
  execute_sql(sql, thd);

  assert(function_dmls.size() > 0);

  sql = "CREATE FUNCTION f" + name_ + "() RETURNS INT DETERMINISTIC BEGIN ";

  lock_table_mutex(thd->ddl_query);
  for (int j = 0; j < rand_int(4, 1); j++) {
    for (auto &dml : function_dmls) {
      if (dml == "INSERT")
        for (int i = 0; i < rand_int(3, 1); i++)
          sql.append("INSERT INTO " + name_ + ColumnValues(thd) + "; ");
      else if (dml == "UPDATE")
        for (int i = 0; i < rand_int(4, 1); i++)
          sql.append("UPDATE " + add_ignore_clause() + name_ + " SET " +
                     SetClause() + GetWherePrecise() + "; ");
      else if (dml == "DELETE")
        for (int i = 0; i < rand_int(4, 1); i++)
          sql.append("DELETE " + add_ignore_clause() + " FROM " + name_ +
                     GetRandomPartition() + GetWherePrecise() + "; ");
    }
  }
  unlock_table_mutex();

  sql.append("RETURN 1; ");
  sql.append("END");

  execute_sql(sql, thd);

  // Call the stored procedure
  execute_sql("SELECT f" + name_ + "()", thd);
}

void Table::Replace(Thd1 *thd) {
  lock_table_mutex(thd->ddl_query);
  auto sql = "REPLACE INTO " + name_ + ColumnValues(thd);
  unlock_table_mutex();
  std::shared_lock<std::shared_mutex> lock(dml_mutex);
  execute_sql(sql, thd);
}

void Table::UpdateRandomROW(Thd1 *thd) {
  lock_table_mutex(thd->ddl_query);

  std::string sql;
  sql = "UPDATE " + add_ignore_clause() + name_ + GetRandomPartition() +
        " SET " + SetClause() + GetWherePrecise();

  unlock_table_mutex();

  std::shared_lock<std::shared_mutex> lock(dml_mutex);
  execute_sql(sql, thd);
}

void Table::DeleteRandomRow(Thd1 *thd) {
  lock_table_mutex(thd->ddl_query);
  std::string sql = "DELETE " + add_ignore_clause() + " FROM " + name_ +
                    GetRandomPartition() + GetWherePrecise();
  unlock_table_mutex();
  std::shared_lock lock(dml_mutex);
  execute_sql(sql, thd);
}

void Table::UpdateAllRows(Thd1 *thd) {
  lock_table_mutex(thd->ddl_query);
  std::string sql = "UPDATE " + add_ignore_clause() + name_ +
                    GetRandomPartition() + " SET " + SetClause() +
                    GetWhereBulk();
  unlock_table_mutex();
  std::shared_lock lock(dml_mutex);
  execute_sql(sql, thd);
}

void Table::DeleteAllRows(Thd1 *thd) {
  lock_table_mutex(thd->ddl_query);
  std::string sql = "DELETE " + add_ignore_clause() + " FROM " + name_ +
                    GetRandomPartition() + GetWhereBulk();
  unlock_table_mutex();
  std::shared_lock lock(dml_mutex);
  execute_sql(sql, thd);
}

void Table::SelectAllRow(Thd1 *thd, bool select_for_update) {
  lock_table_mutex(thd->ddl_query);
  std::string sql = "SELECT " + SelectColumn() + " FROM " + name_ +
                    GetRandomPartition() + GetWhereBulk();
  if (select_for_update &&
      options->at(Option::SECONDARY_ENGINE)->getString() == "")
    sql += " FOR UPDATE SKIP LOCKED";
  unlock_table_mutex();
  if (options->at(Option::SELECT_IN_SECONDARY)->getBool()) {
    execute_sql("COMMIT", thd);
    thd->trx_left = 0;
  }
  execute_sql(sql, thd);
}

std::string Table::SetClause() {
  Column *col = nullptr;

  if ((columns_->at(0)->primary_key &&
       rand_int(100, 1) <= options->at(Option::PKEY_IN_SET)->getInt()) ||
      columns_->size() == 1) {
    col = columns_->at(0);
  } else {
    while (col == nullptr) {
      int set = rand_int(columns_->size() - 1);
      if (columns_->at(set)->type_ != Column::GENERATED &&
          columns_->at(set)->primary_key == false)
        col = columns_->at(set);
    }
  }
  std::string set_clause = col->name_ + " = ";
  set_clause +=
      (col->type_ == Column::JSON ? json_set(col) : col->rand_value());

  /* 10% update most of column */
  if (rand_int(100) < 10) {
    for (const auto &column : *columns_) {
      if (column->primary_key == false && column->type_ != Column::GENERATED &&
          column->name_ != col->name_ && rand_int(100) > 50) {
        set_clause += "," + column->name_ + " = " +
                      (column->type_ == Column::JSON ? json_set(column)
                                                     : column->rand_value());
      }
    }
  }

  return set_clause + " ";
}


/* set mysqld_variable */
void set_mysqld_variable(Thd1 *thd) {
  static int total_probablity = sum_of_all_server_options();
  int rd = rand_int(total_probablity);
  for (auto &opt : *server_options) {
    if (rd <= opt->prob) {
      std::string sql = " SET ";
      sql += rand_int(3) == 0 ? " SESSION " : " GLOBAL ";
      sql += opt->name + "=" + opt->values.at(rand_int(opt->values.size() - 1));
      execute_sql(sql, thd);
    }
  }
}

/* alter tablespace set encryption */
void alter_tablespace_encryption(Thd1 *thd) {
  std::string tablespace;

  if ((rand_int(10) < 2 && thd->db->get_server_version() >= 80000) ||
      g_tablespace.size() == 0) {
    tablespace = "mysql";
  } else if (g_tablespace.size() > 0) {
    tablespace = g_tablespace[rand_int(g_tablespace.size() - 1)];
  }

  if (tablespace.size() > 0) {
    std::string sql = "ALTER TABLESPACE " + tablespace + " ENCRYPTION ";
    sql += (rand_int(1) == 0 ? "'Y'" : "'N'");
    execute_sql(sql, thd);
  }
}

/* alter table discard tablespace */
void Table::Alter_discard_tablespace(Thd1 *thd) {
  std::string sql = "ALTER TABLE " + name_ + " DISCARD TABLESPACE";
  execute_sql(sql, thd);
  /* Discarding the tablespace makes the table unusable, hence recreate the
   * table */
  DropCreate(thd);
}

/* alter database set encryption */
void alter_database_encryption(Thd1 *thd) {
  std::string sql = "ALTER DATABASE " +
                    options->at(Option::DATABASE)->getString() + " ENCRYPTION ";
  sql += (rand_int(1) == 0 ? "'Y'" : "'N'");
  execute_sql(sql, thd);
}

void alter_database_collation(Thd1 *thd) {
  std::string sql = "ALTER DATABASE " +
    options->at(Option::DATABASE)->getString() + " DEFAULT CHARACTER SET utf8mb4 DEFAULT COLLATE ";
  sql += (rand_int(1) == 0 ? "utf8mb4_0900_ai_ci" : "utf8mb4_general_ci");
  execute_sql(sql, thd);
}

/* alter tablespace rename */
void alter_tablespace_rename(Thd1 *thd) {
  if (g_tablespace.size() > 0) {
    auto tablespace = g_tablespace[rand_int(g_tablespace.size() - 1),
                                   1]; // don't pick innodb_system;
    std::string sql = "ALTER TABLESPACE " + tablespace;
    if (rand_int(1) == 0)
      sql += "_rename RENAME TO " + tablespace;
    else
      sql += " RENAME TO " + tablespace + "_rename";
    execute_sql(sql, thd);
  }
}
/* create in memory data about tablespaces, row_format, key_block size and undo
 * tablespaces */
void create_in_memory_data() {

  /* Adjust the tablespaces */
  if (!options->at(Option::NO_TABLESPACE)->getBool()) {
    g_tablespace = {"tab02k", "tab04k"};
    g_tablespace.push_back("innodb_system");
    if (g_innodb_page_size >= INNODB_8K_PAGE_SIZE) {
      g_tablespace.push_back("tab08k");
    }
    if (g_innodb_page_size >= INNODB_16K_PAGE_SIZE) {
      g_tablespace.push_back("tab16k");
    }
    if (g_innodb_page_size >= INNODB_32K_PAGE_SIZE) {
      g_tablespace.push_back("tab32k");
    }
    if (g_innodb_page_size >= INNODB_64K_PAGE_SIZE) {
      g_tablespace.push_back("tab64k");
    }

    /* add addtional tablespace */
    auto tbs_count = opt_int(NUMBER_OF_GENERAL_TABLESPACE);
    if (tbs_count > 1) {
      auto current_size = g_tablespace.size();
      for (size_t i = 0; i < current_size; i++) {
        for (int j = 1; j <= tbs_count; j++)
          if (g_tablespace[i].compare("innodb_system") == 0)
            continue;
          else
            g_tablespace.push_back(g_tablespace[i] + std::to_string(j));
      }
    }
  }

  /* set some of tablespace encrypt */
  if (options->at(Option::USE_ENCRYPTION)->getBool()) {
    int i = 0;
    for (auto &tablespace : g_tablespace) {
      if (i++ % 2 == 0 &&
          tablespace.compare("innodb_system") != 0) // alternate tbs are encrypt
        tablespace += "_e";
    }
  }

  std::string row_format = opt_string(ROW_FORMAT);

  if (row_format.compare("all") == 0 &&
      options->at(Option::NO_TABLE_COMPRESSION)->getInt() == true)
    row_format = "uncompressed";

  if (row_format.compare("uncompressed") == 0) {
    g_row_format = {"DYNAMIC", "REDUNDANT"};
  } else if (row_format.compare("all") == 0) {
    g_row_format = {"DYNAMIC", "REDUNDANT", "COMPRESSED"};
    g_key_block_size = {0, 0, 1, 2, 4};
  } else if (row_format.compare("none") == 0) {
    g_key_block_size.clear();
  } else {
    g_row_format.push_back(row_format);
  }

  if (g_innodb_page_size > INNODB_16K_PAGE_SIZE ||
      options->at(Option::SECONDARY_ENGINE)->getString() != "") {
    g_row_format.clear();
    g_key_block_size.clear();
    g_compression.clear();
  }

  int undo_tbs_count = opt_int(NUMBER_OF_UNDO_TABLESPACE);
  if (undo_tbs_count > 0) {
    for (int i = 1; i <= undo_tbs_count; i++) {
      g_undo_tablespace.push_back("undo_00" + std::to_string(i));
    }
  }
}

/* clean tables from memory */
void clean_up_at_end() {
  for (auto &table : *all_tables)
    delete table;
  delete all_tables;
}

static void ensure_no_table_in_secondary(Thd1 *thd) {
  if (options->at(Option::SELECT_IN_SECONDARY)->getBool()) {
    execute_sql("SET @@SESSION.USE_SECONDARY_ENGINE=OFF", thd);
  }
  std::string sql = "select count(1) from performance_schema." +
                    lower_case_secondary() +
                    "_table_sync_status where "
                    "table_schema=\"";
  sql += options->at(Option::DATABASE)->getString() + "\"";
  while (true) {
    if (thd->db->get_single_value(sql) == "0") {
      break;
    }
    std::this_thread::sleep_for(std::chrono::seconds(5));
    std::cerr << "waiting for table to be removed from secondary engine"
              << std::endl;
  }
  if (options->at(Option::SELECT_IN_SECONDARY)->getBool()) {
    execute_sql("SET @@SESSION.USE_SECONDARY_ENGINE=FORCED", thd);
  }
}

/* create new database and tablespace */
void create_database_tablespace(Thd1 *thd) {

  std::string sql =
      "DROP DATABASE IF EXISTS " + options->at(Option::DATABASE)->getString();
  if (!execute_sql(sql, thd)) {
    std::stringstream ss;
    print_and_log("Failed to drop database", thd, true);
    exit(EXIT_FAILURE);
  }

  sql = "CREATE DATABASE IF NOT EXISTS " +
        options->at(Option::DATABASE)->getString();
  execute_sql(sql, thd);

  if (options->at(Option::SECONDARY_ENGINE)->getString() != "") {
    ensure_no_table_in_secondary(thd);
  }


  for (auto &tab : g_tablespace) {
    if (tab.compare("innodb_system") == 0)
      continue;
    /* If left over from previous run */
    execute_sql("ALTER TABLESPACE " + tab + "_rename rename to " + tab, thd);
    execute_sql("DROP TABLESPACE " + tab, thd);
    std::string sql =
        "CREATE TABLESPACE " + tab + " ADD DATAFILE '" + tab + ".ibd' ";
    if (g_innodb_page_size <= INNODB_16K_PAGE_SIZE) {
      sql += " FILE_BLOCK_SIZE " + tab.substr(3, 3);
    }

    /* encrypt tablespace */
    if (options->at(Option::USE_ENCRYPTION)->getBool()) {
      if (tab.substr(tab.size() - 2, 2).compare("_e") == 0)
        sql += " ENCRYPTION='Y'";
      else
        sql += " ENCRYPTION='N'";
    }

    if (!execute_sql(sql, thd)) {
      print_and_log("Failed to create tablespace " + tab, thd, true);
      exit(EXIT_FAILURE);
    }
  }

  for (auto &name : g_undo_tablespace) {
    std::string sql =
        "CREATE UNDO TABLESPACE " + name + " ADD DATAFILE '" + name + ".ibu'";
    execute_sql(sql, thd);
  }
}

/* load metadata */
bool Thd1::load_metadata() {
  sum_of_all_opts = sum_of_all_options(this);
  rng = std::mt19937(set_seed(nullptr));

  random_strs = random_strs_generator();

  if (options->at(Option::SECONDARY_ENGINE)->getString() != "") {
    validate_secondary_engine(this);
  }

  /*set seed for current step*/
  std::cout << "Running " << FORK << " version " << db->get_server_version()
            << std::endl;

  /* create in-memory data for general tablespaces */
  create_in_memory_data();

  if (options->at(Option::STEP)->getInt() > 1 &&
      !options->at(Option::PREPARE)->getBool()) {

    if (options->at(Option::XA_TRANSACTION)->getInt() != 0)
      execute_sql("XA COMMIT " + std::to_string(thread_id), this);

    auto file = load_metadata_from_file();
    if (file == "FAILED") {
      exit(EXIT_FAILURE);
    } else {
      std::cout << "metadata loaded from " << file << std::endl;
    }
  } else {
    if (strcmp(FORK, "MySQL") == 0)
      create_database_tablespace(this);
    if (load_metadata_from_file() == "FAILED") {
      generate_metadata_for_tables();
      print_and_log(
          "metadata generated randomly using seed " +
              std::to_string(options->at(Option::INITIAL_SEED)->getInt()),
          this);
    }
  }

  if (options->at(Option::TABLES)->getInt() <= 0) {
    print_and_log("no table to work on \n");
    exit(EXIT_FAILURE);
  }

  return 1;
}
std::string toHumanReadable(double number) {
  const char *suffixes[] = {"", "K", "M", "B", "T"};
  int suffixIndex = 0;
  double temp = number;

  while (temp >= 1000 && suffixIndex < 4) {
    temp /= 1000;
    suffixIndex++;
  }

  // Round to 2 decimal places if needed
  char buffer[32];
  if (temp < 10) {
    snprintf(buffer, sizeof(buffer), "%.2f%s", temp, suffixes[suffixIndex]);
  } else if (temp < 100) {
    snprintf(buffer, sizeof(buffer), "%.1f%s", temp, suffixes[suffixIndex]);
  } else {
    snprintf(buffer, sizeof(buffer), "%.0f%s", temp, suffixes[suffixIndex]);
  }

  // Remove trailing zeros and decimal point if unnecessary
  std::string result = buffer;
  if (result.find('.') != std::string::npos) {
    result.erase(result.find_last_not_of('0') + 1);
    if (result.back() == '.') {
      result.pop_back();
    }
  }

  return result;
}
