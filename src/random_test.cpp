/*
 =========================================================
 #     Created by Rahul Malik, Percona LLC             #
 =========================================================
*/
#include "random_test.hpp"
#include "workload_helpers.hpp"
#include "common.hpp"
#include "json.hpp"
#include "node.hpp"
#ifdef USE_CLICKHOUSE
#include "ch_verify.hpp"
#endif
#include <array>
#include <deque>
#include <functional>
#include <map>
#include <document.h>
#include <filesystem>
#include <iomanip>
#include <libgen.h>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>

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
std::vector<SettingSpec> g_table_settings;
std::vector<std::string> g_session_settings;
std::vector<SettingSpec> g_alterable_table_settings;
std::string g_fixed_table_settings;
#ifdef USE_CLICKHOUSE
std::vector<MVInfo> g_materialized_views;
std::mutex g_materialized_views_mutex;
#endif
#ifdef USE_CLICKHOUSE
/* --compare-result-with-setting bookkeeping, reported at the end of the run */
std::atomic<long> g_compare_done(0);
std::atomic<long> g_compare_baseline_failed(0);
std::atomic<long> g_compare_setting_failed(0);
#endif
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

  ss << "Thread " << (thd ? std::to_string(thd->thread_id) : "#")
     << (thd && thd->myParam ? "@" + std::to_string(thd->myParam->port) : "")
     << " : " << str;
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
static bool
compare_query_result(const query_result &r1, const query_result &r2, Thd1 *thd,
                     bool case_insensitive = false,
                     const std::string &r1_file = "secondary_result.csv",
                     const std::string &r2_file = "mysql_result.csv") {
  auto print_query_result = [&]() {
    save_query_result_in_file(r1, r1_file);
    save_query_result_in_file(r2, r2_file);
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
PSTRESS_TARGET_CLONES
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
bool get_check_result(const std::string &sql, Thd1 *thd) {

  auto result = thd->db->get_query_result(sql);

#ifdef USE_CLICKHOUSE
  /* ClickHouse CHECK TABLE returns one row per part as
     (part_name, is_passed UInt8, message) — three columns, not MySQL's four
     (Table, Op, Msg_type, Msg_text), and no "OK" anywhere. Judging it by the
     MySQL shape below reports every successful check as a failure.
     An empty result is not a failure either: a table with no active parts
     (freshly truncated, or every part dropped) has nothing to check. */
  if (!thd->db->get_error().empty()) {
    print_and_log("CHECK TABLE FAILED " + sql + " " + thd->db->get_error(),
                  thd);
    return false;
  }
  for (const auto &row : result) {
    /* single-value form (check_query_single_value_result=1) is one column */
    const std::string &passed = row.size() > 1 ? row[1] : row[0];
    if (passed != "1") {
      print_and_log("Check table failed " + sql + " part " + row[0] + " -> " +
                        (row.size() > 2 ? row[2] : passed),
                    thd);
      return false;
    }
  }
  return true;
#endif

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
  if (strcmp(FORK, "ClickHouse") == 0) {
    /* one kind of partitioning, PARTITION BY a column; --partition-types is
       MySQL vocabulary and is ignored here */
    Partition::supported.push_back(Partition::CH_KEY);
  } else if (part_supp.compare("all") == 0) {
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
  }
  /* Every multi-row INSERT into a uniform CH_KEY table makes one part per
     partition, times --threads. Past a few dozen partitions merges fall behind
     and the server answers TOO_MANY_PARTS, and max_partitions_per_insert_block
     (default 100) rejects the INSERT outright before that. */
  if (strcmp(FORK, "ClickHouse") == 0 &&
      options->at(Option::MAX_PARTITIONS)->getInt() > 64) {
    print_and_log("--max-partitions above 64 is refused on ClickHouse: each "
                  "INSERT writes one part per partition it touches");
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
  if (strcmp(FORK, "MySQL") == 0 &&
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
    /* Convenient defaults for ClickHouse (only if not overridden on CLI) */
    if (!options->at(Option::ADDRESS)->cl)
      options->at(Option::ADDRESS)->setString("127.0.0.1");
    if (!options->at(Option::USER)->cl)
      options->at(Option::USER)->setString("default");
    if (!options->at(Option::DATABASE)->cl)
      options->at(Option::DATABASE)->setString("test_db");
    if (!options->at(Option::PORT)->cl)
      options->at(Option::PORT)->setString(
          options->at(Option::SECURE)->getBool() ? "9440" : "9000");
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
    /* Partition tables stay on: --partition-prob of the tables get a
       PARTITION BY on a dedicated column (Partition::CH_KEY). 25, the MySQL
       default for --max-partitions, is too many parts per INSERT here. */
    if (!options->at(Option::MAX_PARTITIONS)->cl)
      options->at(Option::MAX_PARTITIONS)->setInt(8);
    options->at(Option::IGNORE_DML_CLAUSE)->setInt(0);
    options->at(Option::REPLACE_ROW)->setInt(0);
    options->at(Option::SELECT_FOR_UPDATE)->setInt(0);
    options->at(Option::SELECT_FOR_UPDATE_BULK)->setInt(0);
    options->at(Option::CALL_FUNCTION)->setInt(0);
    options->at(Option::RENAME_INDEX)->setInt(0);
    options->at(Option::ALTER_DISCARD_TABLESPACE)->setInt(0);
    options->at(Option::ALTER_DATABASE_COLLATION)->setInt(0);
    options->at(Option::NO_BIT)->setBool(true);
    /* A multi-column ORDER BY is one of the shapes most worth testing here --
       it is what real ClickHouse tables look like, and it changes how parts
       sort, how the primary index is built and which projections the optimizer
       can prefer. The composite columns land in both PRIMARY KEY and ORDER BY
       (see the pk_cols loop), so the ORDER BY superset invariant holds, and the
       enforcing key stays the prefix, so --seq-pk still appends.

       The roll is per candidate column, not per table, and the candidates are
       fewer than the column count suggests: resolve_composite_key() skips a
       column whose null_val is set, which is about half of them, so the pool
       averages ~3 of the 9 int columns rather than all 9. Measured at 5: about
       one table in 7 ends up with a 2-column key, a couple of percent with 3,
       and the single-column shape stays dominant. Note that nothing is actually
       emitted Nullable on ClickHouse (null-prob defaults to 0), so that skip is
       conservative here rather than necessary -- raise --composite-key-prob if
       you want the multi-column shape more often. An explicit value still wins,
       including 0 to turn it off. */
    if (!options->at(Option::COMPOSITE_KEY_PROB)->cl)
      options->at(Option::COMPOSITE_KEY_PROB)->setInt(5);
    /* The primary key is the whole ORDER BY here, so append-only keys are the
       shape worth testing; an explicit --seq-pk=false still wins. */
    if (!options->at(Option::SEQ_PK)->cl)
      options->at(Option::SEQ_PK)->setBool(true);
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
  /* CH_ALTER_UPDATE/DELETE are ClickHouse-only mutations, and so is the
     KILL MUTATION that cancels them */
  if (strcmp(FORK, "ClickHouse") != 0) {
    options->at(Option::CH_ALTER_UPDATE)->setInt(0);
    options->at(Option::CH_ALTER_DELETE)->setInt(0);
    options->at(Option::CH_KILL_MUTATION)->setInt(0);
    options->at(Option::CH_LIGHTWEIGHT_DELETE)->setInt(0);
  }

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

#ifdef USE_CLICKHOUSE
  {
    const std::string mode =
        options->at(Option::CH_MV_POPULATE_ATOMICALLY)->getString();
    if (mode != "on" && mode != "off" && mode != "random") {
      print_and_log("--mv-populate-atomically must be on, off or random, not " +
                    mode);
      exit(EXIT_FAILURE);
    }
    /* The exact view check needs a source that never merges rows sharing a
       sorting key, otherwise the duplicates a non-atomic populate leaves behind
       are collapsed away before anyone can see them. Say so once at startup
       instead of leaving a weaker check looking like a full one. */
    if (options->at(Option::CH_CREATE_MV)->getInt() > 0 &&
        ch_engine_collapses_rows(options->at(Option::ENGINE)->getString()))
      print_and_log("--engine=" +
                    options->at(Option::ENGINE)->getString() +
                    " merges rows sharing a sorting key, so materialized views "
                    "can only be checked for lost rows, not duplicated ones. "
                    "Use --engine=MergeTree for the exact check.");
  }

  /* ---- projections ---- */
  {
    auto prob = [](Option::Opt o) { return options->at(o)->getInt(); };
    /* only ADD creates one; the other two act on what it made */
    g_projections_enabled = prob(Option::CH_ADD_PROJECTION) > 0;

    if (!g_projections_enabled &&
        (prob(Option::CH_MODIFY_PROJECTION) > 0 ||
         prob(Option::CH_DROP_PROJECTION) > 0 ||
         prob(Option::CH_MATERIALIZE_PROJECTION) > 0)) {
      print_and_log("--modify-projection, --drop-projection and "
                    "--materialize-projection need a projection to act on. "
                    "Pass --add-projection=N, otherwise they would roll and do "
                    "nothing and the run would look healthy while testing "
                    "nothing.");
      exit(EXIT_FAILURE);
    }

    if (g_projections_enabled) {
      /* Projections only exist on the MergeTree family, and ch_resolve_engine
         passes an engine it does not recognise through verbatim, so say so once
         here instead of failing every CREATE TABLE. */
      const std::string base_upper =
          to_upper(ch_engine_base(options->at(Option::ENGINE)->getString()));
      if (base_upper != "MERGETREE" && base_upper != "REPLACINGMERGETREE" &&
          base_upper != "SUMMINGMERGETREE" &&
          base_upper != "AGGREGATINGMERGETREE") {
        print_and_log("--engine=" + options->at(Option::ENGINE)->getString() +
                      " is not a MergeTree family engine pstress knows, so it "
                      "cannot carry projections.");
        exit(EXIT_FAILURE);
      }

      /* A projection index_granularity override needs the parent table to keep
         adaptive granularity, so index_granularity_bytes = 0 in the pool would
         turn every projection ALTER on that table into SUPPORT_IS_DISABLED. */
      for (const auto &spec : g_table_settings)
        if (spec.name == "index_granularity_bytes" &&
            std::find(spec.values.begin(), spec.values.end(), "0") !=
                spec.values.end()) {
          print_and_log("index_granularity_bytes = 0 in the table settings pool "
                        "makes every projection index_granularity override fail "
                        "with SUPPORT_IS_DISABLED. Remove the 0 value.");
          exit(EXIT_FAILURE);
        }

      /* the same trap, reached at run time instead of at CREATE TABLE: an
         alter: entry can put the 0 there later, and a range starting at 0 can
         resolve to it without ever spelling it out */
      for (const auto &spec : g_alterable_table_settings)
        if (spec.name == "index_granularity_bytes" &&
            (spec.is_range ? spec.lo == 0
                           : std::find(spec.values.begin(), spec.values.end(),
                                       "0") != spec.values.end())) {
          print_and_log("alter:index_granularity_bytes in the table settings "
                        "pool lets --modify-table-setting turn off adaptive "
                        "granularity mid-run, which makes every projection "
                        "index_granularity override fail with "
                        "SUPPORT_IS_DISABLED. Drop the alter: prefix from that "
                        "line while projections are on.");
          exit(EXIT_FAILURE);
        }

      if (prob(Option::CH_MODIFY_PROJECTION) > 0 &&
          prob(Option::CH_DROP_PROJECTION) > prob(Option::CH_MODIFY_PROJECTION))
        print_and_log("WARNING: --drop-projection is more likely than "
                      "--modify-projection, so most MODIFYs will find their "
                      "target already dropped.");
    }
  }

  /* ---- text indexes ---- */
  {
    auto prob = [](Option::Opt o) { return options->at(o)->getInt(); };
    g_text_index_phrase_search_enabled =
        prob(Option::CH_TEXT_INDEX_PHRASE_SEARCH_PROB) > 0;

    if (prob(Option::CH_TEXT_INDEX_PROB) == 0 &&
        prob(Option::CH_ADD_TEXT_INDEX) == 0 &&
        (prob(Option::CH_DROP_TEXT_INDEX) > 0 ||
         prob(Option::CH_MATERIALIZE_TEXT_INDEX) > 0))
      print_and_log("WARNING: --drop-text-index and --materialize-text-index "
                    "only act on text indexes that already exist. With "
                    "--text-index-prob=0 and --add-text-index=0 they act only "
                    "on the ones an earlier step left behind.");

    if ((prob(Option::CH_TEXT_INDEX_PROB) > 0 ||
         prob(Option::CH_ADD_TEXT_INDEX) > 0) &&
        prob(Option::TEXT_WORDS) == 0)
      print_and_log("WARNING: text indexes without --text-words. Most String "
                    "values are then a single word, so hasAllTokens and "
                    "hasPhrase rarely have more than one token to look for.");

    if (prob(Option::CH_TEXT_INDEX_PREPROCESSOR_PROB) > 0 &&
        options->at(Option::COMPARE_RESULT_WITH_SETTING)->getBool() &&
        options->at(Option::RUN_QUERY_SETTING)->getString().find(
            "use_skip_indexes") != std::string::npos) {
      print_and_log("--text-index-preprocessor-prob together with "
                    "--run-query-setting use_skip_indexes: a preprocessor is "
                    "only applied on the index path, so the two sides "
                    "legitimately differ and every mismatch would be a false "
                    "alarm. Set --text-index-preprocessor-prob=0, or compare "
                    "against query_plan_direct_read_from_text_index = 0.");
      exit(EXIT_FAILURE);
    }
  }

  /* ---- int skipping indexes ---- */
  {
    auto prob = [](Option::Opt o) { return options->at(o)->getInt(); };
    if (prob(Option::CH_INT_INDEX_PROB) == 0 &&
        prob(Option::CH_ADD_INT_INDEX) == 0 &&
        (prob(Option::CH_DROP_INT_INDEX) > 0 ||
         prob(Option::CH_MATERIALIZE_INT_INDEX) > 0))
      print_and_log("WARNING: --drop-int-index and --materialize-int-index "
                    "only act on int indexes that already exist. With "
                    "--int-index-prob=0 and --add-int-index=0 they act only "
                    "on the ones an earlier step left behind.");
  }

  /* ---- table settings altered while the run is going ---- */
  {
    auto prob = [](Option::Opt o) { return options->at(o)->getInt(); };

    if ((prob(Option::CH_MODIFY_TABLE_SETTING) > 0 ||
         prob(Option::CH_RESET_TABLE_SETTING) > 0) &&
        g_alterable_table_settings.empty()) {
      print_and_log("--modify-table-setting and --reset-table-setting only act "
                    "on settings the pool marked alterable, and there is not "
                    "one: prefix a line of --table-settings-file with alter:. "
                    "Without it both would roll and do nothing and the run "
                    "would look healthy while testing nothing. Note that "
                    "--no-table-settings and --table-settings both bypass the "
                    "pool entirely.");
      exit(EXIT_FAILURE);
    }

    if (prob(Option::CH_RESET_TABLE_SETTING) > 0 &&
        prob(Option::CH_MODIFY_TABLE_SETTING) == 0)
      print_and_log("WARNING: --reset-table-setting without "
                    "--modify-table-setting can only reset what a table rolled "
                    "at CREATE TABLE, so it goes quiet once every table is "
                    "back to the server defaults.");
  }
#else
  /* the create/drop view, kill mutation and projection actions only exist in
     the ClickHouse build. Every one of them has to be zeroed: an option with a
     probability but no case in the dispatch switch aborts the run. */
  opt_int_set(CH_CREATE_MV, 0);
  opt_int_set(CH_DROP_MV, 0);
  opt_int_set(CH_KILL_MUTATION, 0);
  opt_int_set(CH_ADD_PROJECTION, 0);
  opt_int_set(CH_DROP_PROJECTION, 0);
  opt_int_set(CH_MODIFY_PROJECTION, 0);
  opt_int_set(CH_MATERIALIZE_PROJECTION, 0);
  opt_int_set(CH_ADD_TEXT_INDEX, 0);
  opt_int_set(CH_DROP_TEXT_INDEX, 0);
  opt_int_set(CH_MATERIALIZE_TEXT_INDEX, 0);
  opt_int_set(CH_ADD_INT_INDEX, 0);
  opt_int_set(CH_DROP_INT_INDEX, 0);
  opt_int_set(CH_MATERIALIZE_INT_INDEX, 0);
  opt_int_set(CH_MODIFY_TABLE_SETTING, 0);
  opt_int_set(CH_RESET_TABLE_SETTING, 0);
#endif

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
std::string pick_algorithm_lock(std::string *const algo,
                                std::string *const lock) {

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

#ifdef USE_CLICKHOUSE
/* Run the same SQL twice, once as it is and once with SETTINGS
   <--run-query-setting> appended, and compare the two result sets. A setting
   that is not meant to change results has to return exactly the same rows,
   which makes this a cheap oracle for things like join_algorithm.

   Unlike Compare_between_engine() this is not a Table method, because a
   grammar join spans more than one table and there is no single table to lock.
   Nothing is locked at all, so it is on the caller's workload to be
   deterministic: one thread, or insert only, and an ORDER BY on every grammar
   line. */
void compare_result_with_setting(const std::string &sql, Thd1 *thd) {
  static const auto setting = opt_string(RUN_QUERY_SETTING);

  /* A query the server rejects is not a result mismatch, and both sides can
     legitimately fail: an algorithm may not serve a given join, or the setting
     may be what makes the query runnable in the first place. execute_sql() has
     already logged the SQL and the error, so note only which side it was, and
     to the thread log rather than through print_and_log() — that counts every
     console message and kills the run after 300, which an expected failure
     must not do. */
  bool baseline_ok = execute_sql(sql, thd);
  query_result without_setting;
  if (baseline_ok) {
    /* copy the rows out now, the next execute_query() clears the buffer */
    without_setting = thd->db->get_result();
  } else {
    g_compare_baseline_failed++;
    thd->thread_log << "compare-with-setting: failed without the setting"
                    << std::endl;
  }

  /* Run the variant even when the baseline failed. A setting can enable a query
     shape the server otherwise rejects, e.g. an inequality FULL JOIN needs
     join_algorithm='ie_join', and that run is still worth making: it exercises
     the code under test. There is just nothing to compare it against. */
  auto sql_with_setting = add_settings_clause(sql, setting);
  if (!execute_sql(sql_with_setting, thd)) {
    g_compare_setting_failed++;
    thd->thread_log << "compare-with-setting: failed with the setting"
                    << std::endl;
    return;
  }
  if (!baseline_ok)
    return;
  auto with_setting = thd->db->get_result();

  g_compare_done++;
  if (!compare_query_result(
          with_setting, without_setting, thd,
          options->at(Option::COMPARE_CASE_INSENSTIVE)->getBool(),
          "with_setting_result.csv", "default_result.csv")) {
    print_and_log("result set mismatch with SETTINGS " + setting + " for " + sql,
                  thd);
    exit(EXIT_FAILURE);
  }
}

/* How much of the run actually got compared. Without this a run where every
   query failed on one side looks exactly like a clean pass. */
void print_compare_with_setting_stats() {
  if (!options->at(Option::COMPARE_RESULT_WITH_SETTING)->getBool())
    return;
  std::cout << "CompareWithSetting, compared=>" << g_compare_done
            << ", not compared (failed without the setting)=>"
            << g_compare_baseline_failed
            << ", not compared (failed with the setting)=>"
            << g_compare_setting_failed << std::endl;
  if (g_compare_done == 0)
    std::cout << "WARNING: not a single query was compared, the oracle never "
                 "ran. Check the grammar file and --run-query-setting."
              << std::endl;
}
#endif

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
    auto _now = std::chrono::system_clock::now();
    auto _tt  = std::chrono::system_clock::to_time_t(_now);
    std::ostringstream _ts;
    _ts << std::put_time(std::localtime(&_tt), "%H:%M:%S");
    std::lock_guard<std::mutex> lock(ddl_logs_write);
    thd->ddl_logs << "[" << _ts.str() << "] " << thd->thread_id << " " << sql
                  << " " << thd->db->get_error() << std::endl;
  }

  return res;
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
  /* Serialize initialization: both nodes share global all_tables/options */
  static std::mutex load_metadata_mutex;
  std::lock_guard<std::mutex> init_lock(load_metadata_mutex);
  sum_of_all_opts = sum_of_all_options(this);
  rng = std::mt19937(set_seed(nullptr));

  random_strs = random_strs_generator();

#ifdef USE_CLICKHOUSE
  /* drop settings this server does not have before any table rolls for them */
  ch_validate_table_settings(myParam->address, myParam->port,
                             options->at(Option::DATABASE)->getString(),
                             options->at(Option::USER)->getString(),
                             options->at(Option::PASSWORD)->getString());
  /* fail once here rather than on every CREATE TABLE */
  ch_validate_engine(myParam->address, myParam->port,
                     options->at(Option::DATABASE)->getString(),
                     options->at(Option::USER)->getString(),
                     options->at(Option::PASSWORD)->getString());
  /* and once here rather than on every CREATE MATERIALIZED VIEW */
  ch_validate_mv_support(myParam->address, myParam->port,
                         options->at(Option::DATABASE)->getString(),
                         options->at(Option::USER)->getString(),
                         options->at(Option::PASSWORD)->getString());
  /* clear views left by an earlier run before any table is recreated */
  static std::once_flag mv_prepare_once;
  std::call_once(mv_prepare_once, [this]() {
    ch_prepare_materialized_views(this);
  });

#endif

  if (options->at(Option::SECONDARY_ENGINE)->getString() != "") {
    validate_secondary_engine(this);
  }

  /*set seed for current step — print version only once across all nodes */
  static bool version_printed = false;
  if (!version_printed) {
    version_printed = true;
    std::cout << "Running " << FORK << " version " << db->get_server_version()
              << std::endl;
  }

  /* create in-memory data for general tablespaces */
  create_in_memory_data();

  if (options->at(Option::STEP)->getInt() > 1 &&
      !options->at(Option::PREPARE)->getBool()) {

    if (options->at(Option::XA_TRANSACTION)->getInt() != 0)
      execute_sql("XA COMMIT " + std::to_string(thread_id), this);

    if (all_tables->empty()) {
      auto file = load_metadata_from_file();
      if (file == "FAILED") {
        exit(EXIT_FAILURE);
      } else {
        std::cout << "metadata loaded from " << file << std::endl;
        /* Before any worker can allocate a key, and only here: this branch is
           inside the all_tables->empty() guard, so it runs once per process
           even in a multi-node run. Not done for step 1 or --prepare, where
           Table::load() recreates the table and sets the counter itself.
           --seq-pk-trust-metadata skips it and keeps the step file's counter. */
        if (!options->at(Option::SEQ_PK_TRUST_METADATA)->getBool())
          reconcile_pk_counters(this);
      }
    }
  } else {
    if (strcmp(FORK, "MySQL") == 0)
      create_database_tablespace(this);
    if (all_tables->empty()) {
      if (load_metadata_from_file() == "FAILED") {
        generate_metadata_for_tables();
        print_and_log(
            "metadata generated randomly using seed " +
                std::to_string(options->at(Option::INITIAL_SEED)->getInt()),
            this);
      }
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
