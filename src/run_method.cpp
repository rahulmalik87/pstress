
#include "random_test.hpp"
#include <regex>
#include <thread>
extern std::vector<Table *> *all_tables;
extern std::mutex all_table_mutex;
extern thread_local std::mt19937 rng;
extern std::atomic_flag lock_stream;
extern std::atomic<bool> run_query_failed;
extern std::atomic<bool> keyring_comp_status;
extern std::vector<std::string> g_undo_tablespace;
static void random_timezone(Thd1 *thd) {
  std::string time_zone;
  std::string time_zone_list[] = {
      "-12:00", "-11:00", "-10:00", "-09:00", "-08:00", "-07:00", "-06:00",
      "-05:00", "-04:00", "-03:00", "-02:00", "-01:00", "+00:00", "+01:00",
      "+02:00", "+03:00", "+04:00", "+05:00", "+06:00", "+07:00", "+08:00",
      "+09:00", "+10:00", "+11:00", "+12:00"};
  time_zone = time_zone_list[rand_int(9)];
  std::string sql = "SET time_zone='" + time_zone + "'";
  execute_sql(sql, thd);
}

static void kill_query(Thd1 *thd) {

  auto on_exit = std::shared_ptr<void>(nullptr, [&](...) {
    if (options->at(Option::SELECT_IN_SECONDARY)->getBool()) {
      execute_sql("SET @@SESSION.USE_SECONDARY_ENGINE=FORCED", thd);
    }
  });

  if (options->at(Option::SELECT_IN_SECONDARY)->getBool()) {
    execute_sql("SET @@SESSION.USE_SECONDARY_ENGINE=OFF", thd);
  }
  unsigned long current_thread_id = mysql_thread_id(thd->conn);
  std::string query =
      "select ID from information_schema.processlist where user='";
  query += options->at(Option::USER)->getString() + "'";
  /* the Sleep is added so it doesn't kill a query which is about to kill */
  query += " and command not like  '%Sleep%' and id != " +
           std::to_string(current_thread_id);
  if (!execute_sql(query, thd)) {
    return;
  }
  auto result = get_query_result(thd, query);
  if (result.empty()) {
    return;
  }
  auto id = result[rand_int(result.size() - 1)][0];
  query = "kill " + id;
  if (!execute_sql(query, thd)) {
    return;
  }
  return;
}

static std::string getExecutablePath() {
  char buffer[PATH_MAX];
#ifdef _WIN32
  GetModuleFileNameA(NULL, buffer, PATH_MAX);
#else
  ssize_t len = readlink("/proc/self/exe", buffer, PATH_MAX);
  if (len == -1) {
    throw std::runtime_error("Unable to get executable path");
  }
  buffer[len] = '\0';
#endif
  return std::string(buffer);
}

/* load special sql from a file and return*/
static std::vector<grammar_tables> load_grammar_sql_from(Thd1 *thd) {
  std::vector<std::string> statments;
  std::vector<grammar_tables> tables;
  auto grammarFileName = opt_string(GRAMMAR_FILE);
  auto exePath = getExecutablePath();
  std::filesystem::path exeDir = std::filesystem::path(exePath).parent_path();

  // Construct the full path to grammar.sql in the same directory
  std::filesystem::path grammarFilePath = exeDir / grammarFileName;

  std::ifstream myfile(grammarFilePath);
  if (!myfile.is_open()) {
    print_and_log("Unable to find grammar file " + grammarFileName, thd);
    return tables;
  }

  /* Push it into vector */
  while (!myfile.eof()) {
    std::string sql;
    getline(myfile, sql);
    /* remove white spaces or ; at the end */
    sql = std::regex_replace(sql, std::regex(R"(\s+$)"), "");
    if (sql.find_first_not_of(" \t") == std::string::npos)
      continue;
    sql = sql.substr(sql.find_first_not_of(" \t"));
    if (sql.empty() || sql[0] == '#')
      continue;
    statments.push_back(sql);
  }
  myfile.close();

  for (auto &sql : statments) {
    /* Parse the grammar SQL and create a table object */
    std::vector<grammar_table> sql_tables;
    int tab_sql = 1; // start with 1
    do {             // search for table
      std::smatch match;
      std::string tab_p = "T" + std::to_string(tab_sql++); // table pattern

      if (regex_search(sql, match, std::regex(tab_p))) {

        auto add_columns = [&](grammar_table::sql_col_types type) {
          int col_sql = 1;
          do {
            std::string col_p = tab_p + "_" +
                                grammar_table::get_col_type(type) + "_" +
                                std::to_string(col_sql);
            if (regex_search(sql, match, std::regex(col_p))) {
              sql_tables.back().column_count.at(type)++;
              col_sql++;
            } else {
              break;
            }
          } while (true);
        };

        sql_tables.emplace_back(tab_p);

        for (auto &type : grammar_table::get_vector_of_col_type()) {
          add_columns(type);
        }
      } else
        // if no more table found,
        break;
    } while (true);
    tables.emplace_back(sql, sql_tables);
  }
  return tables;
}

/* Read the grammar file and execute the sql */
static void grammar_sql(Thd1 *thd, Table *enforce_table) {

  static auto all_tables_from_grammar = load_grammar_sql_from(thd);

  if (all_tables_from_grammar.size() == 0)
    return;

  auto currrent_table =
      all_tables_from_grammar.at(rand_int(all_tables_from_grammar.size() - 1));

  if (options->at(Option::COMPARE_RESULT)->getBool() ||
      options->at(Option::SELECT_IN_SECONDARY)->getBool()) {
    execute_sql("COMMIT", thd);
  }
  auto sql = currrent_table.sql;
  auto &sql_tables = currrent_table.tables;

  // Find the real table and columns
  for (auto &table : sql_tables) {
    int table_check = 100; // try to find table
    do {
      std::unique_lock<std::mutex> lock(all_table_mutex);
      auto working_table = all_tables->at(rand_int(all_tables->size() - 1));
      /* if we are running DML, lets enforce so it compare result
      if (options->at(Option::COMPARE_RESULT)->getBool()) {
        working_table = enforce_table;
        table_check = 0;
      }
      */
      working_table->lock_table_mutex(thd->ddl_query);
      table.found_name = working_table->name_;

      auto columns = working_table->columns_;
      int column_check = 20; // max number of times to find column
      do {
        auto col = columns->at(rand_int(columns->size() - 1));
        auto col_type =
            grammar_table::get_col_type(col->col_type_to_string(col->type_));

        // if column is defined as NOT SECONDARY skip it
        if (col->not_secondary)
          continue;

        // if a valid column is not found in the table
        if (col_type == grammar_table::MAX)
          continue;

        if (table.column_count.at(col_type) > 0 &&
            table.column_count.at(col_type) !=
                (int)table.columns.at(col_type).size()) {
          table.columns.at(col_type).emplace_back(col->name_,
                                                  col->rand_value());
        }
      } while (column_check-- > 0 &&
               table.total_column_count() != table.total_column_written());

      working_table->unlock_table_mutex();

      if (table.total_column_count() != table.total_column_written())
        table.reset_columns();

    } while (table.total_column_count() != table.total_column_written() &&
             table_check-- > 0);

    if (table.total_column_count() != table.total_column_written()) {
      thd->thread_log << "Could not find table to execute SQL " << sql
                      << std::endl;
      return;
    }
  }

  /* replace the found column and table */
  for (const auto &table : sql_tables) {
    auto table_name = table.name;
    for (size_t i = 0; i < table.columns.size(); i++) {
      auto col = table.columns.at(i);
      for (size_t j = 0; j < col.size(); j++) {
        /* first replace the rand_value */
        sql = std::regex_replace(
            sql,
            std::regex(
                table_name + "_" +
                grammar_table::get_col_type((grammar_table::sql_col_types)i) +
                "_" + std::to_string(j + 1) + R"((=|!=|<>|>=|<=|>|<)RAND)"),
            table_name + "." + col.at(j).first + " $1 " + col.at(j).second);

        sql =
            std::regex_replace(sql,
                               std::regex(table_name + "_" +
                                          grammar_table::get_col_type(
                                              (grammar_table::sql_col_types)i) +
                                          "_" + std::to_string(j + 1)),
                               table_name + "." + col.at(j).first);
      }
    }
    /* Replace table_name when followed by space, closing parenthesis, or end of
     * line */
    sql = std::regex_replace(sql, std::regex(table_name + R"((\s|\)|$))"),
                             table.found_name + " " + table.name + "$1");
  }
  /* replace RAND_INT */
  for (int i = 0; i < 10; i++) {
    std::string randString = "RAND_INT_" + std::to_string(i);
    sql = std::regex_replace(sql, std::regex(randString),
                             std::to_string(rand_int(100)));
  }
  sql = std::regex_replace(sql, std::regex("RAND_INT"),
                           std::to_string(rand_int(100)));

  if (options->at(Option::COMPARE_RESULT)->getBool()) {
    enforce_table->Compare_between_engine(sql, thd);
  } else {
    if (!execute_sql(sql, thd, true)) {
      // print_and_log("Grammar SQL failed " + sql, thd, true);
    }
  }
}

/* create,alter,drop undo tablespace */
static void create_alter_drop_undo(Thd1 *thd) {
  if (g_undo_tablespace.size() == 0)
    return;
  auto x = rand_int(100);
  if (x < 20) {
    std::string name =
        g_undo_tablespace[rand_int(g_undo_tablespace.size() - 1)];
    std::string sql =
        "CREATE UNDO TABLESPACE " + name + " ADD DATAFILE '" + name + ".ibu'";
    execute_sql(sql, thd);
  }
  if (x < 40) {
    std::string sql = "DROP UNDO TABLESPACE " +
                      g_undo_tablespace[rand_int(g_undo_tablespace.size() - 1)];
    execute_sql(sql, thd);
  } else {
    std::string sql =
        "ALTER UNDO TABLESPACE " +
        g_undo_tablespace[rand_int(g_undo_tablespace.size() - 1)] + " SET ";
    sql += (rand_int(1) == 0 ? "ACTIVE" : "INACTIVE");
    execute_sql(sql, thd);
  }
}

/* alter instance enable disable redo logging */
static void alter_redo_logging(Thd1 *thd) {
  std::string sql = "ALTER INSTANCE ";
  sql += (rand_int(1) == 0 ? "DISABLE" : "ENABLE");
  sql += " INNODB REDO_LOG";
  execute_sql(sql, thd);
}

/* Create new table without new records */
static void AddTable(Thd1 *thd) {
  Table *table = nullptr;
  std::unique_lock<std::mutex> lock(all_table_mutex);
  int table_id = rand_int(options->at(Option::TABLES)->getInt(), 1);
  if (!options->at(Option::NO_FK)->getBool() &&
      options->at(Option::FK_PROB)->getInt() > rand_int(100)) {
    table = Table::table_id(Table::FK, table_id, true);
  } else {
    table = Table::table_id(Table::NORMAL, table_id, true);
  }
  lock.unlock();
  if (!execute_sql(table->definition(true, true), thd)) {
    return;
  }
  lock.lock();
  all_tables->push_back(table);
  lock.unlock();
}
extern std::atomic<bool> run_query_failed;
static void Sleepfor() {
  volatile double result = 0.0;            // volatile to prevent optimization
  for (long long i = 0; i < 150000; ++i) { // 1 billion iterations
    result += std::sin(i) * std::cos(i);   // some expensive math operations
  }

  return;
}

static bool is_query_blocked(Thd1 *thd, Option::Opt option) {
  auto thread_id = thd->thread_id;
  auto ddl_query = thd->ddl_query;

  if (options->at(Option::THREAD_DOING_ONLY_SELECT)->getInt() != 0) {
    if (options->at(Option::SINGLE_THREAD_DDL)->getBool() && thread_id == 1 &&
        ddl_query) {
      // do not block ddl query if user want single thread ddl
    } else if (option != Option::SELECT_ALL_ROW &&
               option != Option::SELECT_ROW_USING_PKEY &&
               option != Option::SELECT_FOR_UPDATE &&
               option != Option::SELECT_FOR_UPDATE_BULK &&
               option != Option::GRAMMAR_SQL &&
               option != Option::COMPARE_RESULT) {
      if (thread_id < options->at(Option::THREAD_DOING_ONLY_SELECT)->getInt()) {
        return true;
      }
    } else if (thread_id >=
               options->at(Option::THREAD_DOING_ONLY_SELECT)->getInt()) {
      // Only select queries: skip threads exceeding the allowed range
      return true;
    }
  }

  ddl_query = options->at(option)->ddl == true ? true : false;

  if (thread_id != 1 && options->at(Option::SINGLE_THREAD_DDL)->getBool() &&
      ddl_query == true)
    return true;

  return false;
}
/* return table pointer of matching table. This is only done during the
 * first step or during the prepare, so you would have only tables that are not
 * renamed*/
static Table *pick_table(Table::TABLE_TYPES type, int id) {
  std::lock_guard<std::mutex> lock(all_table_mutex);
  std::string name = TABLE_PREFIX + std::to_string(id);
  if (type == Table::FK) {
    name += FK_SUFFIX;
  } else if (type == Table::PARTITION) {
    name += PARTITION_SUFFIX;
  }
  for (auto const &table : *all_tables) {
    if (table->name_ == name)
      return table;
  }
  return nullptr;
}

static std::string checksum(const std::string &table, Thd1 *thd) {
  std::string sql = "CHECKSUM TABLE " + table;
  execute_sql(sql, thd);
  auto row = mysql_fetch_row_safe(thd);
  if (row && mysql_num_fields_safe(thd, 2))
    return row[1];
  return "";
}

bool Thd1::run_some_query() {
  // save current time in a variable so we have print time it took to execute

  /* set seed for current thread */
  rng = std::mt19937(set_seed(this));
  thread_log << " value of rand_int(500) " << rand_int(500) << std::endl;
  std::vector<Table::TABLE_TYPES> tableTypes = {Table::NORMAL, Table::FK,
                                                Table::PARTITION};
  if (options->at(Option::SECONDARY_ENGINE)->getString() != "") {
    execute_sql("SET SESSION sql_generate_invisible_primary_key = TRUE", this);
    execute_sql("SET SESSION sql_generate_invisible_unique_key = TRUE", this);
  }
  execute_sql("USE " + options->at(Option::DATABASE)->getString(), this);

  /* first create temporary tables metadata if requried */
  int temp_tables;
  if (options->at(Option::ONLY_TEMPORARY)->getBool())
    temp_tables = options->at(Option::TABLES)->getInt();
  else if (options->at(Option::NO_TEMPORARY)->getBool())
    temp_tables = 0;
  else
    temp_tables = options->at(Option::TABLES)->getInt() /
                  options->at(Option::TEMPORARY_PROB)->getInt();

  /* create temporary table */
  std::vector<Table *> *session_temp_tables = new std::vector<Table *>;
  for (int i = 0; i < temp_tables; i++) {

    Table *table = Table::table_id(Table::TEMPORARY, i);
    if (!table->load(this))
      return false;
    session_temp_tables->push_back(table);
  }

  static std::atomic<int> table_processed = 0;
  int starting_index = 1 + thread_id;
  thread_log << "Total table to process "
             << options->at(Option::TABLES)->getInt() << std::endl;

  if (options->at(Option::PREPARE)->getBool() ||
      options->at(Option::STEP)->getInt() == 1) {
    while (starting_index <= options->at(Option::TABLES)->getInt()) {
      for (const auto &tableType : tableTypes) {
        auto table = pick_table(tableType, starting_index);
        if (table == nullptr) {
          thread_log << "Table with index " << starting_index << " not found"
                     << std::endl;
          continue;
        }
        if (!table->load(this)) {
          return false;
        }
        thread_log << " checksum " + table->name_ + " " +
                          checksum(table->name_, this)
                   << std::endl;
      }
      thread_log << "Thread " << thread_id << " finished processing table "
                 << starting_index << std::endl;
      if (run_query_failed) {
        thread_log << "some other thread failed, Exiting. Please check logs "
                   << std::endl;
        return false;
      }
      table_processed++;
      starting_index += options->at(Option::THREADS)->getInt();
    }

    // wait for all tables to finish loading
    while (table_processed < options->at(Option::TABLES)->getInt()) {
      thread_log << "Waiting for all threds to finish processing. Total table "
                    "processed "
                 << table_processed << std::endl;
      std::chrono::seconds dura(1);
      if (run_query_failed) {
        thread_log << "Some other thread failed, Exiting. Please check logs "
                   << std::endl;
        return false;
      }
      std::this_thread::sleep_for(dura);
    }
  }
  /* table initial data is created, empty the unique_keys */
  this->unique_keys.resize(0);

  if (options->at(Option::JUST_LOAD_DDL)->getBool() ||
      options->at(Option::PREPARE)->getBool())
    return true;

  /*Print once on screen and in general logs */
  if (!lock_stream.test_and_set()) {
    print_and_log(
        "Starting load in " +
            std::to_string(options->at(Option::THREADS)->getInt()) +
            " threads. GTID " +
            mysql_read_single_value("select @@global.gtid_executed", this),
        this);
  }

  if (options->at(Option::SELECT_IN_SECONDARY)->getBool()) {
    execute_sql("SET @@SESSION.USE_SECONDARY_ENGINE=FORCED", this);
  }

  auto sec = opt_int(NUMBER_OF_SECONDS_WORKLOAD);
  auto begin = std::chrono::system_clock::now();
  auto end =
      std::chrono::system_clock::time_point(begin + std::chrono::seconds(sec));


  /* freqency of all options per thread */
  int opt_feq[Option::MAX][2] = {{0, 0}};

  static auto savepoint_prob = options->at(Option::SAVEPOINT_PRB_K)->getInt();

  int trx_left = 0;
  int current_save_point = 0;
  if (options->at(Option::SELECT_IN_SECONDARY)->getBool()) {
    execute_sql(" SET @@SESSION.USE_SECONDARY_ENGINE=FORCED ", this);
  }

  int pick_table_id = 0;
  // save the start time in a variable so in every N seconds we print the number
  // of transaction processed
  auto start_time_for_print = std::chrono::system_clock::now();
  while (std::chrono::system_clock::now() < end) {
    auto option = pick_some_option();

    if (is_query_blocked(this, option)) {
      continue;
    }

    /* check if we need to make sql as part of existing or new trx */
    if (trx_left > 0) {

      trx_left--;

      if (trx_left == 0 || ddl_query == true) {
        if (trx == NON_XA) {
          if (rand_int(100, 1) > options->at(Option::COMMIT_PROB)->getInt()) {
            execute_sql("ROLLBACK", this);
          } else {
            execute_sql("COMMIT", this);
          }
          current_save_point = 0;
        } else {
          execute_sql("XA END " + get_xid(), this);
          execute_sql("XA PREPARE " + get_xid(), this);
          if (rand_int(100, 1) > options->at(Option::COMMIT_PROB)->getInt()) {
            execute_sql("XA ROLLBACK " + get_xid(), this);
          } else {
            execute_sql("XA COMMIT " + get_xid(), this);
          }
        }
      } else if (trx == NON_XA) {
        if (rand_int(1000) < savepoint_prob &&
            options->at(Option::COMMIT_PROB)->getInt() < 100) {
          current_save_point++;
          execute_sql("SAVEPOINT SAVE" + std::to_string(current_save_point),
                      this);
        }

        /* 10% chances of rollbacking to savepoint */
        if (current_save_point > 0 && rand_int(10) == 1 &&
            options->at(Option::COMMIT_PROB)->getInt() < 100) {
          auto sv = rand_int(current_save_point, 1);
          execute_sql("ROLLBACK TO SAVEPOINT SAVE" + std::to_string(sv), this);
          current_save_point = sv - 1;
        }
      }
    }

    if (trx_left == 0) {
      if (rand_int(1000) < options->at(Option::TRANSATION_PRB_K)->getInt()) {
        execute_sql("START TRANSACTION", this);
        trx_left =
            rand_int(options->at(Option::TRANSACTIONS_SIZE)->getInt(), 1);
        trx = NON_XA;
      } else if (rand_int(1000) <
                 options->at(Option::XA_TRANSACTION)->getInt()) {
        execute_sql("XA START " + get_xid(), this);
        trx_left =
            rand_int(options->at(Option::TRANSACTIONS_SIZE)->getInt(), 1);
        trx = XA;
      }
    }

    std::unique_lock<std::mutex> lock(all_table_mutex);
    if (options->at(Option::THREAD_PER_TABLE)->getBool()) {
      pick_table_id = thread_id;
      if (pick_table_id >= (int)all_tables->size()) {
        pick_table_id = rand_int(all_tables->size() - 1);
      }
    } else {
      pick_table_id = rand_int(all_tables->size() - 1);
    }
    /* todo enable temporary table are disabled */
    auto table = all_tables->at(pick_table_id);
    lock.unlock();


    switch (option) {
    case Option::DROP_INDEX:
      table->DropIndex(this);
      break;
    case Option::ADD_INDEX:
      table->AddIndex(this);
      break;
    case Option::DROP_COLUMN:
      table->DropColumn(this);
      break;
    case Option::ADD_COLUMN:
      table->AddColumn(this);
      break;
    case Option::MODIFY_COLUMN_SECONDARY_ENGINE:
      table->ModifyColumnSecondaryEngine(this);
      break;
    case Option::TRUNCATE:
      table->Truncate(this);
      break;
    case Option::DROP_CREATE:
      table->DropCreate(this);
      break;
    case Option::ENFORCE_MERGE:
      table->EnforceRebuildInSecondary(this);
      break;
    case Option::SECONDARY_GC:
      execute_sql(
          "SET GLOBAL " + options->at(Option::SECONDARY_ENGINE)->getString() +
              " PRAGMA = \"" + lower_case_secondary() + "_garbage_collect\"",
          this);
      break;
    case Option::ALTER_TABLE_ENCRYPTION:
      table->SetEncryption(this);
      break;
    case Option::ALTER_TABLE_COMPRESSION:
      table->SetTableCompression(this);
      break;
    case Option::ALTER_COLUMN_MODIFY:
      table->ModifyColumn(this);
      break;
    case Option::SET_GLOBAL_VARIABLE:
      set_mysqld_variable(this);
      break;
    case Option::ALTER_TABLESPACE_ENCRYPTION:
      alter_tablespace_encryption(this);
      break;
    case Option::ALTER_DISCARD_TABLESPACE:
      table->Alter_discard_tablespace(this);
      break;
    case Option::ALTER_TABLESPACE_RENAME:
      alter_tablespace_rename(this);
      break;
    case Option::SELECT_ALL_ROW:
      table->SelectAllRow(this);
      break;
    case Option::SELECT_ROW_USING_PKEY:
      table->SelectRandomRow(this);
      break;
    case Option::THROTTLE_SLEEP:
      Sleepfor();
      break;
    case Option::SELECT_FOR_UPDATE:
      table->SelectRandomRow(this, true);
      break;
    case Option::SELECT_FOR_UPDATE_BULK:
      table->SelectAllRow(this, true);
      break;
    case Option::INSERT_RANDOM_ROW:
      table->InsertRandomRow(this);
      break;
    case Option::INSERT_BULK:
      table->InsertRandomRowBulk(this);
      break;
    case Option::DELETE_ALL_ROW:
      table->DeleteAllRows(this);
      break;
    case Option::DELETE_ROW_USING_PKEY:
      table->DeleteRandomRow(this);
      break;
    case Option::UPDATE_ROW_USING_PKEY:
      table->UpdateRandomROW(this);
      break;
    case Option::CALL_FUNCTION:
      table->CreateFunction(this);
      break;
    case Option::UPDATE_ALL_ROWS:
      table->UpdateAllRows(this);
      break;
    case Option::OPTIMIZE:
      table->Optimize(this);
      break;
    case Option::CHECK_TABLE:
      table->Check(this);
      break;
    case Option::ADD_NEW_TABLE:
      AddTable(this);
      break;
    case Option::ADD_DROP_PARTITION:
      if (table->type == Table::PARTITION)
        static_cast<Partition *>(table)->AddDrop(this);
      break;
    case Option::ANALYZE:
      table->Analyze(this);
      break;
    case Option::RENAME_COLUMN:
      table->ColumnRename(this);
      break;
    case Option::RENAME_INDEX:
      table->IndexRename(this);
      break;
    case Option::ALTER_MASTER_KEY:
      execute_sql("ALTER INSTANCE ROTATE INNODB MASTER KEY", this);
      break;
    case Option::ALTER_ENCRYPTION_KEY:
      execute_sql("ALTER INSTANCE ROTATE INNODB SYSTEM KEY " +
                      std::to_string(rand_int(9)),
                  this);
      break;
    case Option::ALTER_GCACHE_MASTER_KEY:
      execute_sql("ALTER INSTANCE ROTATE GCACHE MASTER KEY", this);
      break;
    case Option::ALTER_INSTANCE_RELOAD_KEYRING:
      if (keyring_comp_status)
        execute_sql("ALTER INSTANCE RELOAD KEYRING", this);
      break;
    case Option::ROTATE_REDO_LOG_KEY:
      execute_sql("SELECT rotate_system_key(\"percona_redo\")", this);
      break;
    case Option::ALTER_REDO_LOGGING:
      alter_redo_logging(this);
      break;
    case Option::ALTER_DATABASE_ENCRYPTION:
      alter_database_encryption(this);
      break;
    case Option::ALTER_DATABASE_COLLATION:
      alter_database_collation(this);
      break;
    case Option::UNDO_SQL:
      create_alter_drop_undo(this);
      break;
    case Option::GRAMMAR_SQL:
      grammar_sql(this, table);
      break;
    case Option::ALTER_SECONDARY_ENGINE:
      table->SetSecondaryEngine(this);
      break;
    case Option::KILL_TRANSACTION:
      kill_query(this);
      break;
    case Option::RANDOM_TIMEZONE:
      random_timezone(this);
      break;
    default:
      print_and_log("Unhandled option " + options->at(option)->help, this);
      exit(EXIT_FAILURE);
      break;
    }

    options->at(option)->total_queries++;

    /* sql executed is at 0 index, and if successful at 1 */
    opt_feq[option][0]++;
    if (options->at(Option::PRINT_TRANSACTION_RATE)->getInt() > 0) {
      options->at(option)->query_in_timespan++;
    }
    if (success) {
      options->at(option)->success_queries++;
      opt_feq[option][1]++;
      success = false;
    }
    if (options->at(Option::PRINT_TRANSACTION_RATE)->getInt() > 0 and
        thread_id == 0) {
      auto current_time = std::chrono::system_clock::now();
      auto time_span = std::chrono::duration_cast<std::chrono::seconds>(
          current_time - start_time_for_print);
      if (time_span.count() >=
          options->at(Option::PRINT_TRANSACTION_RATE)->getInt()) {
        for (auto option : *options) {
          if (option == nullptr)
            continue;
          auto current_value = option->query_in_timespan.load();
          if (current_value > 0) {
            std::cout << option->short_help << "=>" << current_value << ",";
            option->query_in_timespan.store(0);
          }
        }
        std::cout << std::endl;
        start_time_for_print = std::chrono::system_clock::now();
      }
    }

    if (run_query_failed) {
      thread_log << "Some other thread failed, Exiting. Please check logs "
                 << std::endl;
      break;
    }
  } // while

  /* print options frequency in logs */
  for (int i = 0; i < Option::MAX; i++) {
    if (opt_feq[i][0] > 0)
      thread_log << options->at(i)->help << ", total=>" << opt_feq[i][0]
                 << ", success=> " << opt_feq[i][1] << std::endl;
  }

  /* cleanup session temporary tables tables */
  for (auto &table : *session_temp_tables)
    if (table->type == Table::TEMPORARY)
      delete table;

  delete session_temp_tables;
  return true;
}
