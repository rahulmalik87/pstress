/* Table construction: random columns and indexes, the CREATE TABLE
   definition, primary key helpers and metadata generation. */

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
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>

void validate_secondary_engine(Thd1 *thd) {
  if (strcmp(FORK, "Duckdb") || strcmp(FORK, "ClickHouse") == 0) {
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
#ifdef USE_CLICKHOUSE
  projections_ = new std::vector<Projection *>;
#endif
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

Table::~Table() {
  for (auto ind : *indexes_)
    delete ind;
  for (auto col : *columns_) {
    col->mutex.lock();
    delete col;
  }
  delete columns_;
  delete indexes_;
#ifdef USE_CLICKHOUSE
  for (auto proj : *projections_)
    delete proj;
  delete projections_;
#endif
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
  std::string name = table_prefix() + std::to_string(id);
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

#ifdef USE_CLICKHOUSE
  /* rolled once per table and persisted, so DropCreate and the next step
     recreate this table with the same settings */
  table->settings = pick_table_settings();
#endif

  table->CreateDefaultColumn();
  table->CreateDefaultIndex();
#ifdef USE_CLICKHOUSE
  table->RollSkipIndexes();
#endif
  table->resolve_composite_key();
  table->resolve_seq_pk_column();

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

Column *Table::pk_column() const {
  for (const auto &col : *columns_) {
    if (col->primary_key)
      return col;
  }
  return nullptr;
}

/* Decide once, when the table is built or restored, whether --seq-pk drives
   this table's primary key. Eagerly and not on demand: worker threads share
   the table, so a lazy resolve would be a data race. */
void Table::resolve_seq_pk_column() {
  seq_pk_col = nullptr;
  if (!options->at(Option::SEQ_PK)->getBool())
    return;
  Column *pk = pk_column();
  if (pk == nullptr)
    return;
  /* An auto-increment key is the server's counter, not ours. A key that is
     also the partition column would send every new row past the last defined
     partition (MySQL only -- ClickHouse forces --no-partition). */
  if (pk->auto_increment || pk->is_partition)
    return;
  switch (pk->type_) {
  case Column::INT:
  case Column::INTEGER:
    seq_pk_col = pk;
    break;
  case Column::VARCHAR:
  case Column::CHAR:
    /* A string key is zero-padded so that it sorts the way it counts, which
       needs room for the padding. A column too narrow to hold it keeps random
       values rather than getting keys that sort wrongly. */
    if (pk->length >= seq_pk_pad_width)
      seq_pk_col = pk;
    break;
  default:
    break;
  }
}

/* A key that may be in the table right now. Never an allocation: this feeds
   predicates, and the grammar-SQL substitution calls rand_value() on every
   column of every table it touches, so allocating here would race the counter
   far ahead of the data. */
long int Table::sample_pk(long int upper_exclusive) const {
  long int hi = pk_next.load(std::memory_order_relaxed); /* exclusive */
  if (upper_exclusive > 0 && upper_exclusive < hi)
    hi = upper_exclusive;
  if (hi <= 1)
    return 1; /* nothing handed out yet: a predicate that matches nothing */
  long int lo = 1;
  if (rand_int(100, 1) <= options->at(Option::SEQ_PK_RECENT_PROB)->getInt()) {
    /* the newest keys, still in small unmerged parts */
    const long int tail = options->at(Option::SEQ_PK_RECENT_ROWS)->getInt();
    if (tail > 0 && hi - lo > tail)
      lo = hi - tail;
  }
  return rand_int(hi - 1, lo);
}

/* Which columns join the primary key beyond the enforcing one -- and on
   ClickHouse the ORDER BY with it, since the two are the same list there.

   Rolled here rather than inside definition(), which used to do it: definition()
   runs again for DropCreate and at every later step, so a fresh roll there
   handed the server a key the metadata did not describe, and nothing downstream
   could tell which columns were in it. The eligibility tests are the ones that
   condition carried. */
void Table::resolve_composite_key() {
#ifdef USE_CLICKHOUSE
  /* A table with no primary key at all -- AddTable builds these -- still needs
     an ORDER BY, and definition() falls back to the first column. That makes it
     a key column on the server while no column is flagged here, so an UPDATE
     picks it and ClickHouse rejects the whole statement with "Cannot UPDATE key
     column". Record it, before the probability check, because the fallback is
     not a roll: it always happens. Nothing else changes -- definition() emits
     PRIMARY KEY only when has_pk(), so this column stays out of that clause and
     only the ORDER BY fallback puts it in the key. */
  if (!has_pk() && !columns_->empty())
    columns_->at(0)->composite_key = true;
#endif
  const long int prob = options->at(Option::COMPOSITE_KEY_PROB)->getInt();
  if (prob <= 0)
    return;
  /* A secondary-engine table whose key is the server's auto-increment counter
     keeps the narrow key the original condition insisted on. */
  if (!options->at(Option::SECONDARY_ENGINE)->getString().empty() &&
      has_auto_inc_col())
    return;
  for (auto col : *columns_) {
    /* the enforcing key is already in; a partition column is added by
       definition() whether it rolls or not, so leave that to it */
    if (col->primary_key || col->is_partition)
      continue;
    if (col->type_ == Column::BLOB || col->type_ == Column::JSON ||
        col->type_ == Column::TEXT)
      continue;
    /* a nullable key sorts nulls first and needs allow_nullable_key; the
       original condition avoided it and so do we */
    if (col->null_val)
      continue;
    if (rand_int(100, 1) <= prob)
      col->composite_key = true;
  }
}

std::string Table::pk_literal(long int value, const Column *col) const {
  std::string digits = std::to_string(value);
  if (col->type_ == Column::INT || col->type_ == Column::INTEGER)
    return digits;
  /* Pad to a fixed width so lexicographic order matches numeric order, and
     quote it -- a String column given a bare number would be stored as the
     unpadded text and would then sort among the padded keys arbitrarily. */
  if (digits.size() < static_cast<size_t>(seq_pk_pad_width))
    digits.insert(0, seq_pk_pad_width - digits.size(), '0');
#ifdef USE_MYSQL
  return "\"" + digits + "\"";
#else
  return "'" + digits + "'";
#endif
}

/* prepare table definition */
std::string Table::definition(bool with_index, bool with_fk,
                              bool with_forced_secondary) {
  std::string def = "CREATE";
  if (type == TEMPORARY)
    def += " TEMPORARY";
#ifdef USE_CLICKHOUSE
  def += " TABLE IF NOT EXISTS " + name_ + " (";
#else
  def += " TABLE " + name_ + " (";
#endif

  assert(columns_->size() > 0);

  /* add columns */
  for (auto col : *columns_) {
    def += col->definition() + ", ";
  }
#ifdef USE_CLICKHOUSE
  /* version column for ReplacingMergeTree — never dropped, and present unless
     --no-version-column */
  if (ch_version_column())
    def += "_pstress_ver UInt64, ";
#endif


  /* if column has primary key */
  std::vector<std::string> pk_cols; /* all PK columns, including composite */
  if (has_pk()) {
    def += " PRIMARY KEY(";
    for (auto col : *columns_) {
      if (col->primary_key) {
        def += col->name_ + ", ";
        pk_cols.push_back(col->name_);
      }
    }
    /* resolve_composite_key() already decided this, once, when the table was
       built. Rolling it here would give a different key every time definition()
       runs -- DropCreate, --step 2 -- and leave the metadata describing a table
       the server does not have. */
    for (auto col : *columns_) {
      if (col->primary_key)
        continue;
      if (col->is_partition || col->composite_key) {
        def += col->name_ + ", ";
        pk_cols.push_back(col->name_); /* track composite key cols too */
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
#ifdef USE_CLICKHOUSE
  /* Whatever with_index says: the text indexes are part of the table, and
     Table::load() creates it with with_index = false. Copied under the lock,
     --add-text-index and --drop-text-index change the list while the run is
     going. */
  {
    std::shared_lock<std::shared_mutex> lock(table_mutex);
    for (const auto &ti : skip_indexes_)
      def += ti.definition() + ", ";
  }
#endif


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

  if (!engine.empty()) {
#ifdef USE_CLICKHOUSE
    def += " ENGINE=" + ch_resolve_engine(engine);
#else
    def += " ENGINE=" + engine;
#endif
  }

#ifdef USE_CLICKHOUSE
  if (!engine.empty() && !columns_->empty()) {
    /* ORDER BY must be a superset of PRIMARY KEY (PK must be a prefix).
       Use pk_cols which captured every column added to PRIMARY KEY above,
       including composite key extras whose primary_key flag is not set. */
    std::string order_cols;
    for (const auto &c : pk_cols) {
      if (!order_cols.empty()) order_cols += ", ";
      order_cols += c;
    }
    if (order_cols.empty())
      order_cols = columns_->at(0)->name_;
    /* A storage ORDER BY is one expression, so more than one column has to be
       a tuple: bare "ORDER BY a, b" is a syntax error at the comma. Same shape
       the projection body uses. */
    def += " ORDER BY " +
           (pk_cols.size() > 1 ? "(" + order_cols + ")" : order_cols) +
           " SETTINGS enable_block_number_column = 1,"
           " enable_block_offset_column = 1";
    if (g_projections_enabled) {
      /* ReplacingMergeTree, which is pstress's default --engine, rejects a
         projection outright under the default throw mode, and a lightweight
         DELETE on a projected table fails the same way. rebuild is the only
         mode that keeps the projection agreeing with the base table: drop makes
         it vanish behind pstress's back and ignore leaves it stale, and either
         would land as a replica checksum or projection oracle mismatch that is
         not a bug.

         Rolled here rather than in the settings pool on purpose. The pool rolls
         per table against a probability, so some tables would reject every
         projection statement and the run would look healthy while testing
         nothing. */
      def += ", deduplicate_merge_projection_mode = 'rebuild'";
      def += ", lightweight_mutation_projection_mode = 'rebuild'";
      /* --null-prob defaults to 1, so every column but the primary key is
         Nullable. Without this a projection could only ever sort by the primary
         key, which is the table's own ORDER BY, and would be a projection the
         optimizer never prefers. The table's own ORDER BY never uses a nullable
         column, so nothing else changes. */
      def += ", allow_nullable_key = 1";
    }
    if (g_text_index_phrase_search_enabled)
      def += ", allow_experimental_text_index_phrase_search = 1";
    /* per-table settings from --table-settings-file / --table-settings.
       Copied under the lock rather than read in place: --modify-table-setting
       rewrites this string while the run is going, and none of definition()'s
       three callers holds table_mutex. */
    std::string current_settings;
    {
      std::shared_lock<std::shared_mutex> lock(table_mutex);
      current_settings = settings;
    }
    if (!current_settings.empty())
      def += ", " + current_settings;
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
