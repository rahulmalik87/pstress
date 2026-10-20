/* DDL under load: DROP/CREATE, OPTIMIZE, CHECK, TRUNCATE, partitions,
   ALTER TABLE column and index actions, and MySQL instance level alters. */

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
  mark_mv_source_mutated("the table was dropped and recreated");
  /* after the DROP rather than after the CREATE: the rows are gone either
     way, and the CREATE below has several fallback forms. */
  reset_pk_counter();

  if (set_session_nbo) {
    execute_sql("SET SESSION wsrep_osu_method=DEFAULT ", thd);
  }
  std::string def = definition(true, true, true);
#ifdef USE_CLICKHOUSE
  /* The projections went with the table and definition() does not re-emit
     them, so drop them from the registry too; --add-projection makes new
     ones. */
  {
    lock_table_mutex(thd->ddl_query);
    ForgetProjections();
    unlock_table_mutex();
  }
#endif
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
    if (execute_sql(sql, thd))
      mark_mv_source_mutated("TRUNCATE PARTITION");
  } else {
    if (execute_sql("TRUNCATE TABLE " + name_, thd)) {
      mark_mv_source_mutated("TRUNCATE TABLE");
      /* the rows are gone, so the key sequence starts over. Deliberately not
         done in the TRUNCATE PARTITION branch above: that empties one
         partition and leaves the rest of the keys in place. */
      reset_pk_counter();
    }
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
        mark_mv_source_mutated("a partition was added or dropped");
        lock_table_mutex(thd->ddl_query);
        number_of_part += new_partition;
        unlock_table_mutex();
      }
    } else {
      if (execute_sql("ALTER TABLE " + name_ + pick_algorithm_lock() +
                          ", COALESCE PARTITION " +
                          std::to_string(new_partition),
                      thd)) {
        mark_mv_source_mutated("a partition was added or dropped");
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
        mark_mv_source_mutated("a partition was added or dropped");
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
        mark_mv_source_mutated("a partition was added or dropped");
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
        mark_mv_source_mutated("a partition was added or dropped");
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
        mark_mv_source_mutated("a partition was added or dropped");
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

#ifdef USE_CLICKHOUSE
  /* to tell a real retype from a length change that ClickHouse ignores */
  const std::string ch_type_before = ch_stored_type(col->clause());
#endif

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
  /* Use Nullable() wrapper for nullable columns, matching CREATE TABLE DDL.
     If null-prob=0, skip Nullable wrapper. */
  bool ch_mod_nullable = col->null_val && options->at(Option::NULL_PROB)->getInt() > 0;
  std::string ch_type = ch_mod_nullable ? "Nullable(" + col->clause() + ")" : col->clause();
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
#ifdef USE_CLICKHOUSE
    /* Only a real retype matters: the column then has a different type in the
       table while every view over it keeps the type it was created with, so the
       same value can serialise differently on the two sides. A VARCHAR length
       change reaches the server but leaves the stored String alone, and treating
       it as a retype would strand every view on the table for nothing. */
    if (ch_stored_type(col->clause()) != ch_type_before)
      mark_mv_source_mutated("MODIFY COLUMN retyped a column");
#endif
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
#ifdef USE_CLICKHOUSE
  /* never drop the version column used by ReplacingMergeTree */
  if (name == "_pstress_ver") {
    unlock_table_mutex();
    return;
  }
  /* a projection over this column would make the DROP fail outright */
  if (!DropProjectionsOnColumn(thd, name)) {
    unlock_table_mutex();
    return;
  }
  /* and so would a text index */
  if (!DropSkipIndexesOnColumn(thd, name)) {
    unlock_table_mutex();
    return;
  }
#endif

  std::string sql = "ALTER TABLE " + name_ + " DROP COLUMN " + name;

  sql += pick_algorithm_lock();
#ifdef USE_CLICKHOUSE
  if (options->at(Option::CH_MUTATIONS_SYNC)->getBool())
    sql += " SETTINGS mutations_sync = 2";
#endif
  unlock_table_mutex();

  if (execute_sql(sql, thd)) {
    mark_mv_source_mutated("DROP COLUMN");
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
#ifdef USE_CLICKHOUSE
  if (options->at(Option::CH_MUTATIONS_SYNC)->getBool())
    sql += " SETTINGS mutations_sync = 2";
#endif

  unlock_table_mutex();

  if (execute_sql(sql, thd)) {
    lock_table_mutex(thd->ddl_query);
    auto add_new_column =
        true; // check if there is already a column with this name
    for (auto col : *columns_) {
      if (col->name_.compare(tc->name_) == 0)
        add_new_column = false;
    }

    /* capture before unlock — tc may be deleted below */
    std::string added_name = tc->name_;
#ifdef USE_CLICKHOUSE
    Column::COLUMN_TYPES added_type = tc->type_;
    int added_length = tc->length;
#endif

    if (add_new_column)
      AddInternalColumn(tc);
    else
      delete tc;

    unlock_table_mutex();

#ifdef USE_CLICKHOUSE
    if (add_new_column &&
        options->at(Option::CH_ADD_COLUMN_BACKFILL)->getBool()) {
      /* Generate a client-side literal value for the backfill.
         ClickHouse rejects non-deterministic server-side functions
         (rand64, today, now…) in mutations on Replicated tables.
         Using a constant literal avoids that restriction entirely. */
      std::string rand_expr;
      switch (added_type) {
      case Column::INT:
      case Column::INTEGER:
        rand_expr = std::to_string(rand_int(1000000));
        break;
      case Column::FLOAT:
        rand_expr = rand_float(1000);
        break;
      case Column::DOUBLE:
        rand_expr = rand_double(1000.0);
        break;
      case Column::BOOL:
        rand_expr = rand_int(1) == 1 ? "1" : "0";
        break;
      case Column::DATE:
        rand_expr = "'" + rand_date() + "'";
        break;
      case Column::DATETIME:
      case Column::TIMESTAMP:
        rand_expr = "'" + rand_timestamp(0, false) + "'";
        break;
      default: /* VARCHAR, CHAR, TEXT, BLOB, etc. */
        rand_expr =
            "'" + rand_string(added_length > 0 ? added_length : 10) + "'";
        break;
      }
      std::string backfill = "ALTER TABLE " + name_ + " UPDATE " +
                             added_name + " = " + rand_expr + " WHERE 1=1";
      if (options->at(Option::CH_MUTATIONS_SYNC)->getBool())
        backfill += " SETTINGS mutations_sync = 2";
      execute_sql(backfill, thd);
    }
#endif
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
#ifdef USE_CLICKHOUSE
  /* ClickHouse rejects a rename of a column a projection references. Drop those
     projections rather than rewriting the stored body text: the body is kept
     verbatim so MODIFY PROJECTION can restate it byte for byte, and a textual
     substitution would have to reproduce ClickHouse's own formatting exactly.
     --add-projection makes fresh ones over the renamed column. */
  if (!DropProjectionsOnColumn(thd, name)) {
    unlock_table_mutex();
    return;
  }
#endif
  std::string sql =
      "ALTER TABLE " + name_ + " RENAME COLUMN " + name + " To " + new_name;
  sql += pick_algorithm_lock();
  unlock_table_mutex();
  if (execute_sql(sql, thd)) {
    mark_mv_source_mutated("RENAME COLUMN");
    lock_table_mutex(thd->ddl_query);
    for (auto &col : *columns_) {
      if (col->name_.compare(name) == 0)
        col->name_ = new_name;
    }
#ifdef USE_CLICKHOUSE
    RenameSkipIndexColumn(name, new_name);
#endif
    unlock_table_mutex();
  }
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
