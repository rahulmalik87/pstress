/* DML under load: SELECT, INSERT helpers, UPDATE, DELETE, REPLACE, the
   WHERE builders, prepared SELECTs and ClickHouse ALTER UPDATE/DELETE. */

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

static bool should_use_prepared_select() {
  auto ps_prob = options->at(Option::PREPARED_STMT_PROB)->getInt();
  return ps_prob > 0 && rand_int(100, 1) <= ps_prob;
}

static bool execute_prepared_select(
    const Table::PreparedSelect &prepared_select, Thd1 *thd) {
  static auto log_all = opt_bool(LOG_ALL_QUERIES);
  static auto log_success = opt_bool(LOG_SUCCEDED_QUERIES);

  std::string template_id;
  unsigned long long execute_count = 0;
  bool prepared_now = false;
  auto res = thd->db->execute_prepared_query(
      prepared_select.prepared_sql, prepared_select.params, &template_id,
      &execute_count, &prepared_now);

  if (!res) {
    thd->thread_log << "PS_FALLBACK " << thd->db->get_error() << " "
                    << prepared_select.printable_sql << std::endl;
    return execute_sql(prepared_select.printable_sql, thd);
  }

  thd->performed_queries_total++;
  thd->max_con_fail_count = 0;
  thd->success = true;
  if (prepared_now) {
    thd->thread_log << "PS_PREPARE " << template_id << " "
                    << prepared_select.prepared_sql << std::endl;
  }
  thd->thread_log << "PS_EXECUTE " << template_id << " reuse="
                  << execute_count << " " << prepared_select.printable_sql
                  << ", rows:" << thd->db->get_affected_rows() << std::endl;

  if (log_all || log_success) {
    thd->thread_log << "S [prepared] " << prepared_select.printable_sql
                    << ", rows:" << thd->db->get_affected_rows()
                    << std::endl;
  } else {
    thd->query_buffer.push("S [prepared] " + prepared_select.printable_sql);
  }
  return true;
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
  /* MySQL's "t PARTITION (p0, p1)" has no ClickHouse equivalent; a CH_KEY
     table is reached through a WHERE on its partition column instead */
  if (ch_partitioned(this))
    return sql;
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

#ifdef USE_CLICKHOUSE
/* A text search predicate over a column that has a text index: the functions
   the index serves, and the prefix and suffix LIKE patterns. */
static std::string ch_text_search_predicate(const std::string &col) {
  auto w = [] { return rand_word(); };
  switch (rand_int(13)) {
  case 0:
    return "hasToken(" + col + ", '" + w() + "')";
  /* SIMILAR TO (PR 101514): LIKE wildcards plus regexp alternation, classes
     and quantifiers. The text index condition does not serve it, so these
     scan every granule; they are here for correctness next to the LIKE
     shape they reduce to. */
  case 9:
    return col + " SIMILAR TO '%" + w() + "%'";
  case 10:
    return col + " SIMILAR TO '%(" + w() + "|" + w() + ")%'";
  case 11:
    return col + " SIMILAR TO '" + rand_word_part(true) + "[a-z0-9]*%'";
  case 12:
    return col + " NOT SIMILAR TO '" + w() + "%'";
  case 1:
    return "hasAnyTokens(" + col + ", ['" + w() + "', '" + w() + "'])";
  case 2:
    return "hasAllTokens(" + col + ", ['" + w() + "', '" + w() + "'])";
  case 3:
    return "hasPhrase(" + col + ", '" + w() + " " + w() + "')";
  case 4:
    return col + " LIKE '" + rand_word_part(true) + "%'";
  case 5:
    return col + " LIKE '%" + rand_word_part(false) + "'";
  case 6:
    return col + " LIKE '" + w() + "%'";
  case 7:
    return col + " LIKE '%" + w() + "'";
  case 8:
    return col + " LIKE '%" + w() + "%'";
  default:
    return "hasAnyTokens(" + col + ", '" + w() + " " + w() + "')";
  }
}
#endif

static PreparedStatementParam param_from_sql_literal(const std::string &value) {
  PreparedStatementParam param;
  if (value == "NULL") {
    param.is_null = true;
    return param;
  }

  if (value.size() >= 2 &&
      ((value.front() == '\'' && value.back() == '\'') ||
       (value.front() == '"' && value.back() == '"'))) {
    param.value = value.substr(1, value.size() - 2);
  } else {
    param.value = value;
  }
  return param;
}

static bool is_preparable_select_column(Column *col) {
  return col->is_col_string() || col->is_col_can_be_compared() ||
         col->type_ == Column::DECIMAL;
}

bool Table::BuildPreparedWherePrecise(
    std::string &prepared_where, std::string &printable_where,
    std::vector<PreparedStatementParam> &params) {
  auto col = GetRandomColumn();
  if (!is_preparable_select_column(col) || col->type_ == Column::GENERATED)
    return false;

  auto value = col->rand_value();
  if (value == "NULL")
    return false;

  prepared_where = " WHERE " + col->name_ + " = ?";
  printable_where = " WHERE " + col->name_ + " = " + value;
  params.push_back(param_from_sql_literal(value));
  return true;
}

bool Table::BuildPreparedWhereBulk(
    std::string &prepared_where, std::string &printable_where,
    std::vector<PreparedStatementParam> &params) {
  auto col = GetRandomColumn();
  if (!is_preparable_select_column(col) || col->type_ == Column::GENERATED)
    return false;

  auto value = col->rand_value();
  if (value == "NULL")
    return false;

  auto add_one_param = [&]() {
    params.push_back(param_from_sql_literal(value));
  };

  std::string where = " WHERE " + col->name_;
  if (col->is_col_can_be_compared() || col->type_ == Column::DECIMAL) {
    int shape = rand_int(3);
    if (shape == 0) {
      prepared_where = where + " = ?";
      printable_where = where + " = " + value;
      add_one_param();
      return true;
    }
    if (shape == 1) {
      prepared_where = where + " >= ?";
      printable_where = where + " >= " + value;
      add_one_param();
      return true;
    }
    if (shape == 2) {
      prepared_where = where + " <= ?";
      printable_where = where + " <= " + value;
      add_one_param();
      return true;
    }

    auto second_value = col->rand_value();
    if (second_value == "NULL")
      return false;
    prepared_where = where + " BETWEEN ? AND ?";
    printable_where = where + " BETWEEN " + value + " AND " + second_value;
    add_one_param();
    params.push_back(param_from_sql_literal(second_value));
    return true;
  }

  if (col->is_col_string()) {
    prepared_where = where + " = ?";
    printable_where = where + " = " + value;
    add_one_param();
    return true;
  }

  return false;
}

std::string Table::GetWherePrecise(bool text_search) {
  auto col = GetRandomColumn();
  std::string where = " WHERE ";
  std::string rand_value;

#ifdef USE_CLICKHOUSE
  /* most of the time a column with a text index gets searched the way the
     index is meant for, rather than compared for equality */
  if (text_search && HasTextIndex(col->name_) && rand_int(99) < 80)
    return where + ch_text_search_predicate(col->name_);
#endif

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
  PreparedSelect prepared_select;
  bool use_prepared = false;
  bool ps_requested = should_use_prepared_select();
  std::string ps_fallback_reason;
  std::string where = GetWherePrecise(true);
  std::string prepared_where;
  std::string printable_where;
  std::vector<PreparedStatementParam> params;
  if (ps_requested) {
    if (BuildPreparedWherePrecise(prepared_where, printable_where, params)) {
      use_prepared = true;
      where = printable_where;
    } else {
      ps_fallback_reason = "unsupported-select-shape";
    }
  }
  assert(where.size() > 4);
  auto select_column = SelectColumn();
  auto partition = GetRandomPartition();
  std::string sql =
      "SELECT " + select_column + " FROM " + name_ + partition + where;
  if (use_prepared) {
    prepared_select.prepared_sql = "SELECT " + select_column + " FROM " +
                                   name_ + partition + prepared_where;
    prepared_select.printable_sql = sql;
    prepared_select.params = std::move(params);
  }

  if (options->at(Option::COMPARE_RESULT)->getBool()) {
    sql += " order by " + select_column;
    if (use_prepared)
      ps_fallback_reason = "compare-result";
    use_prepared = false;
  }
  if (select_for_update &&
      options->at(Option::SECONDARY_ENGINE)->getString() == "")
    sql += " FOR UPDATE SKIP LOCKED";
  if (use_prepared && select_for_update &&
      options->at(Option::SECONDARY_ENGINE)->getString() == "") {
    prepared_select.prepared_sql += " FOR UPDATE SKIP LOCKED";
    prepared_select.printable_sql += " FOR UPDATE SKIP LOCKED";
  }

#ifdef USE_CLICKHOUSE
  /* Under --thread-per-table this thread is the only writer of the table, so
     the same SELECT run with and without the setting must return the same
     rows. ORDER BY ALL keeps the comparison order independent. */
  if (options->at(Option::COMPARE_RESULT_WITH_SETTING)->getBool() &&
      options->at(Option::THREAD_PER_TABLE)->getBool()) {
    sql += " ORDER BY ALL";
    unlock_table_mutex();
    compare_result_with_setting(sql, thd);
    return;
  }
#endif
  unlock_table_mutex();
  if (!use_prepared && ps_requested && !ps_fallback_reason.empty()) {
    thd->thread_log << "PS_FALLBACK " << ps_fallback_reason << " " << sql
                    << std::endl;
  }
  if (options->at(Option::COMPARE_RESULT)->getBool()) {
    Compare_between_engine(sql, thd);
  } else {
    if (options->at(Option::SELECT_IN_SECONDARY)->getBool()) {
      execute_sql("COMMIT", thd);
      thd->trx_left = 0;
    }
    if (use_prepared) {
      execute_prepared_select(prepared_select, thd);
    } else {
      execute_sql(sql, thd);
    }
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
          /* the function body bakes its literals in once and is then called
             many times, so an allocated key would be inserted repeatedly and
             leave a permanent hole in the sequence */
          sql.append("INSERT INTO " + name_ + ColumnValues(thd, 1, false) +
                     "; ");
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
  /* a REPLACE carrying a brand-new key replaces nothing, so sample an
     existing one instead of allocating */
  auto sql = "REPLACE INTO " + name_ + ColumnValues(thd, 1, false);
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
  if (execute_sql(sql, thd))
    mark_mv_source_mutated("UPDATE");
}

/* ClickHouse: DELETE FROM is rewritten server-side into an internal update of
   _row_exists, and lightweight_delete_mode picks how. At its alter_update
   default that is a heavyweight mutation -- a full rewrite of every matched
   part, executed on the background merge pool and only ever scheduled when
   number_of_free_entries_in_pool_to_execute_mutation slots are free, so a pool
   saturated by merges leaves the statement blocked for as long as the insert
   load lasts (DELETE FROM waits by default, lightweight_deletes_sync = 2).
   lightweight_update writes a patch part inline instead, like an INSERT: no
   pool slot, nothing in system.mutations, and the patch is applied on every
   SELECT until a merge folds it into the base part.

   Rolled per statement, not set once for the run, so both implementations get
   exercised by one invocation. Empty for every other fork -- the option is
   zeroed there, the same way CH_ALTER_UPDATE/DELETE are. */
static std::string lightweight_delete_settings() {
  const long int prob = options->at(Option::CH_LIGHTWEIGHT_DELETE)->getInt();
  if (prob > 0 && rand_int(100, 1) <= prob)
    return " SETTINGS lightweight_delete_mode = 'lightweight_update'";
  return "";
}

void Table::DeleteRandomRow(Thd1 *thd) {
  lock_table_mutex(thd->ddl_query);
  std::string sql = "DELETE " + add_ignore_clause() + " FROM " + name_ +
                    GetRandomPartition() + GetWherePrecise() +
                    lightweight_delete_settings();
  unlock_table_mutex();
  std::shared_lock lock(dml_mutex);
  if (execute_sql(sql, thd))
    mark_mv_source_mutated("DELETE");
}

void Table::UpdateAllRows(Thd1 *thd) {
  lock_table_mutex(thd->ddl_query);
  std::string sql = "UPDATE " + add_ignore_clause() + name_ +
                    GetRandomPartition() + " SET " + SetClause() +
                    GetWhereBulk();
  unlock_table_mutex();
  std::shared_lock lock(dml_mutex);
  if (execute_sql(sql, thd))
    mark_mv_source_mutated("a bulk UPDATE");
}

void Table::DeleteAllRows(Thd1 *thd) {
  lock_table_mutex(thd->ddl_query);
  std::string sql = "DELETE " + add_ignore_clause() + " FROM " + name_ +
                    GetRandomPartition() + GetWhereBulk() +
                    lightweight_delete_settings();
  unlock_table_mutex();
  std::shared_lock lock(dml_mutex);
  if (execute_sql(sql, thd))
    mark_mv_source_mutated("a bulk DELETE");
}

#ifdef USE_CLICKHOUSE
/* Return a WHERE clause targeting ~50-70% of the table's row range.
   Uses the integer PK column with a BETWEEN range covering half to
   two-thirds of the full value span.  Falls back to GetWhereBulk()
   when no integer PK exists. */
std::string Table::GetWhereLargeRange() {
  /* --seq-pk: take the bounds from the keys actually handed out. Without this
     the range below covers 50-70% of a value span two orders of magnitude
     wider than the data, so ALTER TABLE UPDATE/DELETE mutate almost nothing.
     Works for a string key too, because the zero-padded values compare
     correctly with BETWEEN -- the span-based path below cannot. */
  if (use_sequential_pk()) {
    const long int hi = pk_next.load(std::memory_order_relaxed) - 1;
    if (hi < 1)
      return GetWhereBulk();
    const long int span = hi;
    /* half to two thirds of the live range, placed uniformly inside it so
       repeated mutations do not keep hitting the same low keys */
    const long int width = span / 2 + rand_int(span / 5);
    const long int start = 1 + rand_int(span - width);
    return " WHERE " + seq_pk_col->name_ + " BETWEEN " +
           pk_literal(start, seq_pk_col) + " AND " +
           pk_literal(start + width, seq_pk_col);
  }

  Column *int_pk = nullptr;
  for (auto col : *columns_) {
    if (col->primary_key && col->is_col_number()) {
      int_pk = col;
      break;
    }
  }
  if (int_pk == nullptr)
    return GetWhereBulk();

  auto unique_range = options->at(Option::UNIQUE_RANGE)->getFloat();
  auto total_range =
      static_cast<long int>(unique_range * (long int)number_of_records);
  if (total_range < 2)
    return GetWhereBulk();

  /* width covers 50-70% of the full value span */
  auto width = rand_int((long int)(total_range * 0.2)) +
               (long int)(total_range * 0.5);
  /* start somewhere in the lower 30% so the range fits inside total_range */
  auto lo = try_negative(rand_int((long int)(total_range * 0.3)));
  auto hi = lo + width;
  return " WHERE " + int_pk->name_ + " BETWEEN " + std::to_string(lo) +
         " AND " + std::to_string(hi);
}

/* A killed mutation fails the ALTER that was waiting on it (--ch-mutations-sync)
   after some parts have already been rewritten, so the source is out of step
   with its materialized views whether the statement succeeded or not. Only
   widen the skip when kills are actually enabled — otherwise a rejected ALTER
   really did change nothing and its views stay checkable. */
static bool mutation_may_have_applied(bool executed) {
  return executed || options->at(Option::CH_KILL_MUTATION)->getInt() > 0;
}

/* ALTER TABLE t UPDATE col=val WHERE ... SETTINGS mutations_sync=2
   mutations_sync=2 waits for the mutation to complete on all replicas. */
void Table::AlterTableUpdate(Thd1 *thd) {
  lock_table_mutex(thd->ddl_query);
  std::string sql = "ALTER TABLE " + name_ + " UPDATE " + SetClause() +
                    GetWhereLargeRange();
  if (options->at(Option::CH_MUTATIONS_SYNC)->getBool())
    sql += " SETTINGS mutations_sync = 2";
  unlock_table_mutex();
  if (mutation_may_have_applied(execute_sql(sql, thd)))
    mark_mv_source_mutated("ALTER TABLE UPDATE");
}

/* ALTER TABLE t DELETE WHERE ... SETTINGS mutations_sync=2 */
void Table::AlterTableDelete(Thd1 *thd) {
  lock_table_mutex(thd->ddl_query);
  std::string sql =
      "ALTER TABLE " + name_ + " DELETE" + GetWhereLargeRange();
  if (options->at(Option::CH_MUTATIONS_SYNC)->getBool())
    sql += " SETTINGS mutations_sync = 2";
  unlock_table_mutex();
  if (mutation_may_have_applied(execute_sql(sql, thd)))
    mark_mv_source_mutated("ALTER TABLE DELETE");
}
#endif

void Table::SelectAllRow(Thd1 *thd, bool select_for_update) {
  lock_table_mutex(thd->ddl_query);
  PreparedSelect prepared_select;
  bool use_prepared = false;
  bool ps_requested = should_use_prepared_select();
  std::string ps_fallback_reason;
  std::string where = GetWhereBulk();
  std::string prepared_where;
  std::string printable_where;
  std::vector<PreparedStatementParam> params;
  if (ps_requested) {
    if (BuildPreparedWhereBulk(prepared_where, printable_where, params)) {
      use_prepared = true;
      where = printable_where;
    } else {
      ps_fallback_reason = "unsupported-select-shape";
    }
  }
  auto select_column = SelectColumn();
  auto partition = GetRandomPartition();
  std::string sql =
      "SELECT " + select_column + " FROM " + name_ + partition + where;
  if (use_prepared) {
    prepared_select.prepared_sql = "SELECT " + select_column + " FROM " +
                                   name_ + partition + prepared_where;
    prepared_select.printable_sql = sql;
    prepared_select.params = std::move(params);
  }
  if (select_for_update &&
      options->at(Option::SECONDARY_ENGINE)->getString() == "")
    sql += " FOR UPDATE SKIP LOCKED";
  if (use_prepared && select_for_update &&
      options->at(Option::SECONDARY_ENGINE)->getString() == "") {
    prepared_select.prepared_sql += " FOR UPDATE SKIP LOCKED";
    prepared_select.printable_sql += " FOR UPDATE SKIP LOCKED";
  }
  unlock_table_mutex();
  if (!use_prepared && ps_requested && !ps_fallback_reason.empty()) {
    thd->thread_log << "PS_FALLBACK " << ps_fallback_reason << " " << sql
                    << std::endl;
  }
  if (options->at(Option::SELECT_IN_SECONDARY)->getBool()) {
    execute_sql("COMMIT", thd);
    thd->trx_left = 0;
  }
  if (use_prepared) {
    execute_prepared_select(prepared_select, thd);
  } else {
    execute_sql(sql, thd);
  }
}

std::string Table::SetClause() {
  Column *col = nullptr;

  if ((columns_->at(0)->primary_key &&
       rand_int(100, 1) <= options->at(Option::PKEY_IN_SET)->getInt()) ||
      columns_->size() == 1) {
    col = columns_->at(0);
  } else {
    /* A composite key column is a key column, and so is the partition column
       of a CH_KEY table, and ClickHouse rejects an UPDATE on any of them
       ("Cannot UPDATE key column"). --pk-in-set above is the
       switch for deliberately trying a key column, so gate these the same way
       rather than letting the ordinary path pick one by accident. Collected
       into a list instead of rejection-sampled: same distribution, and it
       cannot spin forever on a table whose every column is in the key. */
    std::vector<Column *> settable;
    for (auto c : *columns_)
      if (c->type_ != Column::GENERATED && !c->primary_key &&
          !c->composite_key && !c->is_partition)
        settable.push_back(c);
    col = settable.empty() ? columns_->at(0)
                           : settable[rand_int(settable.size() - 1)];
  }
  std::string set_clause = col->name_ + " = ";
  set_clause +=
      (col->type_ == Column::JSON ? json_set(col) : col->rand_value());

  /* 10% update most of column */
  if (rand_int(100) < 10) {
    for (const auto &column : *columns_) {
      /* composite_key as well as primary_key: this tail widens the SET list
         past the column picked above, and a key column here fails the whole
         statement on ClickHouse just as surely as one picked there. */
      if (column->primary_key == false && column->composite_key == false &&
          column->is_partition == false &&
          column->type_ != Column::GENERATED &&
          column->name_ != col->name_ && rand_int(100) > 50) {
        set_clause += "," + column->name_ + " = " +
                      (column->type_ == Column::JSON ? json_set(column)
                                                     : column->rand_value());
      }
    }
  }

  return set_clause + " ";
}
