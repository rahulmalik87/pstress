/* ClickHouse specific DDL: engine resolution, materialized views,
   projections, text and integer skipping indexes, and table settings
   changed under load. */

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

#ifdef USE_CLICKHOUSE
/* Uppercase copy, for the case insensitive engine name matching below. */
std::string to_upper(const std::string &s) {
  std::string u = s;
  std::transform(u.begin(), u.end(), u.begin(), ::toupper);
  return u;
}

/* Base name of a MergeTree family engine with any Replicated prefix removed. */
std::string ch_engine_base(const std::string &engine) {
  std::string name = engine;
  /* an empty argument list carries no information, drop it */
  if (name.size() >= 2 && name.compare(name.size() - 2, 2, "()") == 0)
    name.erase(name.size() - 2);
  if (to_upper(name).rfind("REPLICATED", 0) == 0)
    name.erase(0, strlen("Replicated"));
  return name;
}

/* Turn --engine into the engine clause of a CREATE TABLE.

   Recognises the MergeTree family members pstress knows how to drive, case
   insensitively and with an optional empty "()"; ReplacingMergeTree is given
   _pstress_ver as its version column so duplicate primary keys resolve
   deterministically, which is what pstress relied on before --engine was
   honoured. A Replicated prefix is added when --port names more than one port.

   An engine spelled with its own arguments, or one not in the list, is passed
   through exactly as written, so an engine pstress has not been taught about
   can still be tried without a code change. */
bool ch_version_column() {
  return !options->at(Option::NO_VERSION_COLUMN)->getBool();
}

std::string ch_resolve_engine(const std::string &engine) {
  std::string name = engine;
  if (name.size() >= 2 && name.compare(name.size() - 2, 2, "()") == 0)
    name.erase(name.size() - 2);
  /* the user spelled out arguments, they know what they want */
  if (name.find('(') != std::string::npos)
    return engine;

  const bool already_replicated = to_upper(name).rfind("REPLICATED", 0) == 0;
  const std::string base = ch_engine_base(name);
  const std::string base_upper = to_upper(base);

  if (base_upper != "MERGETREE" && base_upper != "REPLACINGMERGETREE" &&
      base_upper != "SUMMINGMERGETREE" && base_upper != "AGGREGATINGMERGETREE")
    return engine;

  const bool replicated =
      already_replicated ||
      options->at(Option::PORT)->getString().find(',') != std::string::npos;

  std::string resolved = replicated ? "Replicated" : "";
  resolved += base;
  if (base_upper == "REPLACINGMERGETREE" && ch_version_column())
    resolved += "(_pstress_ver)";
  return resolved;
}

/* The type ClickHouse actually stores behind a pstress column clause.
   VARCHAR(n), CHAR(n), TEXT and BLOB are all aliases for String whatever the
   length, so the length pstress randomizes in MODIFY COLUMN is a no-op there and
   must not count as retyping the column. DECIMAL(p,s) genuinely changes. */
std::string ch_stored_type(const std::string &clause) {
  const std::string upper = to_upper(clause);
  if (upper.rfind("VARCHAR", 0) == 0 || upper.rfind("CHAR", 0) == 0 ||
      upper.rfind("TEXT", 0) == 0 || upper.rfind("BLOB", 0) == 0)
    return "String";
  return clause;
}

/* True when the engine merges rows sharing a sorting key into one, which makes
   an exact row for row comparison against a materialized view meaningless: the
   duplicate rows a non atomic POPULATE would leave behind are exactly what
   these engines collapse away. */
bool ch_engine_collapses_rows(const std::string &engine) {
  const std::string base = to_upper(ch_engine_base(engine));
  return base != "MERGETREE";
}

bool g_projections_enabled = false;
bool g_text_index_phrase_search_enabled = false;


/* Columns a projection may put in its SELECT list.

   JSON is out because pstress reads it back through randomized accessor
   expressions rather than as a value, and BLOB/TEXT are out because wide values
   make index_granularity_bytes rather than index_granularity the binding
   constraint on granule size, which is exactly the signal the laziness oracle
   reads out of system.projection_parts. GENERATED columns are already disabled
   on the ClickHouse build; excluded here anyway so a future change cannot
   silently start emitting them. */
static bool ch_projection_usable(const Column *c) {
  switch (c->type_) {
  case Column::JSON:
  case Column::BLOB:
  case Column::TEXT:
  case Column::GENERATED:
    return false;
  default:
    return true;
  }
}

/* Columns a projection may sort by. Same set: everything usable is orderable
   once allow_nullable_key is on, which Table::definition() always emits when
   projections are enabled. */
static bool ch_projection_keyable(const Column *c) {
  return ch_projection_usable(c);
}

/* Build a random projection over this table, or nullptr when the table has
   nothing to build one from. Caller must hold table_mutex.

   Never SELECT *: enable_block_number_column and enable_block_offset_column are
   always on, so a star would drag _block_number and _block_offset into the
   projection and make its contents depend on which part a row landed in. An
   explicit list pins the projection's shape at creation, the same reason
   CreateMaterializedView captures its column list by hand. */
Projection *Table::MakeRandomProjection() {
  std::vector<Column *> usable, keyable;
  for (auto col : *columns_) {
    if (ch_projection_usable(col))
      usable.push_back(col);
    if (ch_projection_keyable(col))
      keyable.push_back(col);
  }
  if (usable.empty() || keyable.empty())
    return nullptr;

  /* A projection keyed on the enforcing primary key alone reorders nothing --
     that column is the table's own ORDER BY prefix -- so the optimizer will
     never prefer it. Reroll once; a table whose only keyable column is the pk
     gets no projection rather than a useless one. A composite key column is
     still worth keying on: it sorts the table only within a prefix, so a
     projection leading with it is a genuinely different order. */
  std::vector<Column *> keys;
  for (int attempt = 0; attempt < 2; attempt++) {
    keys.clear();
    const int want = rand_int(std::min<size_t>(3, keyable.size()), 1);
    std::vector<Column *> pool = keyable;
    for (int i = 0; i < want && !pool.empty(); i++) {
      const size_t pick = rand_int(pool.size() - 1);
      keys.push_back(pool[pick]);
      pool.erase(pool.begin() + pick);
    }
    if (!(keys.size() == 1 && keys[0]->primary_key))
      break;
  }
  if (keys.size() == 1 && keys[0]->primary_key)
    return nullptr;

  auto *proj = new Projection();
  /* A counter, so two threads can never pick the same name — a random suffix
     collides often enough to be visible at this rate. The run's seed goes in
     too: nothing about projections is persisted, so at --step >= 2 the counter
     restarts at zero while the projections an earlier step created are still
     on the server, and only the seed keeps the two apart. */
  static std::atomic<unsigned long> next_id{0};
  proj->name = "p_" + name_ + "_" +
               std::to_string(options->at(Option::INITIAL_SEED)->getInt()) +
               "_" + std::to_string(next_id++);

  /* The select list is the keys plus a few more columns, so the projection is
     worth reading from rather than just a differently sorted key. */
  std::vector<Column *> selected = keys;
  for (auto col : usable) {
    if (selected.size() >= keys.size() + 4)
      break;
    if (std::find(selected.begin(), selected.end(), col) != selected.end())
      continue;
    if (rand_int(100) < 50)
      selected.push_back(col);
  }

  std::string select_list;
  for (auto col : selected) {
    if (!select_list.empty())
      select_list += ", ";
    select_list += col->name_;
    proj->columns.push_back(col->name_);
  }

  /* assumeNotNull() some of the time: it keeps expression keyed projections
     covered, and it is the shape that still works on a server where
     allow_nullable_key is unavailable. */
  std::string order_list;
  for (auto col : keys) {
    const bool wrap = col->null_val && rand_int(100) < 20;
    const std::string key =
        wrap ? "assumeNotNull(" + col->name_ + ")" : col->name_;
    if (!order_list.empty())
      order_list += ", ";
    order_list += key;
    if (std::find(proj->columns.begin(), proj->columns.end(), col->name_) ==
        proj->columns.end())
      proj->columns.push_back(col->name_);
  }

  proj->body = "SELECT " + select_list + " ORDER BY " +
               (keys.size() == 1 ? order_list : "(" + order_list + ")");
  return proj;
}

#endif

#ifdef USE_CLICKHOUSE
/* deque, not vector: the strings must never be reallocated, because the char*
   handed to mark_mv_source_mutated() has to stay valid for the whole run. Only
   touched while loading metadata, which is single threaded. */
const char *intern_mv_reason(const std::string &why) {
  static std::deque<std::string> interned;
  interned.push_back(why);
  return interned.back().c_str();
}

static std::atomic<unsigned long> g_mv_id_counter{0};

unsigned long next_mv_id() { return g_mv_id_counter++; }

void bump_mv_id_floor(unsigned long id) {
  unsigned long current = g_mv_id_counter.load();
  while (current <= id &&
         !g_mv_id_counter.compare_exchange_weak(current, id + 1)) {
  }
}

size_t mv_count_for_table(const std::string &table_name) {
  std::lock_guard<std::mutex> lk(g_materialized_views_mutex);
  size_t count = 0;
  for (const auto &mv : g_materialized_views)
    if (mv.src_table == table_name)
      count++;
  return count;
}

/* Comma separated column list the views select. */
static std::string mv_column_list(const std::vector<std::string> &columns) {
  std::string list;
  for (const auto &col : columns) {
    if (!list.empty())
      list += ", ";
    list += col;
  }
  return list;
}

/* CREATE MATERIALIZED VIEW ... POPULATE over this table, while other threads
   keep inserting into it. That race is the point: POPULATE has to subscribe the
   view to new inserts and snapshot the existing rows as one cut, so every row
   inserted concurrently ends up in the view exactly once. Once inserts stop the
   view must therefore hold exactly the table's rows, which is what
   ch_verify_one_mv() checks. */
void Table::CreateMaterializedView(Thd1 *thd) {
  if (mv_count_for_table(name_) >=
      (size_t)options->at(Option::CH_MAX_MV_PER_TABLE)->getInt())
    return;

  /* Build the statement under the table lock so the column list cannot change
     while we read it, then release the lock before executing. Holding it across
     the CREATE would lock out pstress's own inserts into this table and there
     would be no race left to test. */
  lock_table_mutex(thd->ddl_query);
  std::vector<std::string> columns;
  for (auto col : *columns_)
    columns.push_back(col->name_);
  /* definition() appends the version column by hand, so it is a real column of
     the table but never appears in columns_. DropColumn refuses to drop it, so
     it is safe for the view to select it. */
  if (ch_version_column())
    columns.push_back("_pstress_ver");
  unlock_table_mutex();

  /* An explicit column list, never SELECT *, pins the view's shape at creation:
     a later ADD COLUMN then cannot change what the view is expected to hold. */
  const std::string col_list = mv_column_list(columns);
  const std::string id = std::to_string(next_mv_id());
  const std::string mv_name = MV_PREFIX + name_ + "_" + id;

  bool atomically = true;
  const std::string mode =
      options->at(Option::CH_MV_POPULATE_ATOMICALLY)->getString();
  if (mode == "off")
    atomically = false;
  else if (mode == "random")
    atomically = rand_int(1) == 1;

  std::string settings = "materialized_views_populate_atomically = ";
  settings += atomically ? "1" : "0";
  auto sleep_ms = options->at(Option::CH_MV_SNAPSHOT_SLEEP_MS)->getInt();
  if (sleep_ms > 0)
    settings += ", merge_tree_storage_snapshot_sleep_ms = " +
                std::to_string(sleep_ms);

  /* Shared dml_mutex so a DROP COLUMN, which takes it exclusively, cannot land
     between building the column list and running the statement. */
  std::shared_lock<std::shared_mutex> lock(dml_mutex);

  std::string target;
  if (rand_int(100) < options->at(Option::CH_MV_TO_PROB)->getInt()) {
    /* POPULATE together with TO was a syntax error before the atomic populate
       work. LIMIT 0 gives the target the exact structure of the selected
       columns and no rows, so everything it ends up holding arrived through the
       view and the comparison against the source still holds. */
    target = MV_TARGET_PREFIX + name_ + "_" + id;
    if (!execute_sql("CREATE TABLE " + target +
                         " ENGINE = MergeTree ORDER BY tuple() AS SELECT " +
                         col_list + " FROM " + name_ + " LIMIT 0",
                     thd))
      return;
  }

  std::string sql = "CREATE MATERIALIZED VIEW " + mv_name + " ";
  if (target.empty()) {
    /* ORDER BY tuple() keeps the view side from ever merging rows together, so
       a duplicate a non atomic populate leaves behind stays visible, and it
       avoids putting a Nullable column in a sorting key. */
    sql += "ENGINE = MergeTree ORDER BY tuple() ";
  } else {
    sql += "TO " + target + " ";
  }
  sql += "POPULATE AS SELECT " + col_list + " FROM " + name_ + " SETTINGS " +
         settings;

  if (!execute_sql(sql, thd)) {
    /* The server rolls the view back on a failed population; the target we
       created for it is ours to clean up. */
    if (!target.empty())
      execute_sql("DROP TABLE IF EXISTS " + target, thd);
    return;
  }

  MVInfo mv;
  mv.name = mv_name;
  mv.target = target;
  mv.src_table = name_;
  mv.columns = std::move(columns);
  mv.populate_atomically = atomically;
  std::lock_guard<std::mutex> lk(g_materialized_views_mutex);
  g_materialized_views.push_back(std::move(mv));
}

/* Drop one of this table's materialized views, checking it against the table
   first unless --verify-mv-before-drop is off. Checking here rather than only at
   the end of the run means every view that ever existed gets compared, not just
   the ones still alive when the run finishes.

   The caller must already have quiesced the workload when the check is enabled:
   count(view) against count(table) only holds when nothing is inserting. */
void Table::DropMaterializedView(Thd1 *thd) {
  MVInfo mv;
  {
    std::lock_guard<std::mutex> lk(g_materialized_views_mutex);
    std::vector<size_t> mine;
    for (size_t i = 0; i < g_materialized_views.size(); i++)
      if (g_materialized_views[i].src_table == name_)
        mine.push_back(i);
    if (mine.empty())
      return;
    const size_t pick = mine[rand_int(mine.size() - 1)];
    mv = g_materialized_views[pick];
    g_materialized_views.erase(g_materialized_views.begin() + pick);
  }

  if (options->at(Option::CH_VERIFY_MV_BEFORE_DROP)->getBool()) {
    auto query_one = [thd](const std::string &sql) {
      return thd->db->get_single_value(sql);
    };
    ch_report_mv_check(mv, mv_skip_reason(), engine, query_one, thd);
  }

  execute_sql("DROP VIEW IF EXISTS " + mv.name, thd);
  if (!mv.target.empty())
    execute_sql("DROP TABLE IF EXISTS " + mv.target, thd);
}

/* ---------------------------------------------------------------------------
   Projections. ClickHouse PR 113343 added ALTER TABLE ... MODIFY PROJECTION,
   a metadata only alter that changes a projection's settings and leaves every
   existing part alone. Three actions put it under load: ADD to create one,
   MODIFY to change its settings, DROP to take it away again.

   MODIFY restates the whole definition and rejects anything whose query body
   formats differently, which is the only reason Projection::body exists: the
   text pstress emitted has to be replayed verbatim.
   ------------------------------------------------------------------------- */

/* the server caps projections per table at max_projections = 25 */
static const size_t CH_MAX_PROJECTIONS_PER_TABLE = 5;

/* Only index_granularity is rolled. It is on ClickHouse's allow list for
   projection settings in every version that has the feature, so there is no
   need to probe the server for what it accepts. */
static std::string ch_roll_projection_granularity() {
  static const char *const values[] = {"512", "1024", "4096", "8192", "16384"};
  return values[rand_int(4)];
}

/* ALTER TABLE t ADD PROJECTION p (SELECT ... ORDER BY ...) WITH SETTINGS (...) */
void Table::AddProjection(Thd1 *thd) {
  lock_table_mutex(thd->ddl_query);
  if (projections_->size() >= CH_MAX_PROJECTIONS_PER_TABLE) {
    unlock_table_mutex();
    return;
  }
  Projection *proj = MakeRandomProjection();
  if (proj == nullptr) {
    unlock_table_mutex();
    return;
  }
  const std::string sql = "ALTER TABLE " + name_ + " ADD PROJECTION " +
                          proj->name + " (" + proj->body +
                          ") WITH SETTINGS (index_granularity = " +
                          ch_roll_projection_granularity() + ")";
  unlock_table_mutex();

  /* Shared dml_mutex so a DROP COLUMN, which takes it exclusively, cannot
     remove one of the projection's columns between building the statement and
     running it. */
  std::shared_lock<std::shared_mutex> lock(dml_mutex);
  if (!execute_sql(sql, thd)) {
    delete proj;
    return;
  }

  lock_table_mutex(thd->ddl_query);
  projections_->push_back(proj);
  unlock_table_mutex();
}

/* Copy out one of this table's projections. Returns false when it has none.
   A copy rather than the pointer, because a concurrent DropProjection deletes
   it the moment the table lock is released. */
static bool ch_pick_projection(Table *table, Thd1 *thd, Projection *out) {
  table->lock_table_mutex(thd->ddl_query);
  if (table->projections_->empty()) {
    table->unlock_table_mutex();
    return false;
  }
  *out = *table->projections_->at(rand_int(table->projections_->size() - 1));
  table->unlock_table_mutex();
  return true;
}

void Table::DropProjection(Thd1 *thd) {
  Projection picked;
  if (!ch_pick_projection(this, thd, &picked))
    return;

  if (!execute_sql("ALTER TABLE " + name_ + " DROP PROJECTION IF EXISTS " +
                       picked.name,
                   thd))
    return;

  lock_table_mutex(thd->ddl_query);
  for (auto it = projections_->begin(); it != projections_->end(); it++)
    if ((*it)->name == picked.name) {
      delete *it;
      projections_->erase(it);
      break;
    }
  unlock_table_mutex();
}

/* ALTER TABLE t MODIFY PROJECTION p (<the stored body>) WITH SETTINGS (...)
   — ClickHouse PR 113343.

   Deliberately no mutations_sync: this is a metadata alter, never a mutation,
   so there is nothing to wait for. */
void Table::ModifyProjection(Thd1 *thd) {
  Projection picked;
  if (!ch_pick_projection(this, thd, &picked))
    return;

  std::string sql = "ALTER TABLE " + name_ + " MODIFY PROJECTION ";
  /* IF EXISTS some of the time: racing --drop-projection is how the "already
     gone" path gets exercised without being an error. */
  if (rand_int(100) < 30)
    sql += "IF EXISTS ";
  sql += picked.name + " (" + picked.body +
         ") WITH SETTINGS (index_granularity = " +
         ch_roll_projection_granularity() + ")";

  std::shared_lock<std::shared_mutex> lock(dml_mutex);
  execute_sql(sql, thd);
}

/* ALTER TABLE t MATERIALIZE PROJECTION p — rebuild the projection on the parts
   that do not carry it yet, which after an ADD is all of them.

   A mutation, unlike MODIFY PROJECTION, so it takes mutations_sync. IF EXISTS
   because --drop-projection may have removed it between the pick and here. */
void Table::MaterializeProjection(Thd1 *thd) {
  Projection picked;
  if (!ch_pick_projection(this, thd, &picked))
    return;

  std::string sql = "ALTER TABLE " + name_ +
                    " MATERIALIZE PROJECTION IF EXISTS " + picked.name;
  if (options->at(Option::CH_MUTATIONS_SYNC)->getBool())
    sql += " SETTINGS mutations_sync = 2";
  execute_sql(sql, thd);
}

/* ALTER TABLE t MODIFY SETTING <name> = <value>, with both taken from an alter:
   line of the settings pool.

   Emitted on its own and never folded into another ALTER: MODIFY SETTING is a
   non replicated alter, and a Replicated database - which is what pstress
   creates as soon as --port names more than one server - rejects a statement
   mixing replicated and non replicated alters with QUERY_IS_PROHIBITED.

   No mutations_sync and no dml_mutex: this changes metadata only and names no
   column, so there is nothing to wait for and nothing a concurrent DROP COLUMN
   could invalidate. Not a data mutation either, so the materialized views over
   this table stay comparable. */
void Table::ModifyTableSetting(Thd1 *thd) {
  std::string setting, value;
  if (!ch_roll_alterable_setting(setting, value))
    return;

  if (!execute_sql("ALTER TABLE " + name_ + " MODIFY SETTING " + setting +
                       " = " + value,
                   thd))
    return;

  /* the table now differs from the clause it was created with, so record it:
     DropCreate and the next step both rebuild from Table::settings */
  lock_table_mutex(thd->ddl_query);
  ch_settings_upsert(settings, setting, value);
  unlock_table_mutex();
}

/* ALTER TABLE t RESET SETTING <name> — put a setting back to the server
   default. Only a setting the table actually carries is reset, and only one the
   pool marked alterable: resetting a setting pstress owns, or one the table
   never had, would either break the mutation paths or do nothing. */
void Table::ResetTableSetting(Thd1 *thd) {
  std::string setting;
  {
    std::vector<std::string> candidates;
    lock_table_mutex(thd->ddl_query);
    for (const auto &name : ch_settings_names(settings))
      for (const auto &spec : g_alterable_table_settings)
        if (spec.name == name) {
          candidates.push_back(name);
          break;
        }
    unlock_table_mutex();
    if (candidates.empty())
      return;
    setting = candidates.at(rand_int(candidates.size() - 1));
  }

  if (!execute_sql("ALTER TABLE " + name_ + " RESET SETTING " + setting, thd))
    return;

  lock_table_mutex(thd->ddl_query);
  ch_settings_erase(settings, setting);
  unlock_table_mutex();
}

/* Drop every projection that names this column, before an ALTER that would
   otherwise be rejected.

   ClickHouse refuses to rename a column a projection references, and refuses to
   drop one the projection could not be rebuilt without. Without this pre-pass
   --drop-column and --rename-column would become silent permanent no-ops on
   every projected table. Caller must hold table_mutex; returns false when a
   drop failed, so the caller can leave the column alone rather than leave the
   registry disagreeing with the server. */
bool Table::DropProjectionsOnColumn(Thd1 *thd, const std::string &column) {
  std::vector<std::string> doomed;
  for (const auto *proj : *projections_)
    if (std::find(proj->columns.begin(), proj->columns.end(), column) !=
        proj->columns.end())
      doomed.push_back(proj->name);

  for (const auto &name : doomed) {
    if (!execute_sql("ALTER TABLE " + name_ + " DROP PROJECTION IF EXISTS " +
                         name,
                     thd))
      return false;
    for (auto it = projections_->begin(); it != projections_->end(); it++)
      if ((*it)->name == name) {
        delete *it;
        projections_->erase(it);
        break;
      }
  }
  return true;
}

/* Forget every projection on this table. Called when the table itself is
   dropped and recreated, which takes its projections with it. */
void Table::ForgetProjections() {
  for (auto *proj : *projections_)
    delete proj;
  projections_->clear();
}

/* ---------------------------------------------------------------------------
   Data skipping indexes. Text indexes go on String columns, which is what
   VARCHAR, CHAR, TEXT and BLOB all become, one per column. minmax, set and
   bloom_filter indexes go on int columns, one per column, never on the primary
   key, which the ORDER BY already covers.
   ------------------------------------------------------------------------- */

static bool ch_text_indexable(const Column *c) {
  switch (c->type_) {
  case Column::CHAR:
  case Column::VARCHAR:
  case Column::TEXT:
  case Column::BLOB:
    return true;
  default:
    return false;
  }
}

static std::string ch_roll_text_tokenizer() {
  const int r = rand_int(99);
  if (r < 35)
    return "splitByNonAlpha";
  if (r < 45)
    return "splitByString";
  if (r < 55)
    return "splitByString([' ', ',', '-', '.', '/', '_'])";
  if (r < 67)
    return "ngrams(" + std::to_string(rand_int(4, 2)) + ")";
  if (r < 75)
    return "sparseGrams(3, " + std::to_string(rand_int(6, 4)) + ")";
  if (r < 85)
    return "asciiCJK";
  if (r < 93)
    return "unicodeWord";
  return "splitByRegexp('[^a-zA-Z0-9]+')";
}

/* Index names are <prefix>_<table>_<seed>_<n>. Nothing is persisted, so a
   later step would restart n at 0 and collide with the indexes the server
   still has; LoadSkipIndexesFromServer() pushes the counters past the largest
   n it reads back. */
static std::atomic<unsigned long> g_next_text_index_id{0};
static std::atomic<unsigned long> g_next_int_index_id{0};

static void ch_bump_index_counter(std::atomic<unsigned long> &counter,
                                  const std::string &name) {
  auto pos = name.rfind('_');
  if (pos == std::string::npos)
    return;
  try {
    unsigned long n = std::stoul(name.substr(pos + 1)) + 1;
    unsigned long cur = counter.load();
    while (n > cur && !counter.compare_exchange_weak(cur, n))
      ;
  } catch (const std::exception &) {
    /* not one of ours */
  }
}

SkipIndex Table::MakeRandomTextIndex(const std::string &column) {
  static const int preprocessor_prob = opt_int(CH_TEXT_INDEX_PREPROCESSOR_PROB);
  static const int phrase_prob = opt_int(CH_TEXT_INDEX_PHRASE_SEARCH_PROB);

  SkipIndex ti;
  ti.name = "ti_" + name_ + "_" +
            std::to_string(options->at(Option::INITIAL_SEED)->getInt()) + "_" +
            std::to_string(g_next_text_index_id++);
  ti.column = column;
  ti.kind = "text";

  std::string args = "tokenizer = " + ch_roll_text_tokenizer();
  if (rand_int(99) < preprocessor_prob)
    args += ", preprocessor = " +
            std::string(rand_int(1) ? "lower(" : "lowerUTF8(") + column + ")";
  if (rand_int(99) < phrase_prob)
    args += ", support_phrase_search = 1";
  if (rand_int(4) == 0) {
    static const char *const v[] = {"16", "128", "512", "4096"};
    args += ", dictionary_block_size = " + std::string(v[rand_int(3)]);
  }
  if (rand_int(4) == 0)
    args += ", dictionary_block_frontcoding_compression = " +
            std::to_string(rand_int(1));
  if (rand_int(4) == 0) {
    static const char *const v[] = {"128", "1024", "65536", "1048576"};
    args += ", posting_list_block_size = " + std::string(v[rand_int(3)]);
  }
  if (rand_int(4) == 0) {
    static const char *const v[] = {"'none'", "'bitpacking'", "'pfor'"};
    args += ", posting_list_codec = " + std::string(v[rand_int(2)]);
  }
  /* no GRANULARITY: the server stores 100000000 for a text index whatever
     the statement says */
  ti.type = "text(" + args + ")";
  return ti;
}

static bool ch_int_indexable(const Column *c) {
  /* not the partition column either: the part-level min-max index already
     covers it, and a skip index on top would hide whether that one works */
  return (c->type_ == Column::INT || c->type_ == Column::INTEGER) &&
         !c->primary_key && !c->is_partition;
}

/* minmax 40%, set 30%, bloom_filter 30%. Half the indexes take the default
   GRANULARITY, the rest one of 1, 2, 4, 8: minmax and set over a wider stripe
   is where a stale or wrongly merged index mark would show. */
SkipIndex Table::MakeRandomIntIndex(const std::string &column) {
  SkipIndex ii;
  ii.name = "ii_" + name_ + "_" +
            std::to_string(options->at(Option::INITIAL_SEED)->getInt()) + "_" +
            std::to_string(g_next_int_index_id++);
  ii.column = column;
  const int r = rand_int(99);
  if (r < 40) {
    ii.kind = "minmax";
    ii.type = "minmax";
  } else if (r < 70) {
    /* 0 is unlimited; a small N makes the index give up on most granules,
       which the server has to notice and read them anyway */
    static const char *const n[] = {"0", "10", "100", "1000"};
    ii.kind = "set";
    ii.type = "set(" + std::string(n[rand_int(3)]) + ")";
  } else {
    static const char *const fp[] = {"0.001", "0.01", "0.025", "0.1"};
    ii.kind = "bloom_filter";
    ii.type = rand_int(1) ? "bloom_filter"
                          : "bloom_filter(" + std::string(fp[rand_int(3)]) + ")";
  }
  if (rand_int(1)) {
    static const char *const g[] = {"1", "2", "4", "8"};
    ii.granularity = g[rand_int(3)];
  }
  return ii;
}

void Table::RollSkipIndexes() {
  static const int text_prob = opt_int(CH_TEXT_INDEX_PROB);
  static const int int_prob = opt_int(CH_INT_INDEX_PROB);
  if (text_prob > 0)
    for (auto col : *columns_)
      if (ch_text_indexable(col) && rand_int(99) < text_prob)
        skip_indexes_.push_back(MakeRandomTextIndex(col->name_));
  if (int_prob > 0 && rand_int(99) < int_prob)
    if (auto *col = IntIndexCandidate())
      skip_indexes_.push_back(MakeRandomIntIndex(col->name_));
}

bool Table::HasTextIndex(const std::string &column) const {
  for (const auto &ti : skip_indexes_)
    if (ti.is_text() && ti.column == column)
      return true;
  return false;
}

bool Table::HasIntIndex(const std::string &column) const {
  for (const auto &ii : skip_indexes_)
    if (!ii.is_text() && ii.column == column)
      return true;
  return false;
}

Column *Table::IntIndexCandidate() const {
  for (auto col : *columns_)
    if (ch_int_indexable(col) && !HasIntIndex(col->name_))
      return col;
  return nullptr;
}

void Table::RenameSkipIndexColumn(const std::string &from,
                                  const std::string &to) {
  for (auto &ti : skip_indexes_)
    if (ti.column == from)
      ti.column = to;
}

bool Table::DropSkipIndexesOnColumn(Thd1 *thd, const std::string &column) {
  for (auto it = skip_indexes_.begin(); it != skip_indexes_.end();) {
    if (it->column != column) {
      it++;
      continue;
    }
    if (!execute_sql("ALTER TABLE " + name_ + " DROP INDEX IF EXISTS " +
                         it->name,
                     thd))
      return false;
    it = skip_indexes_.erase(it);
  }
  return true;
}

/* The server is the source of truth: CREATE TABLE may have failed, an earlier
   step may have added or dropped indexes, and none of it is in the metadata. */
bool Table::LoadSkipIndexesFromServer(Thd1 *thd) {
  auto rows = thd->db->get_query_result(
      "SELECT name, expr, type, type_full, granularity FROM "
      "system.data_skipping_indices WHERE database = '" +
      options->at(Option::DATABASE)->getString() + "' AND table = '" + name_ +
      "' AND type IN ('text', 'minmax', 'set', 'bloom_filter') ORDER BY name");
  std::vector<SkipIndex> found;
  for (const auto &row : rows) {
    if (row.size() < 5)
      return false;
    SkipIndex ti;
    ti.name = row[0];
    ti.column = row[1];
    ti.kind = row[2];
    ti.type = row[3];
    ti.granularity = row[4];
    ch_bump_index_counter(ti.is_text() ? g_next_text_index_id
                                       : g_next_int_index_id,
                          ti.name);
    found.push_back(ti);
  }
  lock_table_mutex(true);
  skip_indexes_ = std::move(found);
  unlock_table_mutex();
  return true;
}

void Table::AddTextIndex(Thd1 *thd) {
  lock_table_mutex(thd->ddl_query);
  std::vector<std::string> candidates;
  for (auto col : *columns_)
    if (ch_text_indexable(col) && !HasTextIndex(col->name_))
      candidates.push_back(col->name_);
  if (candidates.empty()) {
    unlock_table_mutex();
    return;
  }
  SkipIndex ti =
      MakeRandomTextIndex(candidates.at(rand_int(candidates.size() - 1)));
  unlock_table_mutex();

  /* shared dml_mutex, so a DROP COLUMN cannot take the column away between
     building the statement and running it */
  std::shared_lock<std::shared_mutex> lock(dml_mutex);
  if (!execute_sql("ALTER TABLE " + name_ + " ADD " + ti.definition(), thd))
    return;

  lock_table_mutex(thd->ddl_query);
  skip_indexes_.push_back(ti);
  unlock_table_mutex();
}

/* Copy out one of this table's text indexes (text = true) or int indexes.
   A copy rather than a reference, because a concurrent drop erases it once
   the lock is released. */
static bool ch_pick_skip_index(Table *table, Thd1 *thd, bool text,
                               SkipIndex *out) {
  table->lock_table_mutex(thd->ddl_query);
  std::vector<const SkipIndex *> family;
  for (const auto &si : table->skip_indexes_)
    if (si.is_text() == text)
      family.push_back(&si);
  if (family.empty()) {
    table->unlock_table_mutex();
    return false;
  }
  *out = *family.at(rand_int(family.size() - 1));
  table->unlock_table_mutex();
  return true;
}

void Table::AddIntIndex(Thd1 *thd) {
  lock_table_mutex(thd->ddl_query);
  auto *col = IntIndexCandidate();
  if (col == nullptr) {
    unlock_table_mutex();
    return;
  }
  SkipIndex ii = MakeRandomIntIndex(col->name_);
  unlock_table_mutex();

  std::shared_lock<std::shared_mutex> lock(dml_mutex);
  if (!execute_sql("ALTER TABLE " + name_ + " ADD " + ii.definition(), thd))
    return;

  lock_table_mutex(thd->ddl_query);
  skip_indexes_.push_back(ii);
  unlock_table_mutex();
}

void Table::DropTextIndex(Thd1 *thd) { DropSkipIndex(thd, true); }
void Table::DropIntIndex(Thd1 *thd) { DropSkipIndex(thd, false); }
void Table::MaterializeTextIndex(Thd1 *thd) { MaterializeSkipIndex(thd, true); }
void Table::MaterializeIntIndex(Thd1 *thd) { MaterializeSkipIndex(thd, false); }

void Table::DropSkipIndex(Thd1 *thd, bool text) {
  SkipIndex picked;
  if (!ch_pick_skip_index(this, thd, text, &picked))
    return;

  if (!execute_sql("ALTER TABLE " + name_ + " DROP INDEX IF EXISTS " +
                       picked.name,
                   thd))
    return;

  lock_table_mutex(thd->ddl_query);
  for (auto it = skip_indexes_.begin(); it != skip_indexes_.end(); it++)
    if (it->name == picked.name) {
      skip_indexes_.erase(it);
      break;
    }
  unlock_table_mutex();
}

/* MATERIALIZE INDEX builds the index on parts that lack it, CLEAR INDEX
   removes it from every part while keeping it in the table definition. Either
   way the queries keep running against a mix of indexed and unindexed parts,
   which must not change a single result. */
void Table::MaterializeSkipIndex(Thd1 *thd, bool text) {
  SkipIndex picked;
  if (!ch_pick_skip_index(this, thd, text, &picked))
    return;

  std::string sql = "ALTER TABLE " + name_ +
                    (rand_int(3) == 0 ? " CLEAR INDEX IF EXISTS "
                                      : " MATERIALIZE INDEX IF EXISTS ") +
                    picked.name;
  if (options->at(Option::CH_MUTATIONS_SYNC)->getBool())
    sql += " SETTINGS mutations_sync = 2";
  execute_sql(sql, thd);
}

#else
void Table::AlterTableUpdate(Thd1 *) {}
void Table::AlterTableDelete(Thd1 *) {}
#endif

#ifdef USE_CLICKHOUSE
/* Run once per run, before any table is created.

   Drops materialized views and TO targets left behind by an earlier run. pstress
   drops and recreates its own tables at step 1 but knows nothing about views, and
   a view left pointing at a recreated table re-binds to it by name and then fails
   every insert whose columns do not match the ones the view was created with.

   Also refuses to start when this connection would insert asynchronously without
   waiting for the flush: the INSERT would return before the views have the rows,
   so every comparison would report a difference that is not a bug. Asked of the
   worker's own connection rather than a fresh one, because the session settings
   pstress applies are what actually decide this. */
void ch_prepare_materialized_views(Thd1 *thd) {
  if (options->at(Option::CH_CREATE_MV)->getInt() > 0) {
    auto truthy = [](const std::string &v) {
      return !v.empty() && v != "0" && v != "false";
    };
    std::string async_insert = "0", wait_for_async = "1";
    for (const auto &row : thd->db->get_query_result(
             "SELECT name, value FROM system.settings WHERE name IN "
             "('async_insert', 'wait_for_async_insert')")) {
      if (row.size() < 2)
        continue;
      if (row[0] == "async_insert")
        async_insert = row[1];
      else
        wait_for_async = row[1];
    }
    if (truthy(async_insert) && !truthy(wait_for_async)) {
      print_and_log("--create-mv needs an INSERT to have reached the views by "
                    "the time it returns, but this connection has "
                    "async_insert=" + async_insert + " with "
                    "wait_for_async_insert=" + wait_for_async +
                    ", so it returns before the data is flushed. Set "
                    "wait_for_async_insert=1 or async_insert=0.",
                    thd);
      exit(EXIT_FAILURE);
    }
  }

  /* only when the tables themselves are being recreated, matching Table::load */
  if (options->at(Option::STEP)->getInt() != 1 &&
      !options->at(Option::PREPARE)->getBool())
    return;

  const std::string db = options->at(Option::DATABASE)->getString();
  /* views before targets, so dropping a target has no dependant left */
  auto leftovers = thd->db->get_query_result(
      "SELECT name, engine FROM system.tables WHERE database = '" + db +
      "' AND (startsWith(name, '" + MV_PREFIX + "') OR startsWith(name, '" +
      MV_TARGET_PREFIX + "')) ORDER BY engine = 'MaterializedView' DESC");
  for (const auto &row : leftovers) {
    if (row.size() < 2)
      continue;
    const bool is_view = row[1] == "MaterializedView";
    print_and_log("Dropping " + std::string(is_view ? "view " : "view target ") +
                      row[0] + " left behind by an earlier run",
                  thd, false, false);
    execute_sql((is_view ? "DROP VIEW IF EXISTS " : "DROP TABLE IF EXISTS ") +
                    row[0],
                thd, false);
  }
}
#endif
