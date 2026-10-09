/* ClickHouse table and session settings pool (--table-settings-file):
   parsing, per-table picks, and the alter:/session: entries. */

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

static std::string trim_ws(const std::string &str) {
  auto begin = str.find_first_not_of(" \t\r");
  if (begin == std::string::npos)
    return "";
  return str.substr(begin, str.find_last_not_of(" \t\r") - begin + 1);
}

#ifdef USE_CLICKHOUSE
/* Append "SETTINGS <setting>" to a query. Used by compare_result_with_setting()
   and by grammar_sql() for --ch-grammar-query-settings; ClickHouse only, the
   MySQL build has no SETTINGS clause. */
std::string add_settings_clause(const std::string &sql,
                                const std::string &setting) {
  auto stripped = trim_ws(sql);
  /* load_grammar_sql_from() only strips trailing whitespace, so a grammar line
     can still end in ';' and we would emit "... ; SETTINGS x = 1" */
  while (!stripped.empty() && stripped.back() == ';')
    stripped = trim_ws(stripped.substr(0, stripped.size() - 1));

  /* ClickHouse takes one SETTINGS clause per query level and it has to come
     last, so join the query's own clause rather than emit a second one.
     "settings" only reads as the keyword when an assignment follows it, which
     keeps a column or a string literal of that name from matching. A match
     inside a subquery does not count either: that clause belongs to the
     subquery and the outer query still needs its own, so only a match that is
     not closed off by a ')' before the end of the string is ours to extend. */
  static const std::regex settings_clause(
      R"(\bsettings\s+[A-Za-z_][A-Za-z0-9_]*\s*=)", std::regex::icase);
  bool has_settings = false;
  for (auto it = std::sregex_iterator(stripped.begin(), stripped.end(),
                                      settings_clause);
       it != std::sregex_iterator(); ++it) {
    int depth = 0;
    for (auto c = stripped.cbegin() + it->position(); c != stripped.cend(); ++c) {
      if (*c == '(')
        depth++;
      else if (*c == ')')
        depth--;
    }
    if (depth >= 0)
      has_settings = true;
  }

  return stripped + (has_settings ? ", " : " SETTINGS ") + setting;
}
#endif

/* split on delimiter keeping each piece whole. splitStringToArray() reads with
   >> and so stops at the first space, which would turn "a = 1" into "a". */
static std::vector<std::string> split_and_trim(const std::string &input,
                                               char delimiter) {
  std::vector<std::string> result;
  std::istringstream iss(input);
  std::string piece;
  while (std::getline(iss, piece, delimiter)) {
    auto trimmed = trim_ws(piece);
    if (!trimmed.empty())
      result.push_back(trimmed);
  }
  return result;
}

/* settings pstress emits itself — the lightweight mutation paths depend on
   them, so the settings file is not allowed to override them */
static bool is_reserved_setting(const std::string &name) {
  return name == "enable_block_number_column" ||
         name == "enable_block_offset_column" ||
         /* the three the projection workload owns; see Table::definition() on
            why a probabilistic roll would make a run look healthy while
            testing nothing */
         name == "deduplicate_merge_projection_mode" ||
         name == "lightweight_mutation_projection_mode" ||
         name == "allow_nullable_key";
}

[[noreturn]] static void settings_file_error(const std::string &file,
                                            int line_no,
                                            const std::string &line,
                                            const std::string &why) {
  std::cerr << "Invalid table setting at " << file << ":" << line_no << ": "
            << why << "\n  " << line << std::endl;
  exit(EXIT_FAILURE);
}

/* one value for a spec: a random element of the list, or a random integer
   inside the range */
static std::string resolve_setting_value(const SettingSpec &spec) {
  if (spec.is_range)
    return std::to_string(rand_int(spec.hi, spec.lo));
  if (spec.values.empty())
    return "";
  return spec.values.at(rand_int(spec.values.size() - 1));
}

/* Read the settings pool. Table entries are kept for per-table rolling in
   pick_table_settings(); session entries are rolled here, once, so that every
   thread issues the same SET — threads disagreeing about an
   allow_experimental_* flag would make CREATE TABLE fail on some of them. */
void load_table_settings_pool() {
  g_table_settings.clear();
  g_session_settings.clear();
  g_alterable_table_settings.clear();
  g_fixed_table_settings.clear();

  if (options->at(Option::NO_TABLE_SETTINGS)->getBool())
    return;

  /* a fixed clause is used for every table, so normalize it here instead of
     re-parsing it per table, and drop the settings pstress owns — ClickHouse
     accepts a duplicated setting and lets the last one win, which would
     silently turn off what the mutation paths need */
  const auto fixed = opt_string(CH_TABLE_SETTINGS);
  if (!fixed.empty()) {
    for (const auto &setting : split_and_trim(fixed, ',')) {
      auto eq = setting.find('=');
      if (eq == std::string::npos) {
        std::cerr << "Invalid --table-settings entry, expected <name> = <value>"
                  << ":\n  " << setting << std::endl;
        exit(EXIT_FAILURE);
      }
      if (is_reserved_setting(trim_ws(setting.substr(0, eq)))) {
        std::cout << "Ignoring " << trim_ws(setting.substr(0, eq))
                  << " in --table-settings, pstress always sets it"
                  << std::endl;
        continue;
      }
      if (!g_fixed_table_settings.empty())
        g_fixed_table_settings += ", ";
      g_fixed_table_settings += setting;
    }
    return;
  }

  auto *file_opt = options->at(Option::CH_TABLE_SETTINGS_FILE);
  const std::string file_name = file_opt->getString();
  if (file_name.empty())
    return;

  /* the shipped pool sits next to the binary, like the dictionary file */
  std::filesystem::path path(file_name);
  if (path.is_relative())
    path = std::filesystem::path(getExecutablePath()).parent_path() / path;
  const std::string path_str = path.string();

  std::ifstream file(path);
  if (!file.is_open()) {
    /* only fatal when the user named this file themselves */
    if (file_opt->cl) {
      std::cerr << "Unable to open table settings file " << path_str
                << std::endl;
      exit(EXIT_FAILURE);
    }
    std::cout << "No table settings file at " << path_str
              << ", tables will use server default settings" << std::endl;
    return;
  }

  std::string line;
  int line_no = 0;
  while (std::getline(file, line)) {
    line_no++;
    std::string rest = trim_ws(line);
    if (rest.empty() || rest[0] == '#')
      continue;

    bool is_session = false;
    bool is_alterable = false;
    if (rest.rfind("session:", 0) == 0) {
      is_session = true;
      rest = trim_ws(rest.substr(strlen("session:")));
    } else if (rest.rfind("alter:", 0) == 0) {
      /* a session setting is not a table setting, so there is nothing an
         ALTER TABLE could do with it */
      is_alterable = true;
      rest = trim_ws(rest.substr(strlen("alter:")));
    }

    auto eq = rest.find('=');
    if (eq == std::string::npos)
      settings_file_error(
          path_str, line_no, line,
          "expected [session:|alter:][<prob>:]<name> = <values>");

    SettingSpec spec;

    /* optional probability prefix. A colon after the '=' belongs to an
       int:<lo>..<hi> value, so only look before it. */
    auto colon = rest.find(':');
    if (colon != std::string::npos && colon < eq) {
      auto prob_str = trim_ws(rest.substr(0, colon));
      if (prob_str.empty() || prob_str.size() > 3 ||
          prob_str.find_first_not_of("0123456789") != std::string::npos)
        settings_file_error(path_str, line_no, line,
                            "probability must be an integer 0-100");
      spec.prob = std::stoi(prob_str);
      if (spec.prob > 100)
        settings_file_error(path_str, line_no, line,
                            "probability must be an integer 0-100");
      rest = trim_ws(rest.substr(colon + 1));
      eq = rest.find('=');
      if (eq == std::string::npos)
        settings_file_error(path_str, line_no, line,
                            "expected <name> = <values> after the probability");
    }

    spec.name = trim_ws(rest.substr(0, eq));
    auto values = trim_ws(rest.substr(eq + 1));
    if (spec.name.empty())
      settings_file_error(path_str, line_no, line, "missing setting name");
    if (values.empty())
      settings_file_error(path_str, line_no, line, "missing value");

    if (values.rfind("int:", 0) == 0) {
      auto range = values.substr(strlen("int:"));
      auto dots = range.find("..");
      if (dots == std::string::npos)
        settings_file_error(path_str, line_no, line,
                            "integer range must be int:<lo>..<hi>");
      try {
        spec.lo = std::stol(trim_ws(range.substr(0, dots)));
        spec.hi = std::stol(trim_ws(range.substr(dots + 2)));
      } catch (const std::exception &) {
        settings_file_error(path_str, line_no, line,
                            "integer range must be int:<lo>..<hi>");
      }
      if (spec.hi < spec.lo)
        settings_file_error(path_str, line_no, line,
                            "integer range ends below where it starts");
      spec.is_range = true;
    } else {
      spec.values = split_and_trim(values, '|');
      if (spec.values.empty())
        settings_file_error(path_str, line_no, line, "missing value");
    }

    if (is_reserved_setting(spec.name)) {
      std::cout << "Ignoring " << spec.name << " at " << path_str << ":"
                << line_no << ", pstress always sets it" << std::endl;
      continue;
    }

    if (is_session) {
      if (rand_int(99) < spec.prob)
        g_session_settings.push_back(spec.name + " = " +
                                     resolve_setting_value(spec));
    } else {
      spec.alterable = is_alterable;
      g_table_settings.push_back(spec);
      /* an alter: entry is still rolled onto CREATE TABLE at its probability,
         so a table can start out with the setting; on top of that it is what
         --modify-table-setting picks from, and MODIFY SETTING can set one the
         table never rolled */
      if (is_alterable)
        g_alterable_table_settings.push_back(spec);
    }
  }
  file.close();

  std::cout << "Loaded " << g_table_settings.size() << " table settings from "
            << path_str << ", " << g_alterable_table_settings.size()
            << " of them alterable" << std::endl;
  for (const auto &setting : g_session_settings)
    std::cout << "Session setting: SET " << setting << std::endl;
}

/* Settings for one table: the fixed clause if given, otherwise every pool
   entry whose probability roll passes, with one of its values. */
std::string pick_table_settings() {
  if (options->at(Option::NO_TABLE_SETTINGS)->getBool())
    return "";

  if (!g_fixed_table_settings.empty())
    return g_fixed_table_settings;

  std::string clause;
  for (const auto &spec : g_table_settings) {
    /* rand_int(99) is 0-99, so prob 100 always fires and prob 0 never does */
    if (rand_int(99) >= spec.prob)
      continue;
    if (!clause.empty())
      clause += ", ";
    clause += spec.name + " = " + resolve_setting_value(spec);
  }
  return clause;
}

/* Table::settings is a flat "name = value, name = value" clause rather than a
   map: it is what CREATE TABLE emits and what the metadata file stores, so
   keeping it as the single copy of the truth means MODIFY/RESET SETTING need no
   new persisted state and no metadata version bump. These three keep it in step
   with the ALTERs that succeeded. */
std::vector<std::string> ch_settings_names(const std::string &clause) {
  std::vector<std::string> names;
  for (const auto &setting : split_and_trim(clause, ',')) {
    auto eq = setting.find('=');
    if (eq == std::string::npos)
      continue;
    auto name = trim_ws(setting.substr(0, eq));
    if (!name.empty())
      names.push_back(name);
  }
  return names;
}

void ch_settings_upsert(std::string &clause, const std::string &name,
                        const std::string &value) {
  std::string rebuilt;
  bool replaced = false;
  for (const auto &setting : split_and_trim(clause, ',')) {
    auto eq = setting.find('=');
    const bool same =
        eq != std::string::npos && trim_ws(setting.substr(0, eq)) == name;
    if (!rebuilt.empty())
      rebuilt += ", ";
    if (same) {
      rebuilt += name + " = " + value;
      replaced = true;
    } else {
      rebuilt += setting;
    }
  }
  if (!replaced) {
    if (!rebuilt.empty())
      rebuilt += ", ";
    rebuilt += name + " = " + value;
  }
  clause = rebuilt;
}

void ch_settings_erase(std::string &clause, const std::string &name) {
  std::string rebuilt;
  for (const auto &setting : split_and_trim(clause, ',')) {
    auto eq = setting.find('=');
    if (eq != std::string::npos && trim_ws(setting.substr(0, eq)) == name)
      continue;
    if (!rebuilt.empty())
      rebuilt += ", ";
    rebuilt += setting;
  }
  clause = rebuilt;
}

/* One alter: entry of the pool with a fresh value for it. The value is rolled
   the same way CREATE TABLE rolls it, so the ALTER can only ever set something
   the table could have been created with. */
bool ch_roll_alterable_setting(std::string &name, std::string &value) {
  if (g_alterable_table_settings.empty())
    return false;
  const auto &spec = g_alterable_table_settings.at(
      rand_int(g_alterable_table_settings.size() - 1));
  name = spec.name;
  value = resolve_setting_value(spec);
  return !value.empty();
}
