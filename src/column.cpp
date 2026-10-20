/* Column, Index and their subclasses: type names, CREATE TABLE column
   definitions and random values of the right type. */

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
    /* --seq-pk: aim inside the keys actually handed out, rather than over
       --range * --records, where almost every value matches no row. This is a
       sample and never an allocation -- see Table::sample_pk(). */
    if (table_ != nullptr && table_->seq_pk_col == this)
      return table_->pk_literal(table_->sample_pk(), this);
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

#ifdef USE_CLICKHOUSE
  if (current_type == Column::COLUMN_TYPES::CHAR ||
      current_type == Column::COLUMN_TYPES::VARCHAR ||
      current_type == Column::COLUMN_TYPES::TEXT ||
      current_type == Column::COLUMN_TYPES::BLOB) {
    static const int text_words = opt_int(TEXT_WORDS);
    if (text_words > 0)
      return "'" + rand_words(text_words) + "'";
  }
#endif

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
  /* ClickHouse columns are NOT NULL by default; use Nullable() for nullable.
     If null-prob=0, user wants no NULLs at all — skip Nullable wrapper. */
  bool ch_nullable = null_val && options->at(Option::NULL_PROB)->getInt() > 0;
  std::string type_str = ch_nullable ? "Nullable(" + clause() + ")" : clause();
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
