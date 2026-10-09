#include "json.hpp"
#include <document.h>
using namespace rapidjson;

static const int MAX_VALUE = 100;
static const int MAX_KEYS = 100;

#ifdef USE_CLICKHOUSE
/* ClickHouse's native JSON type only accepts an object at the top level, so the
   "array document" variant (odd column ids) nests its array under this key
   instead of being a top level array like it is on MySQL. */
static const char *ARRAY_KEY = "arr";

/* The JSON* functions only accept constant string paths when the argument is a
   JSON typed column, so positional access into the nested array has to go
   through a String cast. Nullable is kept so NULL documents stay NULL. */
static std::string as_string(const Column *col) {
  return "CAST(" + col->name_ + " AS Nullable(String))";
}

/* Array returning functions (JSONExtractKeys) reject a Nullable argument -
   'Nested type Array(String) cannot be inside Nullable type' - so those need a
   non-nullable document. */
static std::string as_non_null_string(const Column *col) {
  return "ifNull(" + as_string(col) + ", '{}')";
}

static std::string array_path(const Column *col) {
  return as_string(col) + ", '" + ARRAY_KEY + "', " +
         std::to_string(rand_int(MAX_KEYS) + 1);
}
#endif

std::string json_where(const Column *col) {
#ifdef USE_CLICKHOUSE
  if (col->get_id() % 2 == 0) {
    return "JSONExtractUInt(" + col->name_ + ", 'key" +
           std::to_string(rand_int(MAX_KEYS)) + "')";
  } else {
    return "JSONExtract(" + array_path(col) + ", 'UInt64')";
  }
#else
  if (col->get_id() % 2 == 0) {
    return "CAST(JSON_EXTRACT(" + col->name_ + ", '$." + "key" +
           std::to_string(rand_int(MAX_KEYS)) + "') AS UNSIGNED)";
  } else {
    /* document is json array */
    return "CAST(JSON_EXTRACT(" + col->name_ + ", '$[" +
           std::to_string(rand_int(MAX_KEYS)) + "]') AS UNSIGNED)";
  }
#endif
  return "";
}

/* method used for a column value. It can be used in where clause or set clause
 */
std::string json_value(const Column *col) {

  bool should_return_null =
      rand_int(MAX_VALUE) <= options->at(Option::NULL_PROB)->getInt() &&
      col->null_val;
  if (should_return_null)
    return "NULL";

  return "'" + std::to_string(rand_int(MAX_VALUE)) + "'";
}


std::string json_set(const Column *col) {
#ifdef USE_CLICKHOUSE
  /* ClickHouse lacks MySQL JSON mutation functions; replace with new JSON doc */
  return "'" + json_rand_doc(col) + "'";
#else
  int prob = rand_int(3);
  if (col->get_id() % 2 == 0) {
    if (prob == 0) {
      return "JSON_INSERT(" + col->name_ + ", '$." + "key" +
             std::to_string(rand_int(MAX_KEYS)) + "', " +
             std::to_string(rand_int(MAX_VALUE)) + ")";
    } else if (prob == 1) {
      return "JSON_REPLACE(" + col->name_ + ", '$." + "key" +
             std::to_string(rand_int(MAX_KEYS)) + "', " +
             std::to_string(rand_int(MAX_VALUE)) + ")";
    } else if (prob == 2) {
      return "JSON_SET(" + col->name_ + ", '$." + "key" +
             std::to_string(rand_int(MAX_KEYS)) + "', " +
             std::to_string(rand_int(MAX_VALUE)) + ")";
    } else {
      return "JSON_REMOVE(" + col->name_ + ", '$." + "key" +
             std::to_string(rand_int(MAX_KEYS)) + "')";
    }
  } else {
    /* document is json_array */
    if (prob == 0) {
      return "JSON_INSERT(" + col->name_ + ", '$[" +
             std::to_string(rand_int(MAX_KEYS)) + "]', " +
             std::to_string(rand_int(MAX_VALUE)) + ")";
    } else if (prob == 1) {
      return "JSON_REPLACE(" + col->name_ + ", '$[" +
             std::to_string(rand_int(MAX_KEYS)) + "]', " +
             std::to_string(rand_int(MAX_VALUE)) + ")";
    } else if (prob == 2) {
      return "JSON_SET(" + col->name_ + ", '$[" +
             std::to_string(rand_int(MAX_KEYS)) + "]', " +
             std::to_string(rand_int(MAX_VALUE)) + ")";
    } else {
      return "JSON_REMOVE(" + col->name_ + ", '$[" +
             std::to_string(rand_int(MAX_KEYS)) + "]')";
    }
  }
  return "";
#endif
}
std::string json_rand_doc(const Column *col) {
  Document d;
  if (col->get_id() % 2 == 0) {
    d.SetObject();
    auto &allocator = d.GetAllocator();
    for (int i = 0; i < MAX_KEYS; i++) {
      if (rand_int(4) == 1) {
        continue;
      }
      std::string key = "key" + std::to_string(i);
      std::string value = std::to_string(rand_int(MAX_VALUE));
      d.AddMember(Value(key.c_str(), allocator).Move(),
                  Value(value.c_str(), allocator).Move(), allocator);
    }
  } else {
#ifdef USE_CLICKHOUSE
    /* a top level array can't be inserted into a JSON column, nest it */
    d.SetObject();
    auto &allocator = d.GetAllocator();
    Value array(kArrayType);
    auto max_key = rand_int(MAX_KEYS, 1);
    for (int i = 0; i < max_key; i++) {
      std::string value = std::to_string(rand_int(MAX_VALUE));
      array.PushBack(Value(value.c_str(), allocator).Move(), allocator);
    }
    d.AddMember(Value(ARRAY_KEY, allocator).Move(), array, allocator);
#else
    d.SetArray();
    auto &allocator = d.GetAllocator();
    auto max_key = rand_int(MAX_KEYS, 1);
    for (int i = 0; i < max_key; i++) {
      std::string value = std::to_string(rand_int(MAX_VALUE));
      d.PushBack(Value(value.c_str(), allocator).Move(), allocator);
    }
#endif
  }
  StringBuffer buffer;
  Writer<StringBuffer> writer(buffer);
  d.Accept(writer);
  return buffer.GetString();
}

std::string json_select(const Column *col) {
#ifdef USE_CLICKHOUSE
  if (col->get_id() % 2 == 0) {
    auto prob = rand_int(MAX_KEYS);
    std::string key = "'key" + std::to_string(rand_int(MAX_KEYS)) + "'";
    if (prob == 1) {
      return "JSONHas(" + col->name_ + ", " + key + ")";
    } else if (prob == 2) {
      return as_string(col);
    } else if (prob < 5) {
      return "JSONExtractUInt(" + col->name_ + ", " + key + ")";
    } else if (prob < 8) {
      return "JSONExtractString(" + col->name_ + ", " + key + ")";
    } else if (prob < 9) {
      return "JSONExtractUInt(" + col->name_ + ", " + key + ")";
    } else {
      return "length(JSONExtractKeys(" + as_non_null_string(col) + "))";
    }
  } else {
    auto prob = rand_int(MAX_KEYS);
    if (prob == 1) {
      return "JSONHas(" + array_path(col) + ")";
    } else if (prob < 5) {
      return "JSONExtract(" + array_path(col) + ", 'UInt64')";
    } else if (prob < 8) {
      return "JSONExtract(" + array_path(col) + ", 'String')";
    } else if (prob < 9) {
      return "JSONExtract(" + array_path(col) + ", 'UInt64')";
    } else {
      return "JSONLength(" + as_non_null_string(col) + ", '" + ARRAY_KEY + "')";
    }
  }
#else
  if (col->get_id() % 2 == 0) {
    auto prob = rand_int(MAX_KEYS);
    if (prob == 1) {
      return "CAST(JSON_CONTAINS_PATH(" + col->name_ + "," +
             (rand_int(1) == 0 ? "'one'" : "'all'") + ", '$." + "key" +
             std::to_string(rand_int(MAX_KEYS)) + "') AS UNSIGNED)";
    } else if (prob == 2) {
      return "CAST(" + col->name_ + " AS CHAR)";
    } else if (prob < 5) {
      return "CAST(JSON_EXTRACT(" + col->name_ + ", '$." + "key" +
             std::to_string(rand_int(MAX_KEYS)) + "') AS UNSIGNED)";
    } else if (prob < 8) {
      return "CAST(JSON_UNQUOTE(JSON_EXTRACT(" + col->name_ + ", '$." + "key" +
             std::to_string(rand_int(MAX_KEYS)) + "')) AS UNSIGNED)";
    } else if (prob < 9) {
      return "CAST(" + col->name_ + "->" + "\"$.key" +
             std::to_string(rand_int(MAX_KEYS)) + "\" AS UNSIGNED)";
    } else {
      return "CAST(JSON_KEYS(" + col->name_ + ") AS CHAR)";
    }
  } else {
    auto prob = rand_int(MAX_KEYS);
    if (prob == 1) {
      return "CAST(JSON_CONTAINS_PATH(" + col->name_ + "," +
             (rand_int(1) == 0 ? "'one'" : "'all'") + ", '$[" +
             std::to_string(rand_int(MAX_KEYS)) + "]') AS UNSIGNED)";

    } else if (prob < 5) {
      return "CAST(JSON_EXTRACT(" + col->name_ + ", '$[" +
             std::to_string(rand_int(MAX_KEYS)) + "]') AS UNSIGNED)";
    } else if (prob < 8) {
      return "CAST(JSON_UNQUOTE(JSON_EXTRACT(" + col->name_ + ", '$[" +
             std::to_string(rand_int(MAX_KEYS)) + "]')) AS UNSIGNED)";
    } else if (prob < 9) {
      return "CAST(" + col->name_ + "->" + "\"$[" +
             std::to_string(rand_int(MAX_KEYS)) + "]\" AS UNSIGNED)";
    } else {
      return "CAST(JSON_LENGTH(" + col->name_ + ") AS UNSIGNED)";
    }
  }
#endif
  return "";
}
