/* Random value generators: numbers, strings, dictionary words, dates and
   times. Everything here draws from the thread-local rng. */

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

PSTRESS_TARGET_CLONES
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

std::string rand_bit(int length) {
  std::string bit = "b\'";
  for (int i = 0; i < length; i++) {
    bit += std::to_string(rand_int(1));
  }
  bit += "\'";
  return bit;
}

PSTRESS_TARGET_CLONES
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
PSTRESS_TARGET_CLONES
std::string rand_string(size_t size) {
  /* if bigger string is requested ensure minimum 10 */
  if (size > 10)
    size = rand_int(size, 10);

  std::string rs;

  if (random_strs.empty() || size == 0) {
    rs = generateRandomString(size);
  } else {
    auto pick = [&]() -> const std::string & {
      return random_strs.at(rand_int(random_strs.size() - 1));
    };

    int strategy = rand_int(9); /* 0-9 */

    if (strategy < 6) {
      /* 60%: truncate — pick one word, use up to size chars */
      rs = pick().substr(0, size);
    } else if (strategy < 8) {
      /* 20%: word + numeric suffix for high cardinality */
      const std::string &w = pick();
      std::string suffix = std::to_string(rand_int(9999));
      rs = (w + suffix).substr(0, size);
    } else if (strategy == 8) {
      /* 10%: join two words with a separator */
      static const char seps[] = {' ', '-', '_', '/'};
      char sep = seps[rand_int(3)];
      std::string combined = pick() + sep + pick();
      rs = combined.substr(0, size);
    } else {
      /* 10%: pure random noise */
      rs = generateRandomString(size);
    }
  }

#ifdef USE_DUCKDB
  rs = std::regex_replace(rs, std::regex("'"), "''");
#endif
  return rs;
}

#ifdef USE_CLICKHOUSE
/* --text-words: 1 to max_words dictionary words joined by a separator, mostly
   a space, sometimes one that only some tokenizers split on. One word in ten is
   capitalized, so a lower() preprocessor and a case sensitive needle disagree
   on something. The dictionary has no quotes, so the value needs no escaping. */
std::string rand_words(int max_words) {
  static const char *const seps[] = {" ", " ", " ", " ", ", ", "-", ". ", "/",
                                     "_"};
  const int n = rand_int(max_words, 1);
  std::string rs;
  for (int i = 0; i < n; i++) {
    if (i > 0)
      rs += seps[rand_int(8)];
    std::string w = random_strs.at(rand_int(random_strs.size() - 1));
    if (!w.empty() && rand_int(9) == 0)
      w[0] = std::toupper(static_cast<unsigned char>(w[0]));
    rs += w;
  }
  return rs;
}

/* The dictionary entries that are a single token: letters and digits only.
   It also has compounds like "wide-query-merger", which hasToken rejects as a
   needle, and which a LIKE needle would match as several tokens. */
static const std::vector<std::string> &single_token_words() {
  static const std::vector<std::string> words = [] {
    std::vector<std::string> out;
    for (const auto &w : random_strs)
      if (!w.empty() && std::all_of(w.begin(), w.end(), [](unsigned char c) {
            return std::isalnum(c);
          }))
        out.push_back(w);
    return out;
  }();
  return words;
}

/* a random single token word of the dictionary, for RAND_WORD */
std::string rand_word() {
  const auto &words = single_token_words();
  return words.at(rand_int(words.size() - 1));
}

/* The first (RAND_PREFIX) or last (RAND_SUFFIX) 2 to 8 characters of a random
   word, never the whole word, for LIKE 'abc%' and LIKE '%xyz' needles.
   text_index_like_min_pattern_length defaults to 4, so the range straddles
   it: shorter needles take the path that does not use the index. */
std::string rand_word_part(bool prefix) {
  std::string w;
  do
    w = rand_word();
  while (w.size() < 3);
  const size_t len = rand_int(std::min<size_t>(8, w.size() - 1), 2);
  return prefix ? w.substr(0, len) : w.substr(w.size() - len);
}
#endif

std::string table_prefix() {
  return options->at(Option::TABLE_PREFIX)->getString();
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
std::string rand_date() {
  std::ostringstream out;
#ifdef USE_CLICKHOUSE
  /* ClickHouse Date range is 1970-01-01 .. 2149-06-06. Out-of-range values are
     silently clamped to the boundary (date_time_overflow_behavior defaults to
     "ignore"), so a 1000..9999 year range makes ~99% of generated dates land
     on 2149-06-06. That collapses the column to a single value: every
     "WHERE dt = <date>" predicate then matches the whole table and each
     lightweight UPDATE becomes a full-table patch part. Stay in range. */
  int year = rand_int(2149, 1970);
  int month = (year == 2149) ? rand_int(6, 1) : rand_int(12, 1);
#else
  /* MySQL DATE range is 1000-01-01 .. 9999-12-31. */
  int year = rand_int(9999, 1000); // Year from 1000 to 9999
  int month = rand_int(12, 1);     // Month from 1 to 12
#endif
  int max_day = 28;                // Default to 28 for February
  if (month == 4 || month == 6 || month == 9 || month == 11) {
    max_day = 30; // April, June, September, November
  } else if (month != 2) {
    max_day = 31; // January, March, May, July, August, October, December
  } else if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
    max_day = 29; // Leap year February
  }
#ifdef USE_CLICKHOUSE
  if (year == 2149 && month == 6) {
    max_day = 6; // Date tops out at 2149-06-06
  }
#endif
  int day = rand_int(max_day, 1); // Day based on month and leap year
  out << std::setfill('0') << std::setw(4) << year << "-" << std::setw(2)
      << month << "-" << std::setw(2) << day;
  return out.str();
}

std::string rand_datetime() {
  std::ostringstream out;
#ifdef USE_CLICKHOUSE
  /* ClickHouse DateTime range is 1970-01-01 00:00:00 .. 2106-02-07 06:28:15
     (UTC). Same clamping problem as rand_date() -- see the comment there. */
  int year = rand_int(2106, 1970);
  int month = (year == 2106) ? rand_int(2, 1) : rand_int(12, 1);
#else
  /* MySQL DATETIME range is 1000-01-01 .. 9999-12-31. */
  int year = rand_int(9999, 1000); // Year from 1000 to 9999
  int month = rand_int(12, 1);     // Month from 1 to 12
#endif
  int max_day = 28;                // Default to 28 for February
  if (month == 4 || month == 6 || month == 9 || month == 11) {
    max_day = 30; // April, June, September, November
  } else if (month != 2) {
    max_day = 31; // January, March, May, July, August, October, December
  } else if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
    max_day = 29; // Leap year February
  }
#ifdef USE_CLICKHOUSE
  if (year == 2106 && month == 2) {
    max_day = 7; // DateTime tops out on 2106-02-07
  }
#endif
  int day = rand_int(max_day, 1); // Day based on month and leap year
  int hour = rand_int(23, 0);     // Hour from 0 to 23
  int minute = rand_int(59, 0);   // Minute from 0 to 59
  int second = rand_int(59, 0);   // Second from 0 to 59
#ifdef USE_CLICKHOUSE
  if (year == 2106 && month == 2 && day == 7) {
    hour = rand_int(6, 0); // DateTime tops out at 2106-02-07 06:28:15
    if (hour == 6) {
      minute = rand_int(28, 0);
      if (minute == 28) {
        second = rand_int(15, 0);
      }
    }
  }
#endif

  out << std::setfill('0') << std::setw(4) << year << "-" << std::setw(2)
      << month << "-" << std::setw(2) << day << " " << std::setw(2) << hour
      << ":" << std::setw(2) << minute << ":" << std::setw(2) << second;
  return out.str();
}
std::string rand_timestamp(int precision, bool include_timezone) {
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
