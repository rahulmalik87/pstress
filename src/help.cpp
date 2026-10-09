#include "common.hpp"
#include "pstress.hpp"
#include <iostream>
#include <prettywriter.h>
#include <stringbuffer.h>

Opx *options = new Opx;
Ser_Opx *server_options = new Ser_Opx;

/* Process --mso=abc=30=40 to abc,{30,40}*/
void add_server_options(std::string str) {
  auto found = str.find_first_of(":", 0);
  int probability = 100;
  if (found != std::string::npos) {
    probability = std::stoi(str.substr(0, found));
    str = str.substr(found + 1, str.size());
  }

  /* extract probability */
  found = str.find_first_of("=", 0);
  if (found == std::string::npos)
    throw std::runtime_error("Invalid string, " + str);

  std::string name = str.substr(0, found);
  Server_Option *so = new Server_Option(name);
  so->prob = probability;
  server_options->push_back(so);
  str = str.substr(found + 1, str.size());

  found = str.find_first_of("=");
  while (found != std::string::npos) {
    auto val = str.substr(0, found);
    so->values.push_back(val);
    str = str.substr(found + 1, str.size());
    found = str.find_first_of("=");
  }
  /* push the last one */
  so->values.push_back(str);
}

/* process file. and push to mysqld server options */
void add_server_options_file(std::string file_name) {
  std::ifstream f1;
  f1.open(file_name);
  if (!f1)
    throw std::runtime_error("unable to open " + file_name);
  std::string option;
  while (f1 >> option)
    add_server_options(option);
  f1.close();
}

Option::~Option() {}


/* One JSON object per registered option, in Option::Opt order. "default" is
   the registered default as a string; "value" is what the option holds now,
   after the command line, the config file and the per-fork defaults. */
void dump_options_json(std::ostream &out) {
  rapidjson::StringBuffer sb;
  rapidjson::PrettyWriter<rapidjson::StringBuffer> w(sb);
  w.StartArray();
  for (auto *op : *options) {
    if (op == nullptr)
      continue;
    w.StartObject();
    w.Key("name");
    w.String(op->name.c_str());
    w.Key("enum");
    w.String(op->enum_name.c_str());
    w.Key("enum_value");
    w.Int(op->getOption());
    w.Key("type");
    switch (op->getType()) {
    case Option::BOOL:
      w.String("bool");
      break;
    case Option::INT:
      w.String("int");
      break;
    case Option::STRING:
      w.String("string");
      break;
    case Option::FLOAT:
      w.String("float");
      break;
    }
    w.Key("default");
    w.String(op->default_value.c_str());
    w.Key("value");
    switch (op->getType()) {
    case Option::BOOL:
      w.Bool(op->getBool());
      break;
    case Option::INT:
      w.Int64(op->getInt());
      break;
    case Option::STRING:
      w.String(op->getString().c_str());
      break;
    case Option::FLOAT:
      w.Double(op->getFloat());
      break;
    }
    w.Key("set_on_command_line");
    w.Bool(op->cl);
    w.Key("argument");
    w.String(op->getArgs() == no_argument         ? "none"
             : op->getArgs() == optional_argument ? "optional"
                                                  : "required");
    w.Key("sql");
    w.Bool(op->sql);
    w.Key("ddl");
    w.Bool(op->ddl);
    w.Key("short_help");
    w.String(op->short_help.c_str());
    w.Key("help");
    w.String(op->help.c_str());
    w.EndObject();
  }
  w.EndArray();
  out << sb.GetString() << std::endl;
}

void Option::print_pretty() {
  std::string s;
  s = "--" + name + "      ";
  std::cout << "--" << name << ": " << help << std::endl;
  std::cout << " default";
  switch (type) {
  case STRING:
    std::cout << ": " << getString() << std::endl;
    break;
  case INT:
    std::cout << "#: " << getInt() << std::endl;
    break;
  case BOOL:
    std::cout << ": " << getBool() << std::endl;
    break;
  case FLOAT:
    std::cout << ": " << getFloat() << std::endl;
  }
  std::cout << std::endl;
}

/* delete options and server options */
void delete_options() {
  for (auto &i : *options)
    delete i;
  delete options;
  /* delete server options */
  for (auto &i : *server_options)
    delete i;
  delete server_options;
}

void show_help(Option::Opt option) {
  /* Print all avilable options */

  if (option == Option::MAX) {
    print_version();
    for (auto &it : *options) {
      it->print_pretty();
    };
  } else {
    std::cout << " Invalid options " << std::endl;
    std::cout << " use --help --verbose to see all supported options "
              << std::endl;
  }
}

void print_version(void) {
  std::cout << " - PStress v" << PQVERSION << "-" << PQREVISION
            << " compiled with " << FORK << std::endl;
}

void show_help(std::string help) {
  print_version();
  if (help.compare("verbose") == 0) {

    for (auto &op : *options) {
      if (op != nullptr)
        op->print_pretty();
    }
  }
  bool help_found = false;
  for (auto &op : *options) {
    if (op != nullptr && help.size() > 0 && help.compare(op->getName()) == 0) {
      help_found = true;
      op->print_pretty();
    }
  }
  if (!help_found)
    std::cout << "Not a valid option! " << help << std::endl;
}

void show_help() {
  print_version();
  std::cout << " - For complete help use => pstress  --help --verbose"
            << std::endl;
  std::cout << " - For help on any option => pstress --help=OPTION e.g. \n "
               "            pstress --help=no-ddl"
            << std::endl;
}

void show_cli_help(void) {
  print_version();
  std::cout << " - General usage: pstress --user=USER --password=PASSWORD "
               "--database=DATABASE"
            << std::endl;
  std::cout << "=> pstress doesn't support multiple nodes when using "
               "commandline options mode!"
            << std::endl;
  std::cout << "---------------------------------------------------------------"
               "--------------------------\n"
            << "| OPTION               | EXPLANATION                           "
               "       | DEFAULT         |\n"
            << "---------------------------------------------------------------"
               "--------------------------\n"
            << "--database             | The database to connect to            "
               "       | \n"
            << "--address              | IP address to connect to              "
               "       | \n"
            << "--port                 | The port to connect to                "
               "       | 3306\n"
            << "--infile               | The SQL input file                    "
               "       | pquery.sql\n"
            << "--logdir               | Log directory                         "
               "       | /tmp\n"
            << "--socket               | Socket file to use                    "
               "       | /tmp/my.sock\n"
            << "--user                 | The MySQL userID to be used           "
               "       | shell user\n"
            << "--password             | The MySQL user's password             "
               "       | <empty>\n"
            << "--threads              | The number of threads to use          "
               "       | 1\n"
            << "--queries-per-thread   | The number of queries per thread      "
               "       | 10000\n"
            << "--verbose              | Duplicates the log to console when "
               "threads=1 | no\n"
            << "--log-all-queries      | Log all queries (succeeded and "
               "failed)       | no\n"
            << "--log-succeeded-queries| Log succeeded queries                 "
               "       | no\n"
            << "--log-failed-queries   | Log failed queries                    "
               "       | no\n"
            << "--no-shuffle           | Execute SQL sequentially              "
               "       | randomly\n"
            << "--test-connection      | Test connection to server and exit    "
               "       | no\n"
            << "--log-query-number     | Write query # to logs                 "
               "       | no\n"
            << "--log-client-output    | Log query output to separate file     "
               "       | no\n"
            << "--ddl		    | USE DDL in command line option           "
               "    | true\n"
            << "---------------------------------------------------------------"
               "--------------------------"
            << std::endl;
}

void show_config_help(void) {

  print_version();

  std::cout << " - Usage: pstress --config-file=pstress.cfg" << std::endl;
  std::cout << " - CLI params has been replaced by config file (INI format)"
            << std::endl;
  std::cout << " - You can redefine any global param=value pair in "
               "host-specific section"
            << std::endl;
  std::cout << "\nConfig example:\n" << std::endl;
  std::cout <<

      "[node0.domain.tld]\n"
            << "# The database to connect to\n"
            << "database = \n"
            << "# IP address to connect to, default is AF_UNIX\n"
            << "address = <empty>\n"
            << "# The port to connect to\n"
            << "port = 3306\n"
            << "# The SQL input file\n"
            << "infile = pquery.sql\n"
            << "# Directory to store logs\n"
            << "logdir = /tmp\n"
            << "# Socket file to use\n"
            << "socket = /tmp/my.sock\n"
            << "# The MySQL userID to be used\n"
            << "user = test\n"
            << "# The MySQL user's password\n"
            << "password = test\n"
            << "# The number of threads to use by worker\n"
            << "threads = 1\n"
            << "# The number of queries per thread\n"
               "queries-per-thread = 10000\n"
            << "# Duplicates the log to console when threads=1 and workers=1\n"
               "verbose = No\n"
            << "# Log all queries\n"
            << "log-all-queries = No\n"
            << "# Log succeeded queries\n"
            << "log-succeeded-queries = No\n"
            << "# Log failed queries\n"
            << "log-failed-queries = No\n"
            << "# Log output from executed query (separate log)\n"
            << "log-client-output = No\n"
            << "# Log query numbers along the query results and statistics\n"
            << "log-query-number = No\n\n"
            << "[node1.domain.tld]\n"
            << "address = 10.10.6.10\n"
            << "# default for \"run\" is No, need to set it explicitly\n"
            << "run = Yes\n\n"
            << "[node2.domain.tld]\n"
            << "address = 10.10.6.11\n"
            << std::endl;
}

void read_option_prob_file(const std::string &prob_file) {
  std::ifstream file(prob_file);
  if (!file.is_open()) {
    std::cout << "Error opening file " << prob_file << std::endl;
    exit(EXIT_FAILURE);
  }

  std::string line;
  std::cout << "Picking options from " << prob_file << std::endl;
  while (std::getline(file, line)) {
    if (line[0] == '#' || line.empty())
      continue;

    if (line.find(':') == std::string::npos) {
      std::cerr << "Invalid format " << std::endl;
      exit(EXIT_FAILURE);
    }

    int prob = atoi(line.substr(0, line.find(":")).c_str());

    if (prob < rand_int(100)) {
      continue;
    }

    auto line_options =
        splitStringToArray<std::string>(line.substr(line.find(":") + 1), ';');

    for (auto &each_option : line_options) {

      auto option_value = splitStringToArray<std::string>(each_option, '=');
      auto option = option_value.at(0);
      std::vector<std::string> values;
      if (option_value.size() > 1)
        values = splitStringToArray<std::string>(option_value.at(1), '|');

      Option *op = nullptr;
      for (auto &i : *options) {
        if (i != nullptr && option.compare(i->getName()) == 0) {
          op = i;
          break;
        }
      }
      if (op == nullptr) {
        std::cout << "Invalid option  " << option << " found in " << prob_file;
        exit(EXIT_FAILURE);
      }
      if (op->cl)
        continue;

      if (op->getType() == Option::BOOL) {
        op->setBool(true);
        std::cout << option << std::endl;
        continue;
      }

      if (values.size() == 0) {
        std::cout << "option " << option << " does not have values in "
                  << prob_file << std::endl;
        exit(EXIT_FAILURE);
      }

      if (op->getType() == Option::INT) {
        auto value = values.at(rand_int(values.size() - 1));
        std::cout << option << ":" << value << std::endl;
        op->setInt(value);
      } else {
        auto value = values.at(rand_int(values.size() - 1));
        op->setString(value);
        std::cout << option << ":" << value << std::endl;
      }
    }
  }
}
