#include "node.hpp"
#include "common.hpp"
#include "random_test.hpp"
#include <cerrno>
#include <cstring>
#include <iostream>

std::mutex node_mutex;
Node::Node() {
  performed_queries_total = 0;
  failed_queries_total = 0;
}

void Node::end_node() {
  writeFinalReport();
  if (general_log)
    general_log.close();
}

bool Node::createGeneralLog() {
  std::string file_name;
  file_name = myParams.myName + "_" +
              options->at(Option::DATABASE)->getString() + "_general_step_" +
              std::to_string(options->at(Option::STEP)->getInt()) + ".log";
  setupClientOutputLog(general_log, myParams.logdir, file_name);
  return true;
}

void Node::writeFinalReport() {
  if (general_log.is_open()) {
    std::ostringstream exitmsg;
    exitmsg.precision(2);
    exitmsg << std::fixed;
    exitmsg << "* NODE SUMMARY: " << failed_queries_total << "/"
            << performed_queries_total << " queries failed, ("
            << (performed_queries_total - failed_queries_total) * 100.0 /
                   performed_queries_total
            << "% were successful)";
    general_log << exitmsg.str() << std::endl;
    std::cout << exitmsg.str() << std::endl;
  }
}

int Node::startWork() {

  if (!createGeneralLog()) {
    std::cerr << "Exiting..." << std::endl;
    return 2;
  }

  std::string connectionInfo =
      (myParams.socket != "") ? myParams.socket : std::to_string(myParams.port);

  std::cout << "- Connecting to " << myParams.myName << " [" << connectionInfo
            << "]..." << std::endl;
  general_log << "- Connecting to " << myParams.myName << " [" << connectionInfo
              << "]..." << std::endl;

  /* END log replaying */
  std::vector<std::thread> workers;
  auto start = std::chrono::system_clock::now();
  try {
    for (int i = 0; i < myParams.threads; ++i) {
      // Assuming thread_id starts at 0 unless otherwise specified
      workers.emplace_back(&Node::workerThread, this, i);
      // sleep for 10 milliseconds
    }
  } catch (const std::system_error &e) {
    // Handle thread creation failure
    std::cerr << "Thread creation failed: " << e.what() << std::endl;
    for (auto &t : workers) {
      if (t.joinable())
        t.join(); // Join any threads that were created
    }
    throw; // Re-throw or handle the exception further up the call stack
  }

  // Join all threads
  for (auto &t : workers) {
    if (t.joinable())
      t.join();
  }
  std::cout << "Time taken by pstress is " +
                   std::to_string(
                       std::chrono::duration_cast<std::chrono::seconds>(
                           std::chrono::system_clock::now() - start)
                           .count()) +
                   " seconds"
            << std::endl;

  return EXIT_SUCCESS;
}
void setupClientOutputLog(std::ofstream &log_file, std::string_view logdir,
                          std::string_view filename) {

  namespace fs = std::filesystem;
  try {
    // Ensure log directory exists
    if (!logdir.empty() && !fs::exists(logdir)) {
      fs::create_directories(logdir);
    }

    // Construct and normalize path
    fs::path clientPath = fs::path(logdir) / filename;
    clientPath = fs::absolute(clientPath); // Normalize to absolute path

    // Open the log file
    log_file.open(clientPath, std::ios::out | std::ios::trunc);
    if (!log_file.is_open()) {
      throw std::runtime_error("Unable to open logfile " + clientPath.string() +
                               ": " + std::strerror(errno));
    }
  } catch (const fs::filesystem_error &e) {
    throw std::runtime_error("Filesystem error for path '" +
                             std::string(logdir) + "': " + e.what());
  }
}
