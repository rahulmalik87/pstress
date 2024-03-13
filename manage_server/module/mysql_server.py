""" MySQLSever class is used to start and stop the MySQL server. """
import logging
import os
from typing import Optional, List
import subprocess
import retry
from .utils import find_executable, check_call


class MySQLServer:
    """ MySQLServer class is used to start and stop the MySQL server. """

    def __init__(self, args):
        """ Method to intialize the MySQLServer class. """

        self.mysqld = find_executable("mysqld", [args.basedir + "/bin",
                                                 args.basedir + "bld/runtime_output_directory"])
        self.mysql = find_executable("mysql", [args.basedir + "/bin",
                                               args.basedir + "bld/runtime_output_directory"])
        self.datadir = os.path.join(args.workdir, "var", "data")
        self.port = args.port_base
        self.mysqlx_port = args.port_base + 60
        self.process_id = None
        self.process = None
        if self.__is_port_in_use():
            raise ValueError("Port " + str(self.port) + " is in use")

    def __common_args(self):
        return ["--no-defaults",
                "--datadir=" + self.datadir,
                "--port=" + str(self.port),
                "--log-error=" + os.path.join(self.datadir, "mysqld.err")]

    def initialize(self):
        """ Method to initialize the MySQL server. """
        parameter = [self.mysqld]
        logging.info("Initializing MySQL server")
        parameter.append("--datadir=" + self.datadir)
        parameter.append("--initialize-insecure")
        check_call(parameter)
        logging.info(f"MySQL server initialized in {self.datadir}")

    def __is_port_in_use(self):
        """ Method to check if the port is in use. """
        logging.debug("Checking if port is in use")
        try:
            result = subprocess.check_output(
                "lsof -i :" + str(self.port), shell=True)
            lines = result.decode("utf-8").split("\n")
            if lines:
                pid = int(lines[1].split()[1])
                logging.info("Port is in use by process " + str(pid))
                return True
            return False
        except subprocess.CalledProcessError:
            return False

    def run_query(self, query: str, database: Optional[str] = None,
                  user: Optional[str] = "root", password: Optional[str] = None):
        """ Method to run a query. """
        parameter = [self.mysql, "-u", user, "-e", query]
        parameter.extend(["--port=" + str(self.port)])
        if password is not None:
            parameter.extend(["-p", password])
        if database is not None:
            parameter.append(database)
        check_call(parameter)

    def run_query_return(self, query: str, user: Optional[str] = "root",
                         password: Optional[str] = None) -> List[str]:
        """ Method to run a query and return the result as a list. """
        parameter = [self.mysql, "-u", user, "-e", query]
        if password is not None:
            parameter.extend(["-p", password])
        output = subprocess.check_output(parameter)
        return output.decode("utf-8").split("\n")

    def create_user(self, user, password):
        """ Method to create a user. """
        self.run_query("CREATE USER '" + user +
                       "'@'localhost' IDENTIFIED BY '" + password + "'")

    @retry.retry(Exception, tries=10, delay=1, backoff=2)
    def wait_for_server(self):
        """ Method to wait for the MySQL server to start. """
        logging.info("Waiting for MySQL server to start")
        self.run_query("SELECT 1")

    def start(self):
        """ Method to start the MySQL server as a background process. """
        parameter = [self.mysqld]
        parameter.extend(self.__common_args())
        logging.info(" ".join(parameter))
        logging.info("Starting MySQL server as a background process")
        self.process = subprocess.Popen(parameter)
        if self.process is None:
            raise ValueError("Failed to start MySQL server")
        self.process_id = self.process.pid
        self.wait_for_server()

    def stop(self):
        """ Method to stop the MySQL server. """
        if self.process is not None:
            logging.info(f"Stopping MySQL server")
            self.process.terminate()
            self.process.wait()
            self.process_id = None
            self.process = None

    def kill_server(self):
        """ Method to kill the MySQL server. """
        if self.process_id is not None:
            logging.info(f"Killing MySQL server")
            os.kill(self.process_id, 9)
            self.process_id = None

    def kill(self):
        """ Method to kill the MySQL server. """
        self.kill_server()

    def __del__(self):  # destructor
        """ Method to delete the MySQLServer class. """
        self.kill_server()
