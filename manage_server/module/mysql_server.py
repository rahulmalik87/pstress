""" MySQLSever class is used to start and stop the MySQL server. """
import logging
import os
import subprocess
from typing import Optional , List
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
        if not self.check_port_free(self.port):
            raise ValueError("Port already in use")

    def __common_args(self):
        return ["--no-defaults", "--datadir=" + self.datadir + " --port=" + str(self.port)]

    def start(self):
        """ Method to start the MySQL server. """
        parameter = [self.mysqld]
        parameter.extend(self.__common_args())
        logging.info("MySQLServer start called")

    def stop(self):
        """ Method to stop the MySQL server. """
        logging.info("MySQLServer stop called")
        parameter = [self.mysql, "-u", "root", "-e", "shutdown"]
        check_call(parameter)

    def kill(self):
        """ Method to kill the MySQL server. """
        logging.info("MySQLServer kill called")
        parameter = ["kill", "-9", self.mysqld]
        check_call(parameter)

    def initialize(self):
        """ Method to initialize the MySQL server. """
        parameter = [self.mysqld]
        logging.info("Initializing MySQL server")
        parameter.append("--datadir=" + self.datadir)
        parameter.append("--initialize-insecure")
        check_call(parameter)

    @retry.retry(subprocess.CalledProcessError, tries=5, delay=5, backoff=2)
    def check_port_free(self, port: int):
        """ Method to check if the port is free. """
        try:
            check_call(["nc", "-z", "localhost", str(port)])
            return False
        except subprocess.CalledProcessError:
            return True

    def run_query(self, query: str, database : Optional[str] = None,
                  user: Optional[str] = "root", password: Optional[str] = None):
        """ Method to run a query. """
        parameter = [self.mysql, "-u", user, "-e", query]
        if password is not None:
            parameter.extend(["-p", password])
        if database is not None:
            parameter.append(database)
        check_call(parameter)

   #run query and return ressult as list
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
        self.run_query("CREATE USER '" + user + "'@'localhost' IDENTIFIED BY '" + password + "'")
