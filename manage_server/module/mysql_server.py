""" MySQLSever class is used to start and stop the MySQL server. """
import logging
import os
from typing import Optional, List
import subprocess
import retry
import psutil
from .utils import find_executable
from .safe_process import check_call,  run_backgroud, check_output


class MySQLServer:
    """ MySQLServer class is used to start and stop the MySQL server. """

    def __init__(self, args):
        """ Method to intialize the MySQLServer class. """
        path = [args.basedir + "/bin", args.basedir +
                "bld/runtime_output_directory"]

        self.executable = {
            "mysqld": find_executable("mysqld", path),
            "mysql": find_executable("mysql", path),
            "mysqladmin": find_executable("mysqladmin", path)
        }

        self.datadir = os.path.join(args.workdir, "var", "data")
        self.port_and_socket = {
            "port": args.port_base,
            "socket": os.path.join(self.datadir, "mysql.sock")
        }
        self.process_id = None
        self.process = None
        self.abc = 2
        if self.__is_port_in_use():
            raise ValueError(
                "Port " + str(self.port_and_socket["port"]) + " is in use")

    def __common_args(self):
        """ Method to return the common arguments. """
        return ["--no-defaults",
                "--datadir=" + self.datadir,
                "--port=" + str(self.port_and_socket["port"]),
                "--socket=" + self.port_and_socket["socket"],
                "--log-error=" + os.path.join(self.datadir, "mysqld.err")]

    def __common_connect_args(self):
        """ Method to return the common connection arguments. """
        return ["--protocol=TCP", "--socket=" + self.port_and_socket["socket"]]

    def initialize(self):
        """ Method to initialize the MySQL server. """
        logging.info(f"Initializing MySQL")
        parameter = [self.executable["mysqld"]]
        parameter.extend(
            ["--datadir=" + self.datadir, "--initialize-insecure"])
        check_call(parameter)

    def __is_port_in_use(self):
        """ Method to check if the port is in use. """
        logging.debug("Checking if port is in use")
        try:
            # failed to run with access denied on some systems
            #    for conn in psutil.net_connections(kind="inet"):
            #        if conn.laddr.port == self.port_and_socket["port"]:
            #            logging.info("Port is in use by process " + str(conn.pid))
            #            return True
            # except psutil.AccessDenied as error:
            #    logging.error("Access denied to process while checking port " + str(error))
            # return False
            paramter = ["lsof", "-i", ":" + str(self.port_and_socket["port"])]
            result = check_output(paramter, True)
            lines = result.splitlines()
            if lines:
                pid = lines[1].split()[1]
                logging.info(f"Port is in use by process {pid}")
                process_name = psutil.Process(int(pid)).name()
                logging.info(f"Process name is {process_name}")
                return True
            return False
        except subprocess.CalledProcessError as error:
            logging.debug(f"Error while checking port {error}")
            return False

    def run_query(self, query: str, database: Optional[str] = None,
                  user: Optional[str] = "root", password: Optional[str] = None) -> List[List[str]]:
        """ Method to run a query. """
        parameter = [self.executable["mysql"]]
        parameter.extend(self.__common_connect_args())
        if user is not None:
            parameter.extend(["-u", user])
        if password is not None:
            parameter.extend(["-p", password])
        if database is not None:
            parameter.append(database)
        parameter.extend(["-ss", "-b"])
        parameter.extend(["-e", query])
        result = check_output(parameter)
        ret = []
        for line in result.splitlines():
            cols = line.split("\t")
            ret.append(cols)
        return ret

    def create_user(self, user, password):
        """ Method to create a user. """
        self.run_query("CREATE USER '" + user +
                       "'@'localhost' IDENTIFIED BY '" + password + "'")

    @retry.retry(Exception, tries=10, delay=1, logger=None)
    def wait_for_server(self):
        """ Method to wait for the MySQL server to start. """
        parameter = [self.executable["mysqladmin"], "ping"]
        parameter.extend(self.__common_connect_args())
        logging.debug("Waiting for MySQL server to start")

        check_call(parameter, True)

    def start(self):
        """ Method to start the MySQL server as a background process. """
        parameter = [self.executable["mysqld"]]
        parameter.extend(self.__common_args())
        self.process = run_backgroud(parameter)

        if self.process is None:
            raise ValueError("Failed to start MySQL server")
        self.process_id = self.process.pid
        logging.debug("Started mysqld with process %d", self.process_id)
        self.wait_for_server()
        logging.info(
            f"Mysql server start with socket {self.port_and_socket['socket']}")

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
            logging.info("Killing MySQL server with process %d",
                         self.process_id)
            os.kill(self.process_id, 9)
            self.process_id = None

    def kill(self):
        """ Method to kill the MySQL server. """
        self.kill_server()

    def __del__(self):  # destructor
        """ Method to delete the MySQLServer class. """
        self.kill_server()
