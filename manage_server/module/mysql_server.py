""" MySQLSever class is used to start and stop the MySQL server. """
import logging
from .utils import find_executable

class MySQLServer:
    """ MySQLServer class is used to start and stop the MySQL server. """

    def __init__(self, args):
        """ Method to intialize the MySQLServer class. """
        self.mysqld = find_executable("mysqld", args.basedir)
        self.mysql = find_executable("mysql", args.basedir)

    def start(self):
        """ Method to start the MySQL server. """
        logging.info("MySQLServer start called")

    def stop(self):
        """ Method to stop the MySQL server. """
        logging.info("MySQLServer stop called")

    def initialize(self):
        """ Method to initialize the MySQL server. """
        logging.info("MySQLServer initialize called")
