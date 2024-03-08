""" MySQLSever class is used to start and stop the MySQL server. """
import logging
import subprocess
import threading
class MySQLServer:
    """ MySQLServer class is used to start and stop the MySQL server. """
    def __init__(self):
        """ Method to intialize the MySQLServer class. """
        logging.info("MySQLServer __init__ called")

    def start(self):
        """ Method to start the MySQL server. """
        logging.info("MySQLServer start called")

    def stop(self):
        """ Method to stop the MySQL server. """
        logging.info("MySQLServer stop called")


    def initialize(self):
        """ Method to initialize the MySQL server. """
        subprocess.run(["mysqld", "--initialize-insecure", "--user=mysql"]
                       , stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        for line in out.splitlines():
            logging.info(line)

