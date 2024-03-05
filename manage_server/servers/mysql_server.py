""" MySQLSever class is used to start and stop the MySQL server. """
import logging
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
