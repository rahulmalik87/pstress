""" This module contains the factory classes for creating server and load generator instances. """
from servers.mysql_server import MySQLServer
from servers.duckdb_server import DuckDBServer
from load_generators.sysbench_load_generator import SysbenchLoadGenerator
from load_generators.tpch_load_generator import TPCHLoadGenerator


def create_server_instance(server_type):
    """ This method creates a server instance based on the server type. """
    if server_type.lower() == "mysql":
        return MySQLServer()
    if  server_type.lower() == "duckdb":
        return DuckDBServer()
    raise ValueError("Unsupported server type")


def create_load_generator_instance(load_generator_type):
    """ This method creates a load generator instance based on the load generator type. """
    if load_generator_type.lower() == "sysbench":
        return SysbenchLoadGenerator()
    if load_generator_type.lower() == "tpch":
        return TPCHLoadGenerator()
    raise ValueError("Unsupported load generator type")
