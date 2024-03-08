""" This module contains the factory classes for creating server and load generator instances. """

from .sysbench_load_generator import SysbenchLoadGenerator
def create_load_generator_instance(load_generator_type):
    """ This method creates a load generator instance based on the load generator type. """
    if load_generator_type.lower() == "sysbench":
        return SysbenchLoadGenerator()
    raise ValueError("Unsupported load generator type")
