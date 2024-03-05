""" Common arguments for all the scripts """
import argparse
from load_generators import sysbench_load_generator

class CommonArgs(argparse.ArgumentParser):
    """ Common arguments for all the scripts """

    def __init__(self):
        """ Common arguments for all the scripts """
        super().__init__()
        group = self.add_argument_group("Common Arguments")
        group.add_argument("-l", "--load-generator", required=True, choices=["sysbench", "tpch"],
                           help="Type of load generator (e.g., sysbench or tpch)")

        group.add_argument("-s", "--server", required=True, choices=["mysql", "duckdb"],
                           help="Type of server (e.g., mysql or duckdb)")

        group.add_argument("--workdir", default="./workdir",
                           help="Working directory")
        group.add_argument("--log-level", default="INFO",
                           help="Log level (e.g., INFO, DEBUG, WARNING, ERROR, CRITICAL)",
                           choices=["INFO", "DEBUG", "WARNING", "ERROR", "CRITICAL"])
        sysbench_load_generator.init_args(self)
