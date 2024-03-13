""" Common arguments for all the scripts """
import argparse
import os
from .sysbench_load_generator import sysbench_init_args


class CommonArgs(argparse.ArgumentParser):
    """ Common arguments for all the scripts """

    def __init__(self):
        """ Common arguments for all the scripts """
        super().__init__()
        group = self.add_argument_group("Common arguments")
        group.add_argument("-l", "--load-generator", choices=["sysbench", "pstress"],
                           default="sysbench",
                           help="Type of load generator (e.g., sysbench or tpch)")

        group.add_argument("-s", "--server", choices=["mysql", "duckdb"], default="mysql",
                           help="Type of server (e.g., mysql or duckdb)")
        group.add_argument("--workdir", default = os.path.join(os.getcwd(),
                                                               "workdir"), help="Working directory")

        group.add_argument("-b", "--basedir",
                           help="path of binares. Multiple path are seprated by : similar to PATH")
        group.add_argument("--port-base", default=3306, type=int,
                           help="Base port number")
        group.add_argument("--log-level", default="INFO",
                           help="Log level (e.g., INFO, DEBUG, WARNING, ERROR, CRITICAL)",
                           choices=["INFO", "DEBUG", "WARNING", "ERROR", "CRITICAL"])
        sysbench_init_args(self)
