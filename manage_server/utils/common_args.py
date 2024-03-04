import argparse

class CommonArgs(argparse.ArgumentParser):
    def __init__(self):
        super().__init__()
        self.add_argument("--load-generator", required=True, choices=["sysbench", "tpch"],
                          help="Type of load generator (e.g., sysbench or tpch)")
        self.add_argument("--server", required=True, choices=["mysql", "duckdb"],
                          help="Type of server (e.g., mysql or duckdb)")

