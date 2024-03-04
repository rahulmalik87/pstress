from utils.common_args import CommonArgs
from servers.mysql_server import MySQLServer
from servers.duckdb_server import DuckDBServer
from load_generators.sysbench_load_generator import SysbenchLoadGenerator
from load_generators.tpch_load_generator import TPCHLoadGenerator


class RunLoadTest(CommonArgs):
    def run_load(self, load_generator_type, server_type):
        if server_type.lower() == "mysql":
            server = MySQLServer()
        elif server_type.lower() == "duckdb":
            server = DuckDBServer()
        else:
            raise ValueError("Unsupported server type")

        if load_generator_type.lower() == "sysbench":
            load_generator = SysbenchLoadGenerator()
        elif load_generator_type.lower() == "tpch":
            load_generator = TPCHLoadGenerator()
        else:
            raise ValueError("Unsupported load generator type")

        # Start the server
        server.start()

        # Start load generation
        load_generator.start_load()

        # Stop load generation and server
        load_generator.stop_load()
        server.stop()


if __name__ == "__main__":
    args = RunLoadTest().parse_args()
    test = RunLoadTest()
    test.run_load(args.load_generator, args.server)
