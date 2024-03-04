from servers.mysql_server import MySQLServer
from servers.duckdb_server import DuckDBServer

class ServerController:
    def __init__(self):
        self.servers = []

    def start_server(self, server_type):
        """Start a server instance."""
        if server_type.lower() == "mysql":
            server = MySQLServer()
        elif server_type.lower() == "duckdb":
            server = DuckDBServer()
        else:
            raise ValueError("Unsupported server type")

        server.start()
        self.servers.append(server)
        return server

    def stop_server(self, server_index):
        """Stop a server instance."""
        server = self.servers.pop(server_index)
        server.stop()

    def stop_all_servers(self):
        """Stop all server instances."""
        for server in self.servers:
            server.stop()
        self.servers = []

    def get_num_servers(self):
        """Get the number of running server instances."""
        return len(self.servers)

