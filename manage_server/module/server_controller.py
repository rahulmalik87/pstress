""" Controller module for managing server instances. """
from .mysql_server import MySQLServer

class ServerController:
    """Controller class for managing server instances."""
    def __init__(self, args ):
        """Initialize the server controller."""
        self.servers = []
        self.args = args

    def start_server(self,server_type : str):
        """Start a server instance."""
        if server_type.lower() == "mysql":
            server = MySQLServer(self.args)
            server.initialize()
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
