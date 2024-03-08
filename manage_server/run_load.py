""" This script is used to run the load test. It takes the following arguments:
    --load_generator: The type of load generator to use
    --server: The type of server to use
    --time: The time to run the load test
    """

import logging
from utils.common_args import CommonArgs
from utils.logger import setup_logger
from utils.factory import create_load_generator_instance
from servers.controller import ServerController


class RunLoadTest(CommonArgs):
    """ This class is used to run the load test """
    def __init__(self):
        """ Initialize the parser and add the arguments """
        super().__init__()  # Initialize the parser from the parent class
        run_load_group = self.add_argument_group("RunLoadTest")
        run_load_group.add_argument("--time", default=60, type=int,
                                    help="Time to run the load")

    def run_load(self, load_generator_type, server_type):
        """ This method is used to run the load test """
        server = create_server_instance(server_type)
        load_generator = create_load_generator_instance(load_generator_type)
        # Start the server
        server.start()

        # Start load generation
        load_generator.start_load()

        # Stop load generation and server
        load_generator.stop_load()
        server.stop()


if __name__ == "__main__":
    test = RunLoadTest()
    args = test.parse_args()
    setup_logger("run_load.py", args)
    logging.info("Starting run")
    test.run_load(args.load_generator, args.server)
