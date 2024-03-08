""" This script is used to run the load test. It takes the following arguments:
    --load_generator: The type of load generator to use
    --server: The type of server to use
    --time: The time to run the load test
    """

import logging
from module.common_args import CommonArgs
from module.logger import setup_logger
from module.load_generator_controller import create_load_generator_instance
from module.server_controller import ServerController


class RunLoadTest(CommonArgs):
    """ This class is used to run the load test """
    def __init__(self):
        """ Initialize the parser and add the arguments """
        super().__init__()  # Initialize the parser from the parent class
        run_load_group = self.add_argument_group("RunLoadTest")
        run_load_group.add_argument("--time", default=60, type=int,
                                    help="Time to run the load")
        self.args = None
        self.parse_args()
        setup_logger("run_load.py", self.args)

    def parse_args(self):
        """ Parse the arguments """
        self.args = super().parse_args()
        return self.args

    def run_load(self):
        """ This method is used to run the load test """
        controller = ServerController(self.args)
        instance = server.start_server(self.args.server)


if __name__ == "__main__":
    test = RunLoadTest()
    logging.info("Starting run")
    test.run_load()
