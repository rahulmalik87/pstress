""" This script is used to run the load test. It takes the following arguments:
    --load_generator: The type of load generator to use
    --server: The type of server to use
    --time: The time to run the load test
    """

import logging
from module.common_args import CommonArgs
from module.load_generator_controller import create_load_generator_instance
from module.server_controller import ServerController
from module.utils import initial_setup


def argument_for_test():
    """ This function is used to parse the arguments """
    parser = CommonArgs()
    run_load_group = parser.add_argument_group("RunLoadTest")
    run_load_group.add_argument("--time", default=60, type=int,
                                help="Time to run the load")
    return parser.parse_args()

if __name__ == "__main__":
    args = argument_for_test()
    initial_setup(args, "RunLoadTest")
    controller = ServerController(args)
    instance = controller.start_server(args.server)
    instance.run_query("CREATE DATABASE test")
    
   
    instance.stop()
