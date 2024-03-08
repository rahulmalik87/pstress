"""Module to handle sysbench load generation."""
import logging
from .load_generator_interface import LoadGeneratorInterface


def sysbench_init_args(parser):
    """Initialize the arguments for the sysbench load generator."""
    group = parser.add_argument_group("Sysbench Load Generator")
    group.add_argument("--sysbench-conf", default="sysbench.conf",
                       help="Sysbench configuration file")


class SysbenchLoadGenerator(LoadGeneratorInterface):
    """Class to handle sysbench load generation."""

    def __init__(self):
        logging.info("Initializing sysbench load generator")

    def start_load(self):
        """Start the sybench load generation."""
        # Implementation specific to starting TPCH load
        # star the sysbench as a subprocess
        logging.info("Starting sysbench load generation")

    def stop_load(self):
        """Stop the sysbench load generation."""
        # Implementation specific to stopping TPCH load
        logging.info("Stop sysbench load generation")

    def get_status(self):
        """Get the status of the TPCH load generation."""
        # Implementation specific to getting TPCH load status
        logging.info("Getting sysbench load status")
