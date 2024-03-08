""" Initialize and configure a logger. """
import logging
import os


def setup_logger(name, args):
    """
    Set up and configure a logger.

    Args:
        name (str): Name of the logger.
        args (argparse.Namespace): Command line arguments.

    Returns:
        logging.Logger: Configured logger object.
    """
    log_file = os.path.join(args.workdir, name + ".log")
    os.makedirs(args.workdir, exist_ok=True)
    # do logging on console and in file
    logging.basicConfig(level=args.log_level,
                        format="%(asctime)s [%(levelname)s] %(message)s",
                        handlers=[logging.FileHandler(log_file), logging.StreamHandler()])
