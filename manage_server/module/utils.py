"""Utility functions for the package."""
import logging
import os
import shutil
import subprocess
from typing import List

def find_executable(executable, path=None):
    """Tries to find 'executable' in the directories listed in 'path' (a
    string listing directories separated by 'os.pathsep'; defaults to
    os.environ['PATH']).  Returns the complete filename or None if not
    found.
    """
    if path is None:
        path = os.environ.get("PATH", os.defpath)
    if not isinstance(path, (list, tuple)):
        path = path.split(os.pathsep)
    for inpath in path:
        if os.path.exists(os.path.join(inpath, executable)):
            logging.debug("Found %s in %s", executable, inpath)
            return os.path.join(inpath, executable)
    raise FileNotFoundError(f"Could not find {executable} in {path}")


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


def initial_setup(args, test_name: str):
    """
    Perform initial setup of the package.

    Args:
        args (argparse.Namespace): Command line arguments.
    """
    # find the executable
    setup_logger(test_name, args)
    if args.basedir is None:
        args.basedir = os.environ.get("BASEDIR")
    if args.basedir is None:
        raise ValueError("Basedir not set")
    __clean_vardir(os.path.join(args.workdir, "var"))


def __clean_vardir(directory: str):
    """
    Clean the directory.

    Args:
        directory (str): Directory to clean.
    """
    if os.path.exists(directory):
        shutil.rmtree(directory)
    os.makedirs(directory, exist_ok=True)


def check_call(*args):
    """
    Run a command with arguments.

    Args:
        args (list): List of command line arguments.
    """
    logging.debug(" ".join(*args))
    subprocess.check_call(*args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def run_in_background(args : List[str]):
    """
    Run a command in the background.

    Args:
        args (list): List of command line arguments.
    Returns:
        subprocess.Popen: Process object.
    """
    logging.debug(" ".join(args))
    process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    logging.debug("Started process %d", process.pid)
    return process
