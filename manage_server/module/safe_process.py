""" methods used for running subprocesses with logging """
import subprocess
import logging
from typing import List

def check_call(args : List[str], silent = False):
    """
    Run a command with arguments.
    Args:
        args (list): List of command line arguments.
        silent (bool): If True, do not log the command.
    """

    logging.debug(" ".join(args))
    try:
        subprocess.run(args,stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                text=True, check=True)
    except subprocess.CalledProcessError as error:
        if not silent:
            logging.error("Command failed with return code %d", error.returncode)
            logging.error("stdout: error %s", error.stdout)
            logging.error("stderr: error %s", error.stderr)
        raise error

def check_output(args: List[str], silent = False):
    """
    Run a command with arguments and return the output.
    Args:
        args (list): List of command line arguments.
        silent (bool): If True, do not log the command.
    Returns:
        str: Output of the command.
    """
    logging.debug(" ".join(args))
    try:
        return subprocess.check_output(args, text=True)
    except subprocess.CalledProcessError as error:
        if not silent:
            logging.error("Command failed with return code %d", error.returncode)
            logging.error("stdout: error %s", error.stdout)
            logging.error("stderr: error %s", error.stderr)
        raise error

def run_backgroud(args: List[str], silent = False):
    """
    Run a command with arguments.
    Args:
        args (list): List of command line arguments.
        silent (bool): If True, do not log the command.
    """
    logging.debug(" ".join(args))
    try:
        return subprocess.Popen(args,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as error:
        if not silent:
            logging.error("Command failed with return code %d", error.returncode)
            logging.error("stdout: error %s", error.stdout)
            logging.error("stderr: error %s", error.stderr)
        raise error
