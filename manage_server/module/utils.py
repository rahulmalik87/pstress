"""Utility functions for the package."""
import logging
import os
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
    for inpath  in path:
        if os.path.exists(os.path.join(inpath, executable)):
            logging.debug("Found executable %s in %s", executable, inpath)
            return os.path.join(inpath, executable)
    raise FileNotFoundError(f"Could not find {executable} in {path}")
