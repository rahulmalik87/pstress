""" This module defines the interface for the load generator. """
from abc import ABC, abstractmethod

class LoadGeneratorInterface(ABC):
    """ Interface for the load generator. """
    @abstractmethod
    def start_load(self):
        """Start the load generation."""

    @abstractmethod
    def stop_load(self):
        """Stop the load generation."""

    @abstractmethod
    def get_status(self):
        """Get the status of the load generation."""
