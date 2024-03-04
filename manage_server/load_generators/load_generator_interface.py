from abc import ABC, abstractmethod

class LoadGeneratorInterface(ABC):
    @abstractmethod
    def start_load(self):
        """Start the load generation."""
        pass

    @abstractmethod
    def stop_load(self):
        """Stop the load generation."""
        pass

    @abstractmethod
    def get_status(self):
        """Get the status of the load generation."""
        pass

