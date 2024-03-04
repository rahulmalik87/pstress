from load_generators.load_generator_interface import LoadGeneratorInterface


class SysbenchLoadGenerator(LoadGeneratorInterface):
    def start_load(self):
        """Start the TPCH load generation."""
        # Implementation specific to starting TPCH load
        pass

    def stop_load(self):
        """Stop the TPCH load generation."""
        # Implementation specific to stopping TPCH load
        pass

    def get_status(self):
        """Get the status of the TPCH load generation."""
        # Implementation specific to getting TPCH load status
        pass
