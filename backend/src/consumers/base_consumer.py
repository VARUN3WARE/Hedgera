"""Base Consumer Class"""

class BaseConsumer:
    def __init__(self):
        pass
    
    def consume(self):
        """Consume data"""
        raise NotImplementedError
