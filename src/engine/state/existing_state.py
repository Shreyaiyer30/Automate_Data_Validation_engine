# Legacy state management
class PipelineState:
    def __init__(self, df: None):
        self.df = df
        self.history = []

    def update(self, df):
        self.df = df
        self.history.append(datetime.now())
