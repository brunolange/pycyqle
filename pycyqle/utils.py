class Processor:
    def __init__(self, closure):
        self._closure = closure
        self._models = []

    def attach(self, model):
        self._models.append(model)

    def run(self):
        for model in self._models:
            if isinstance(self._closure, str):
                method = getattr(model, self._closure)
                method()
            elif callable(self._closure):
                self._closure(model)
