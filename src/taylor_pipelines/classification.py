import os
from typing import Union, Optional
import concurrent.futures
from .process import Map
from sklearn.linear_model import PassiveAggressiveClassifier
import joblib


class TrainClassifier(Map):
    def __init__(
        self,
        name: str,
        input_field: str, # should be embeddings or list of floats
        label_field: str, # should be string or int
        labels: list[Union[str, int]],
        output_path: Optional[str] = None, # will use name if not provided
        optional: bool = False,
    ):
        super().__init__(
            name, 
            description=f"Train a classifier to predict {label_field} from {input_field}.",
            optional=optional
        )
        self.label2idx = {label: i for i, label in enumerate(labels)}
        self.idx2label = {i: label for i, label in enumerate(labels)}
        self.input_field = input_field
        self.label_field = label_field
        self.model = PassiveAggressiveClassifier()
        self.output_path = output_path or f"models/{name}"
        self.iters = 0
        self.metrics = {"accuracy": []}

        if not os.path.exists("models"):
            os.mkdir("models")

    def compile(self, **kwargs):
        self.compiled = True

    async def map(self, batch: list[dict], executor: concurrent.futures.Executor):
        X = [entry[self.input_field] for entry in batch]
        y = [self.label2idx[entry[self.label_field]] for entry in batch]
        try:
            y_pred = self.model.predict(X)
            accuracy = sum(y_pred == y) / len(y)
            self.metrics["accuracy"].append(accuracy)
        except:
            pass
        self.model.partial_fit(X, y, classes=range(len(self.label2idx)))
        self.iters += 1
        
        joblib.dump(self.model, f"{self.output_path}_{self.iters}.joblib")

        return batch
        

class Classifier:
    pass








# def embed_text():
#     pass


# propensity_trainer = TrainClassifier(
#     "propensity_to_buy",
#     input_field="text_embedding",
#     label_field="label",
#     labels=["strong no", "weak no", "weak yes", "strong yes"],
# )

# propensity_classifier = Classifier(
#     "propensity_to_buy", input_field="text_embedding", label_field="label"
# )


# trainer_pipeline = Pipeline(
#     source=S3(),
#     transforms=[
#         embed_text,
#         propensity_trainer,
#     ],
# )

# inference_pipeline = Pipeline(
#     source=S3(),
#     transforms=[
#         embed_text,
#         propensity_classifier,
#     ],
# )
