import os
import numpy as np
from typing import Union, Optional
import concurrent.futures
from .process import Map
from sklearn.linear_model import PassiveAggressiveClassifier
from sklearn.metrics import classification_report
from collections import Counter
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
        self.metrics = {"accuracy": [], "per_class": []}

        if not os.path.exists("models"):
            os.mkdir("models")

    def compile(self, **kwargs):
        self.compiled = True

    def print_metrics(self):
        print(f"=== Classification Metrics for {self.name} ===")
        print(f"Accuracy on Last (Full) Batch: {self.metrics['accuracy'][-2]:.3f}")
        for cls in self.idx2label:
            print(f"[Metrics for {cls}]")
            print(f"  ↳ Precision: {self.metrics['per_class'][-2][cls]['precision']:.3f}")
            print(f"  ↳ Recall: {self.metrics['per_class'][-2][cls]['recall']:.3f}")
            print(f"  ↳ F1: {self.metrics['per_class'][-2][cls]['f1-score']:.3f}")

    async def map(self, batch: list[dict], executor: concurrent.futures.Executor):
        X = [entry[self.input_field] for entry in batch]
        y = [self.label2idx[entry[self.label_field]] for entry in batch]
        try:
            y_pred = self.model.predict(X)
            accuracy = sum(y_pred == y) / len(y)
            report = classification_report(y, y_pred, output_dict=True, zero_division=np.nan)
            self.metrics["per_class"].append({
                label: report[str(self.label2idx[label])] for label in self.label2idx
            })
            self.metrics["accuracy"].append(accuracy)
        except:
            pass
        self.model.partial_fit(X, y, classes=range(len(self.label2idx)))
        self.iters += 1
        
        joblib.dump(self.model, f"models/{self.output_path}_{self.iters}.joblib")

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
