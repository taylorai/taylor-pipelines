import os
import numpy as np
from typing import Union, Optional, Literal
import concurrent.futures
from .process import Map
from functools import partial
from sklearn.linear_model import PassiveAggressiveClassifier, SGDClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.metrics import classification_report
from collections import Counter
import joblib

NAME_TO_MODEL = {
    "passive_aggressive": partial(PassiveAggressiveClassifier, average=True),
    "logistic": partial(SGDClassifier, loss="log"),
    "mlp": partial(MLPClassifier, solver="adam", hidden_layer_sizes=(768, 768)),
}

class TrainClassifier(Map):
    def __init__(
        self,
        name: str,
        input_field: str, # should be embeddings or list of floats
        label_field: str, # should be string or int
        labels: list[Union[str, int]],
        model: Literal["passive_aggressive", "logistic_regression", "mlp"] = "passive_aggressive",
        output_directory: Optional[str] = None, # will use name if not provided
        optional: bool = False,
    ):
        super().__init__(
            name, 
            description=f"Train a classifier to predict {label_field} from {input_field}.",
            optional=optional
        )
        self.output_directory = output_directory
        self.label2idx = {label: i for i, label in enumerate(labels)}
        self.idx2label = {i: label for i, label in enumerate(labels)}
        self.input_field = input_field
        self.label_field = label_field
        self.model = NAME_TO_MODEL[model]()
        self.iters = 0
        self.metrics = {"accuracy": [], "per_class": []}

    def compile(self, **kwargs):
        self.compiled = True

    def print_metrics(self):
        print(f"=== Classification Metrics for {self.name} ===")
        print(f"Accuracy on Last (Full) Batch: {self.metrics['accuracy'][-2]:.3f}")
        for cls in self.label2idx:
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
        
        output_file = f"{self.output_directory}/{self.name}_model.joblib"
        os.makedirs(self.output_directory, exist_ok=True)
        # each iteration will overwrite the previous model
        joblib.dump({
            "model": self.model, 
            "idx2label": self.idx2label,
        }, f"{output_file}")

        return batch      

class Classifier(Map):
    def __init__(
        self,
        name: str,
        input_field: str, # should be embeddings or list of floats
        label_field: str, # should be string or int
        model_path: str,
        optional: bool = False,
    ):
        super().__init__(
            name, 
            description=f"Use a trained classifier to predict {label_field} from {input_field}.",
            optional=optional
        )
        self.input_field = input_field
        self.label_field = label_field
        self.model = joblib.load(model_path)
    
    def compile(self, **kwargs):
        self.compiled = True

    async def map(self, batch: list[dict], executor: concurrent.futures.Executor):
        X = [entry[self.input_field] for entry in batch]
        y_pred = self.model["model"].predict(X)
        for entry, label in zip(batch, y_pred):
            entry[self.label_field] = self.model["idx2label"][label]
        return batch