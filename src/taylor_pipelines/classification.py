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
from .embeddings import ONNXEmbeddingModel
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
        weights: Optional[list[float]] = None,
        balanced: bool = False,
        epochs: int = 1,
        model: Literal["passive_aggressive", "logistic_regression", "mlp"] = "passive_aggressive",
        output_directory: Optional[str] = None, # will use name if not provided
        optional: bool = False,
    ):
        super().__init__(
            name, 
            description=f"Train a classifier to predict {label_field} from {input_field}.",
            optional=optional
        )
        if model == "mlp" and (balanced or weights is not None):
            raise ValueError("MLP does not support class weights.")
        self.output_directory = output_directory
        self.label2idx = {label: i for i, label in enumerate(labels)}
        self.idx2label = {i: label for i, label in enumerate(labels)}
        self.input_field = input_field
        self.label_field = label_field
        kwargs = {}
        if weights is not None:
            kwargs["class_weight"] = {i:weight for i, weight in enumerate(weights)}
        elif balanced:
            kwargs["class_weight"] = "balanced"
        self.model = NAME_TO_MODEL[model](**kwargs)
        self.iters = 0
        self.metrics = {"accuracy": [], "per_class": []}
        self.epochs = epochs
        self.data = {"X": [], "y": []}
        self.batch_size = None

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
        if self.batch_size is None:
            self.batch_size = len(batch)
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

        # save data if doing >1 epoch
        if self.epochs > 1:
            self.data["X"].extend(X)
            self.data["y"].extend(y)
        
        output_file = f"{self.output_directory}/{self.name}_model.joblib"
        # check if output director exists (it's NFS so if we don't check, os.makedirs will fail)
        if not os.path.exists(self.output_directory):
            os.makedirs(self.output_directory)
        # each iteration will overwrite the previous model
        joblib.dump({
            "model": self.model, 
            "idx2label": self.idx2label,
        }, f"{output_file}")

        return batch
    
    def complete_remaining_epochs(self):
        print(f"=== Training {self.name} for {self.epochs - 1} more epochs ===")
        # shape check
        print("X shape: ", np.array(self.data["X"]).shape)
        print("y shape: ", np.array(self.data["y"]).shape)
        if self.epochs == 1:
            return
        for ep in range(self.epochs - 1):
            # shuffle the data
            idxs = np.arange(len(self.data["X"]))
            np.random.shuffle(idxs)
            X = [self.data["X"][i] for i in idxs]
            y = [self.data["y"][i] for i in idxs]
            # another shape check
            print("X shape: ", np.array(X).shape)
            print("y shape: ", np.array(y).shape)
            # fit the model
            for i in range(0, len(X), self.batch_size):
                X_batch, y_batch = X[i:i+self.batch_size], y[i:i+self.batch_size]
                # yet another shape check
                print("X_batch shape: ", np.array(X_batch).shape)
                print("y_batch shape: ", np.array(y_batch).shape)
                try:
                    y_pred = self.model.predict(X_batch)
                    accuracy = sum(y_pred == y_batch) / len(y_batch)
                    report = classification_report(y_batch, y_pred, output_dict=True, zero_division=np.nan)
                    self.metrics["per_class"].append({
                        label: report[str(self.label2idx[label])] for label in self.label2idx
                    })
                    self.metrics["accuracy"].append(accuracy)
                except Exception as e:
                    print("Error:", e)
                    pass
                self.model.partial_fit(X_batch, y_batch, classes=range(len(self.label2idx)))
                self.iters += 1
        # save the model
        output_file = f"{self.output_directory}/{self.name}_model.joblib"
        # check if output director exists (it's NFS so if we don't check, os.makedirs will fail)
        if not os.path.exists(self.output_directory):
            os.makedirs(self.output_directory)
        # each iteration will overwrite the previous model
        joblib.dump({
            "model": self.model, 
            "idx2label": self.idx2label,
        }, f"{output_file}")


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
    

class ClassifierWithEmbeddings(Map):
    def __init__(
        self,
        name: str,
        input_field: str, # should be embeddings or list of floats
        label_field: str, # should be string or int
        classifier_model_path: str,
        local_onnx_path: str,
        huggingface_repo: str = None,  # used for tokenizer, and model not found at local_onnx_path
        huggingface_path_in_repo: Optional[str] = None,  # used if model not found at local_onnx_path
        max_length=512,
        normalize=True,
        optional: bool = False,
    ):
        super().__init__(
            name, 
            description=f"Embed texts & inference a trained classifier to predict {label_field} from {input_field}.",
            optional=optional
        )
        self.input_field = input_field
        self.label_field = label_field
        self.classifier_model_path = classifier_model_path
        self.local_onnx_path = local_onnx_path
        self.huggingface_repo = huggingface_repo
        self.huggingface_path_in_repo = huggingface_path_in_repo
        self.max_length = max_length
        self.normalize = normalize
        self.embeddings = None
        self.classifier = None
        
    
    def compile(self, **kwargs):
        self.embeddings = ONNXEmbeddingModel(
            local_onnx_path=self.local_onnx_path,
            huggingface_repo=self.huggingface_repo,
            huggingface_path_in_repo=self.huggingface_path_in_repo,
            max_length=self.max_length,
        )
        self.classifier = joblib.load(self.classifier_model_path)
        self.compiled = True

    def infer_one(self, text: str):
        embedding = self.embeddings.embed(text, normalize=self.normalize)
        prediction = self.classifier["model"].predict([embedding])[0]
        return self.classifier["idx2label"][prediction]
    
    async def map(self, batch: list[dict], executor: concurrent.futures.Executor):
        X = [entry[self.input_field] for entry in batch]
        X_emb = self.embeddings.embed_batch(X, normalize=self.normalize)
        y_pred = self.classifier["model"].predict(X_emb)
        for entry, label in zip(batch, y_pred):
            entry[self.label_field] = self.classifier["idx2label"][label]
        return batch