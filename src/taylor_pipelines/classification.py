import os
import numpy as np
from typing import Union, Optional, Literal
import concurrent.futures
from .process import Map
from functools import partial
from sklearn.linear_model import PassiveAggressiveClassifier, SGDClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split
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
        validation_split: float = 0.1,
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
        self.metrics = {
            "accuracy": [], 
            "per_class": [],
            "val_accuracy": [],
            "val_per_class": [],
        }
        self.epochs = epochs
        self.data = {"X_train": [], "y_train": [], "X_val": [], "y_val": []}
        self.batch_size = None

    def compile(self, **kwargs):
        self.compiled = True

    def print_metrics(self):
        print(f"=== Classification Metrics for {self.name} ===")

        # if multi-epoch, report validation metrics
        if self.epochs > 1:
            print("Validation accuracy for each epoch:")
            print([round(acc, 2) for acc in self.val_accuracy])
            print(f"Final validation accuracy: {self.val_accuracy[-1]:.3f}")
            
            for cls in self.label2idx:
                print(f"[Final Val Metrics for {cls}]")
                print(f"  ↳ Precision: {self.val_per_class[-1][cls]['precision']:.3f}")
                print(f"  ↳ Recall: {self.val_per_class[-1][cls]['recall']:.3f}")
                print(f"  ↳ F1: {self.val_per_class[-1][cls]['f1-score']:.3f}")

        # otherwise, report metrics on last batch
        else:
            print(f"Accuracy on Last Batch: {self.metrics['accuracy'][-1]:.3f}")
            for cls in self.label2idx:
                print(f"[Metrics for {cls}]")
                print(f"  ↳ Precision: {self.metrics['per_class'][-1][cls]['precision']:.3f}")
                print(f"  ↳ Recall: {self.metrics['per_class'][-1][cls]['recall']:.3f}")
                print(f"  ↳ F1: {self.metrics['per_class'][-1][cls]['f1-score']:.3f}")

    def evaluate(self, X, y):
        y_pred = self.model.predict(X)
        accuracy = sum(y_pred == y) / len(y)
        report = classification_report(y, y_pred, output_dict=True, zero_division=np.nan)
        per_class = {label: report[str(self.label2idx[label])] for label in self.label2idx}
        return accuracy, per_class
    
    def save_model(self):
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

    async def map(self, batch: list[dict], executor: concurrent.futures.Executor):
        if self.batch_size is None:
            self.batch_size = len(batch)
        X = [entry[self.input_field] for entry in batch]
        y = [self.label2idx[entry[self.label_field]] for entry in batch]
        
        # train test split for the batch (only if >1 epoch)
        if self.epochs > 1:
            X, X_val, y, y_val = train_test_split(X, y, test_size=self.validation_split)
            # save validation data for later
            self.data["X_val"].extend(X_val) 
            self.data["y_val"].extend(y_val)

            # add training data to the data dict
            self.data["X_train"].extend(X)
            self.data["y_train"].extend(y)

        # metrics on train data
        accuracy, per_class = self.evaluate(X, y)
        self.metrics["per_class"].append(per_class)
        self.metrics["accuracy"].append(accuracy)

        # fit the model
        self.model.partial_fit(X, y, classes=range(len(self.label2idx)))
        self.iters += 1
        
        # save the model
        self.save_model()
        return batch
    
    def complete_remaining_epochs(self):
        if self.epochs == 1:
            return
        
        # calculate initial validation metrics
        X_val = self.data["X_val"]
        y_val = self.data["y_val"]
        accuracy, per_class = self.evaluate(X_val, y_val)
        self.metrics["val_per_class"].append(per_class)
        self.metrics["val_accuracy"].append(accuracy)

        print(f"=== Training {self.name} for {self.epochs - 1} more epochs ===")
        for _ in range(self.epochs - 1):
            # shuffle the data
            idxs = np.arange(len(self.data["X_train"]))
            np.random.shuffle(idxs)
            X = [self.data["X_train"][i] for i in idxs]
            y = [self.data["y_train"][i] for i in idxs]
            
            # fit the model
            for i in range(0, len(X), self.batch_size):
                X_batch, y_batch = X[i:i+self.batch_size], y[i:i+self.batch_size]
                self.model.partial_fit(X_batch, y_batch, classes=range(len(self.label2idx)))
                self.iters += 1

            # calculate validation metrics
            accuracy, per_class = self.evaluate(X_val, y_val)
            self.metrics["val_per_class"].append(per_class)
            self.metrics["val_accuracy"].append(accuracy)

            # save the model
            self.save_model()

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