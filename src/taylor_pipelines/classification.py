import taylor_pipelines.argument as argument
from taylor_pipelines.process import FunctionFilter, FunctionMap, JSONLSink
from taylor_pipelines.pipeline import Pipeline
from taylor_pipelines.source import S3, ParquetParser

class Classifier:
    pass



from .process import Map

class TrainClassifier(Map):
    def __init__(self):
        pass
    
def embed_text():
    pass

propensity_trainer = TrainClassifier(
    "propensity_to_buy",
    input_field="text_embedding",
    label_field="label",
    labels=["strong no", "weak no", "weak yes", "strong yes"],
)

propensity_classifier = Classifier(
    "propensity_to_buy",
    input_field="text_embedding",
    label_field="label"
)




trainer_pipeline = Pipeline(
    source=S3(),
    transforms=[
        embed_text,
        propensity_trainer,
    ]
)

inference_pipeline = Pipeline(
    source=S3(),
    transforms=[
        embed_text,
        propensity_classifier,
    ]
)