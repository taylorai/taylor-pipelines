# README

`taylor-pipelines` is a library for building configurable data pipelines, and is designed especially for streaming unstructured and semi-structured data from Amazon S3. It is self-contained, but is designed to integrate with our cloud runtime (hosted at https://www.trytaylor.ai), which allows you to host and run your data pipelines in the cloud. When you push a pipeline to the cloud, we'll automatically create a user interface for configuration and execution. This allows you to build data pipelines that can be run by anyone on your team, even if they can't or don't want to write code.

## Getting Started
`taylor-pipelines` can be installed from `pip`:
```
pip install taylor-pipelines
```

If you want the bleeding-edge version (may be unstable!), you can install straight from Github:
```
pip install taylor-pipelines@git+https://github.com/taylorai/taylor-pipelines.git
```

## Pipeline Building Blocks
A data pipeline in Taylor Pipelines is defined by a sequence of transforms: maps, filters, and sinks. A map modifies each element of a stream, a filter filters out elements of a stream, and a sink writes the stream to a destination. We provide basic wrappers that allow you to turn any Python function into a map or filter (by wrapping it in a `FunctionMap` or `FunctionFilter`), and we provide a built-in JSONL sink that writes to a JSONL file. 

Here is an example usage for `FunctionFilter`:
```python
from taylor_pipelines.process import FunctionFilter

def my_filter_func(x: dict) -> bool:
    return x["color"] == "blue"

blue_filter = FunctionFilter(
    "blue_filter", 
    my_filter_func,
    description="Filter out all elements that are not blue."
)
```

Here is an example usage for `FunctionMap`:
```python
def my_map_func(x: dict) -> dict:
    x["color"] = "red"
    return x

red_map = FunctionMap(
    "red_map", 
    my_map_func,
    description="Change the color of all elements to red."
)
```

And here is an example usage for `JSONLSink`:
```python
from taylor_pipelines.process import JSONLSink

sink = JSONLSink(
    "my_sink",
    "output-file.jsonl",
    description="Write the stream to a JSONL file."
)
```

So far, this just looks like more overhead on top of native Python functions. The magic comes from making a pipeline that's *configurable*--using `Argument`s, you can make your functions configurable by the user. For example, we can make the `my_filter_func` function above configurable by adding an `Argument`:
```python
from taylor_pipelines.argument import MultipleChoiceArgument

def my_filter_func(x: dict, color: str) -> bool:
    return x["color"] == color

color_filter = FunctionFilter(
    "color_filter", 
    my_filter_func,
    description="Filter out all elements that are not the selected color.",
    arguments=[
        MultipleChoiceArgument(
            "color",
            description="The color to filter by",
            choices=["red", "green", "blue"]B
        )
    ]
)
```

This combination is already very powerful, but the advanced users can easily build custom `Map`s, `Filter`s, and `Sink`s to do more complex things (see the examples folder for inspiration!).

## Your First Pipeline
Let's see how to put these pieces together to make a pipeline.
```python
from taylor_pipelines.pipeline import Pipeline
from taylor_pipelines.source import S3, ParquetParser
from taylor_pipelines.process import FunctionFilter, FunctionMap, JSONLSink

my_s3_bucket = S3(
    bucket="my-bucket",
    prefix="example_folder/file-",
    # you should generally get these from environment variables
    access_key_id="xxx-xxx-xxx-xxx",
    secret_access_key="xxx-xxx-xxx-xxx",
    parser=ParquetParser(),

)



1. Log into [Taylor AI](www.trytaylor.ai) with your Google or GitHub account. 
2. Navigate to [Data Sources](/data_sources) and click to "Add Data Source". Add your Data Source. Please ensure you are granting only the necessary permissions. Write access is not advised. 
3. Navigate to [Data Pipelines](/data-pipelines) and click to "Create a pipeline". Give your pipeline a name and click "Create". 
4. Open your preferred text editor. 
5. Install the Taylor Pipelines package. 



## Write a Script

Write a Python script for your data. Note: You do not need to specify your folder structure. This is handled on the frontend in the `Prefix` field. 

1. Import the necessary functions.
```
import taylor_pipelines.argument as argument
from taylor_pipelines.process import FunctionFilter, FunctionMap, JSONLSink
from taylor_pipelines.pipeline import Pipeline
from taylor_pipelines.source import S3, JSONLParser 
```

2. If you'd like a function to be displayed as a user input on the frontend (e.g. a toggle, select, or text input), wrap your function in a `FunctionFilter`

        FunctionFilter(name: str, description: str = "", arguments: list[Argument] = [], optional: bool = False)
        
- `FunctionFilter(function_name, function, description)`: Displays a toggle, select, or text input depending on type

3. `FunctionMap`: Displays a toggle, select, or text input depending on type
4. `JSONLSink`: Displays a toggle, select, or text input depending on type

For example: 
```
def is_valid_conversation(entry: dict) -> bool:
    try:
        if entry["messages"][0]["role"] != "system":
            return False
        if entry["messages"][-1]["role"] != "user":
            return False
    except Exception as e:
        return False
    return True

is_valid_conversation_filter = FunctionFilter(
    "toggle_for_valid_conversation", 
    is_valid_conversation, 
    description="Toggle on if you want to filter the dataset by whether the conversations are valid (e.g. the user is the last message and the system is the first message)." 
)
```

3. Define a `pipeline` object. 
```
pipeline = Pipeline(
    source=None,
    parser=JSONLParser(),
    transforms=[
        is_valid_conversation_filter
    ],
    batch_size=25
)
```
4. Once your script is ready, add your script file to the pipeline.
5. Click "New Data Pull" and select this pipeline and bucket. You will now see the toggles to pull the data. 

## Share

You may share pipelines with your team. 
1. Create a team via your "Profile" page. 
2. Invite team members by email address. 
3. Go to your pipelines and click "Share" for each pipeline you'd like to share with your team. 
4. Go to your data sources and click "Share" for each data source you'd like to share. 
5. If a pipeline is shared with your team, all team members can see the data pulls made with that shared pipeline. 

## Contributing

Contributions are welcome! If you would like to contribute, please feel free to contribute to [Taylor Pipelines](https://github.com/taylorai/taylor-pipelines).

## Contact

If you have any questions or suggestions regarding the Taylor Site project, please [contact us](contact@trytaylor.ai).
