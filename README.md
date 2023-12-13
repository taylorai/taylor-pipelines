# README

This README contains important information about setting up, deploying pipelines, and managing your team settings. 

## Getting Started

1. Log into [Taylor AI](www.trytaylor.ai) with your Google or GitHub account. 
2. Navigate to [Data Sources](/data_sources) and click to "Add Data Source". Add your Data Source. Please ensure you are granting only the necessary permissions. Write access is not advised. 
3. Navigate to [Data Pipelines](/data-pipelines) and click to "Create a pipeline". Give your pipeline a name and click "Create". 
4. Open your preferred text editor. 
5. Install the Taylor Pipelines package. 
```
pip install taylor_pipelines@git+https://github.com/taylorai/taylor-pipelines.git
```


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
