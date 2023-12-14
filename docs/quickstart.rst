Quick Start
=================

Setup dataset
-------------------
Once you have installed Taylor Pipelines, you can start using it by following the steps below:

1. Import argument

.. code:: python 

   import taylor_pipelines.argument as argument

2. Import FunctionFilter, FunctionMap, and JSONLSink

.. code:: python 

   from taylor_pipelines.process import FunctionFilter, FunctionMap, JSONLSink

3. Import pipeline

.. code:: python 

   from taylor_pipelines.pipeline import Pipeline

3. Import source

.. code:: python 

   from taylor_pipelines.source import S3, JSONLParser



4. Load a Dataset

.. note::
      See :doc:`GalacticDataset.loaders <loaders>` for more options on loading datasets (e.g. parquet, HuggingFace, jsonl, pandas, etc.)

.. code:: python 

   # Load from a csv file
   ds = GalacticDataset.from_csv("path/to/dataset")

5. Verify the Dataset

.. code:: python

   # Confirm the dataset loaded
   print(ds)

   # Verify the first element of the dataset
   first_element = ds[0]
   print(first_element)

   # Verify the column names and length of the dataset
   print(ds.column_names, len(ds))
