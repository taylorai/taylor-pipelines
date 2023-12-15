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
