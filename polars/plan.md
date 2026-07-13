# Polars Overview

# Goals
- Have a polars demo to showcase where polars strengths and weaknesses are for learning for myself and for customers





# Architecture
- A workspace where the data used is.  
- This should contain data of 3 different sizes, 1 GB, 10 GB, 50 GB
- This should compare performance between pyspark and polars
- The comaprison should very much be similar to the benchmarking done by Miles Cole that can be described at these two pages:
 - https://milescole.dev/data-engineering/2024/12/12/Should-You-Ditch-Spark-DuckDB-Polars.html
 - https://milescole.dev/data-engineering/2025/06/30/Spark-v-DuckDb-v-Polars-v-Daft-Revisited.html
- In additiona to the data workspace described above (which should be named polars-benchmark-data-ws) all the artifacts to create the code and perform the benchmarking should
be stored in a workspace named: polars-benchmark-engineering-ws)
- The notebooks between polars and pyspark should be different and different environments between them 
- All notebooks should time the amount of time it takes (using timeit) to list how long it takes
- There should be a README.md file that explains each artifact created and its purpose
- The environments created should be configurable and adjusted/tuned to allow the resource it's supporting to achieve maximum results
- The environments will be separated by technology and layer.  What I mean by this for example is: [polars-bronze, polars-silver,polars-gold, pyspark-bronze, pyspark-silver, pyspark-gold]
- There should also be an evalution if the additional separation of layer (medallion layer) is worth the effort or if just separation by technology is sufficient

