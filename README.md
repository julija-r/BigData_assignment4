The dataset can be accessed here https://web.ais.dk/aisdata/

**A brief report or set of comments within the code that discusses the findings and any interesting insights about the data or the computation process.**

General notes and insights:

1. PySpark commands are easy to implement and are quickly executed which makes it a great tool for working with Big Data.
2. Setting up spark in windows environment might be complicated for a less experienced user.
3. current pyspark version is not compatible with the newest java version, in order to make things work you have to:
  - either downgrade your java
  - or modify parameters with .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") when building your spark session (applied this solution)
then things work fine.
4. However, once everything is set up properly, pyspark is an easy-to-use and flexible tool. 
