# sfmunidata

## Finding Transit Roadblocks (UnBlockMUNI)

Project on Data Engineering at Insight in San Francisco. Data pipeline to analyze transit GIS data, called UnBlockMUNI.

[Raw San Francisco Municipal Transportation Agency (SFMTA) data](https://data.sfgov.org/Transportation/Historical-raw-AVL-GPS-data/5fk7-ivit)

_Tech Stack_
* S3 to store raw data
* Spark to clean and analyze historical data
* PostgreSQL to store cleaned data, query current data, and query data processed by Spark
* Flask and SQLAlchemy to provide user interface

[Video of presentation describing and demonstrating the UnBlockMUNI project](https://www.youtube.com/watch?v=cIgb6K9h38Y)

[Video of demo of UnBlockMUNI only](https://www.youtube.com/watch?v=Iyf0YJESohs)
