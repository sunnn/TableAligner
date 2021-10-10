# This utility provides adding columns between source and target dataframes in spark dynamically
1.In Source if there are less columns while writing it to target dataframe then using this utility we can the extra columns
  from the config file provided
For ex : 

Dataframe 1 
+---+-----+-----+----+
|id|first|last |year|
+---+-----+-----+----+
|1  |John |Doe  |1986|
|2  |Ive  |Fish |1990|
|4  |John |Wayne|1995|
+---+-----+-----+----+

Dataframe 2
+---+-----+-----+----+----------+-------+
|sno|first|last |year|population|country|
+---+-----+-----+----+----------+-------+
|1  |John |Doe  |1986|2377      |UK     |
|2  |Ive  |Fish |1990|2376      |CEH     |
|4  |John |Wayne|1995|2378      |SDS     |
+---+-----+-----+----+----------+-------+

Dataframe 3:
+---+-----+-----+----+----------+-------+
|id	|first|last |year|population|country|
+---+-----+-----+----+----------+-------+
|1  |John |Doe  |1986|2377      |UK     |
|2  |Ive  |Fish |1990|2377      |UK     |
|4  |John |Wayne|1995|2377      |UK     |
+---+-----+-----+----+----------+-------+

Rename columns for existing source Datframe
Datframe  4: 
+---+-----+-----+----+----------+-------+
|sno|first|last |year|population|country|
+---+-----+-----+----+----------+-------+
|1  |John |Doe  |1986|2377      |UK     |
|2  |Ive  |Fish |1990|2377      |UK     |
|4  |John |Wayne|1995|2377      |UK     |
+---+-----+-----+----+----------+-------+
