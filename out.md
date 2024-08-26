# Sparky
## Data ETL Tool in Rust


### Reading from ./sample.csv and inferring types

```text
Name [Text]         |ValueA [Float]      |ValueB [Float]      
--------------------------------------------------------------
jlksdjfk            |              639.00|                3.21
jfdkslj             |              111.10|                3.33
llllllll            |              121.20|                2.22
--------------------------------------------------------------
```


### Creating a dataframe with strings

```text
First Name [Text]   |Last Name [Float]   |Email [Float]       
--------------------------------------------------------------
Hello               |There               |.                   
Lorem ipsum dolor...|Kenobi!             |New                 
--------------------------------------------------------------
```


### Creating new weather dataframe with three float fields

```text
Temp (F) [Float]    |Temp (C) [Float]    |Humidity [Float]    
--------------------------------------------------------------
      10109876543.01|                4.01|                4.02
                0.00|                1.00|                4.02
--------------------------------------------------------------
```


### Creating another weather dataframe with three float fields

```text
Temp (F) [Float]    |Temp (C) [Float]    |Humidity [Float]    
--------------------------------------------------------------
                4.10|                5.10|                6.10
                0.10|                1.10|                2.10
--------------------------------------------------------------
```


### Performing a union on the weather dataframes

```text
Temp (F) [Float]    |Temp (C) [Float]    |Humidity [Float]    
--------------------------------------------------------------
      10109876543.01|                4.01|                4.02
                0.00|                1.00|                4.02
                4.10|                5.10|                6.10
                0.10|                1.10|                2.10
--------------------------------------------------------------
```


