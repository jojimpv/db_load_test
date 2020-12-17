# snow test suite

## Env Set Up
* This code requires spark.
* Create a virtual enviornment.  
* pip install -r requirements.txt

## Test Data Set up
* Use the queries.xlsx in the base path as a template and set up your test data  
* PARAM is a keyword in the excel  
* You can pass as many parameters. If you need more than 3 parameters add columns param4, param5 and call them $PARAM4  


## Logic


 
## How to run
from terminal inside the virtual env 
gunicorn app:app -b 0.0.0.0:5000 --timeout 100
localhost:5000/setup

