# Database test suite

## Env Set Up
* Create a virtual enviornment  
* virtualenv -p `which python3` venv
* source venv/bin/activate    
* pip install -r requirements.txt

## Test Data Set up
* Use the queries.xlsx in the base path as a template and set up your test data  
* PARAM is a keyword in the excel  
* You can pass as many parameters. If you need more than 3 parameters add columns param4, param5 and call them $PARAM4 
* If there is a IN condition where we need to get 3 values from the parameter list then code as $PARAM1:3#

## How to run
from terminal inside the virtual env 
gunicorn app:app -b 0.0.0.0:5000 --timeout 100  
localhost:5000

