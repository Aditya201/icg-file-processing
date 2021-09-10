# icg-file-processing

### What is this repository for? ###
* This repository is to read two flat files (excel and csv), validate them and to produce output files. 
* The handler function can be used to for an AWS Lambda function trigger by file available event
* This component assumes that look up file contains all the fields and and its data types in the first worksheet
* The second worksheet of the lookup file contains all the Country, Company and Currency code with their type and Name

### To Test the repository 
1. Clone repository 
1. Create new virtual environment, Python 3.8
1. Install all required packages using command pip install -r requirements/dev.txt
1. run python -m pytest