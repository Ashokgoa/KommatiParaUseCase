# KommatiParaUseCase
===================================

### About the project


This Use case is used to derive a new dataset from 2 input datasets applying some business transformations:
1)read  2 different data sets to spark data frames one has client data ther has financial client financial information
2) Filter out client data set for Netherlands and United Kingdom countries
3)Join filtered client data with financial data sets to get client financial details for Netherlands and United Kingdom
4)Rename the columns for user readability and store output data to output location

* Main python script are located in /code folder.
* Input datasets are in /data/input
* Output dataset is in /data/client_data
* Testcases are located in  /code/testcases/


### Pre-requisites

You will need the following components to run the project:
```
Python 3.7
```
### Environment setup
- Clone the project repository from the main branch:
```
git clone --single-branch --branch main https://github.com/Ashokgoa/KommatiParaUseCase.git
```
- Install the requirements
```
pip install -r requirements.txt
```
- Create your own  branch for experimentation and creating/modifying/improving features.