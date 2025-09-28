# custom_pipline_Fintel
#### Developer: Derek Fintel
#### Contact: s542635@youremail; 555-abc-1234

## Custome Pipleline Overview
This project builds upon a prior assignment that streamed data from a producer to a consumer and transformed the data through a duckDB OLAP and exported to CSV. 

For this project, we're going to use the distribution of working programs and modify them towards a new use-case and data source. 

### Example repos:
Past Project: https://github.com/dfintel25/buzzline-05-fintel
Original Base:https://github.com/denisecase/buzzline-05-case

### Source Data:
We'll leverage a publicly available test dataset from [Kaggle](https://www.kaggle.com/) that resembles point of sale data typical from a coffee shop. 
Link: https://www.kaggle.com/datasets/jawad3664/coffee-shop/data

### Use-case:


### Visualization:


### Preliminary Setup Steps
### 1. Initialize
```
1. Click "New Repository"
    a. Generate name with no spaces
    b. Add a "README.md"
2. Clone Repository to machine via VS Code
    a. Create folder in "C:\Projects"
3. Install requirements.txt
4. Setup gitignore
5. Test example scripts in .venv
```
### 2. Create Project Virtual Environment
```
py -m venv .venv
.venv\Scripts\Activate
py -m pip install --upgrade pip 
py -m pip install -r requirements.txt
Retrieve installed items: !pip list
```
### 3. Git add, clone, and commit
```
git add .
git clone "urlexample.git"
git commit -m "add .gitignore, cmds to readme"
git push -u origin main
```
### 4. If copying a repository:
```
1. Click "Use this template" on this example repository (if it's not a template, click "Fork" instead).
2. Clone the repository to your machine:
   git clone example-repo-url
3. Open your new cloned repository in VS Code.
```
### 5. spaCy Specific Installs
```
1. pip install -U pip setuptools wheel
2. pip install -U spacy
3. python -m spacy download en_core_web_sm
```
### 6. HTML Export
```
import os os.system('jupyter nbconvert --to html python-ds.ipynb')
```
### 7. Specific Module 7 Imports
```
python -m pip install beautifulsoup4
python -m pip install html5lib
python -m pip install requests
python -m pip install spacy
python -m pip install spacytextblob

```
**Commands:**
To run the Producer:
```shell
.venv\Scripts\Activate
py -m producers.producer_case
```

To run the Consumer:
```shell
.venv\Scripts\Activate
py -m consumers.consumer_fintel
```