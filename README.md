# formatting_pipeline
A pipeline to format bescha and ted in an equal way


### Create a virtual environment

Before creating the environment some packages might be necessary in order to create a virtual environment. Pick a python version which suits your project.

```
apt install python-venv
```
If the package above is already installed you can proceed further.


```
python3 -m venv .venv
source .venv/bin/activate
pip3 install -r requirements.txt
```

### Create a .env file

Load environment variables from .env file which you need to create in your local repo. The .env file is not pushed to the remote repository.

How to use the .env in you Notebooks:
```
from dotenv import load_dotenv
import os
load_dotenv()  

any_variable = os.environ.get("general_description_of_data")
```