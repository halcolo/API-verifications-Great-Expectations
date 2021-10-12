from flask import Flask
from get_project_root import root_path
import great_expectations as ge

# Create Great expectations context
context=ge.get_context()

# Getting root project path
PROJECT_ROOT = root_path(ignore_cwd=False)

# Creating flask app
app = Flask(__name__)