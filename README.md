# Container Chameleon

## Setup

1. Create a venv: `python -m venv .venv`
2. Activate the venv: `.venv\Scripts\activate` on Windows, `source .venv/bin/activate` on Linux/MacOS
3. Install the required packages: `pip install -r requirements.txt`
4. Add your API key to a `.env` file at the project root, or set it as a environment variable. You can use `.env.example` as a template.

## Usage

First, activate the venv: `.venv\Scripts\activate` on Windows, `source .venv/bin/activate` on Linux/MacOS

Then you can:
- Run unit tests in `test/` folder using `pytest`
- Compare performance of algorithms on data by running `evaluation/compare_performance.py`
- Run algorithms by themselves by running other scripts in subfolders of `evaluation/`

## Development:
The main code is under `src`, code for performance evaluation is under `evaluation`, code for unit tests is under `test`

When running and developing code, it is recommended to put the entry points into `evaluation` and only keep logic under `src`. For example, `evaluation/metaheuristic/run_aco.py` might fetch data, and call a method in `src/metaheuristic` to optimise over it.

### Adding new packages
1. Add it to `requirements.txt` by putting output of `pip freeze` into `requirements.txt`. On Linux/MacOS, it is easiest to do `pip freeze > requirements.txt`
2. If you pull and the code has updated `requirements.txt`, you will need to install the new packages: `pip install -r requirements.txt`
