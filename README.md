<p>
    <a href="https://github.com/RedCarpetUp/ledger/actions">
        <img src="https://github.com/RedCarpetUp/ledger/workflows/Tests/badge.svg" alt="Test Status" height="18">
    </a>
    <a href="https://github.com/RedCarpetUp/ledger/actions">
        <img src="https://github.com/RedCarpetUp/ledger/workflows/pre-commit%20hooks/badge.svg" alt="Pre-commit Status" height="18">
    </a>

</p>
<p>
    <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/python-3.7+-blue.svg" alt="Python version" height="18"></a>
    <a href="https://github.com/RedCarpetUp/ledger/blob/master/LICENSE"><img src="https://img.shields.io/pypi/l/markdown-subtemplate.svg" alt="License" height="18"></a>
    <a href="https://github.com/psf/black">
        <img src="https://img.shields.io/badge/code%20style-black-000000.svg" alt="Codestyle Black" height="18">
    </a>
</p>

# Setup

- make sure you are using python > 3.7+
- setup virtualenv using `virtualenv env` and activate it
- run `pip install -e "."` to install dependencies. keep setup.py updated
  - rerun `pip install -e "."` if you make changes to models/source code. not needed for testcase changes
- run `pytest --mypy --black --isort --cov=rush --cov-report=xml --cov-report=term` to run your tests
  - if black formatting tests fail, just run `black .` from your top level directory. Alternatively you can setup black in vscode (I highly recommend setting up all three ***"format on paste/save/type"***)
  - if isort formatting tests fail, just run `isort  -rc .` from your top level directory.
- all source code is under `src/rush/` . That is where you should make your code
- docker kill rush_pg
- psql postgresql://alem_user:password@localhost:5680/alem_db
