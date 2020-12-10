# catalog-service

## Setting up the development environment

Setting up the environment is normally done via GNU `make` 

Start by creating an environment with `conda` (assuming it's already installed)

`make create-environment`

Base environment is declared in `environment.yml`.
`pip`-managed requirements are handled in `requirements*.txt` files.
These files are read both by `environment.yml` and by the `setup.py` script.

Subsequent changes in the environment, can be tracked & managed via other `make` targets accordingly
* `make update-environment`
* `make list-outdated-packages`