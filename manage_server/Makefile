all: mypy pylint

mypy:
	python3 -m mypy -p module 
	python3 -m mypy *py

pylint:
	PYTHONPATH=module python3 -m pylint --rcfile=lint.rc module/*py
