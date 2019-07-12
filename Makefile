.PHONY: docs

test:
	python -m unittest discover -p '*_test.py'                                                                                                                              

docs:
	make -C sphinx html