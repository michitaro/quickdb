.PHONY: docs

test:
	python -m unittest discover -p '*_test.py'                                                                                                                              

docs:
	make -C sphinx html

gh-pages:
	bash ./tools/gh-pages.bash
