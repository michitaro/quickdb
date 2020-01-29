.PHONY: docs test

test:
	pipenv run python -m unittest discover -p '*_test.py'

coverage:
	pipenv run coverage run --source=. --omit='*_test.py' -m unittest discover -p '*_test.py'
	pipenv run coverage html

docs:
	make -C sphinx html

gh-pages:
	bash ./tools/gh-pages.bash
