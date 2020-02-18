.PHONY: docs test

test:
	TEST=1 pipenv run python -m unittest discover -p '*_test.py'

coverage:
	TEST=1 pipenv run coverage run --source=. --omit='*_test.py' -m unittest discover -p '*_test.py'
	pipenv run coverage html

pyright:
	pyright -p .

pyright-watch:
	pyright -p . -w

docs:
	make -C sphinx html

gh-pages:
	bash ./tools/gh-pages.bash
