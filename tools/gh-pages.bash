set -e

if [ $(git status -s | wc -l) -ne 0 ] ; then
  echo "Working tree is not clean."
  exit 1
fi

if [ $(git symbolic-ref --short HEAD) != 'master' ] ; then
  echo "Checkout master first"
  exit 1
fi

if ! git branch | grep -q gh-pages ; then
  git checkout -b gh-pages origin/gh-pages
  git checkout master
fi

make docs
git checkout gh-pages
mv sphinx/_build/html .html
rm -rf *
mv .html/* .
git add .
git commit -m "Update gh-pages"
git checkout master

echo "After a few miniuts, https://michitaro.github.io/quickdb/ will be updated."
