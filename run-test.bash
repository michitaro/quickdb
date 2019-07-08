set -e
python=~/anaconda3/bin/python

find ./sql2mapreduce -name '*.py' | grep -v trash | cut -c3- | rev | cut -c4- | rev | tr / . | while read i ; do
    echo $i
    $python -m $i
done
