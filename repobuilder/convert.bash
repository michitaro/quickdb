export PYTHONPATH=$PWD/extinction/python OMP_NUM_THREADS=4

for rerun in pdr2_{dud,wide} ; do
    python ./buildrepo.py ./rerun/$rerun -o repo/$rerun -j 8
done

