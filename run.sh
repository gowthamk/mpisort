make; make clean all
./generate 8192
mpirun -n 2 -machinefile mfile run
