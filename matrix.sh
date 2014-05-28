#!/bin/bash

#/home/matrix/matrix-hpc/server 50000 neighbor zht.cfg TCP kiran 0 1 1 /tmp ~/matrix-hpc/

#/home/matrix/matrix-hpc/client 1 neighbor zht.cfg TCP 20000000 0 1 1 1 /tmp ~/matrix-hpc/ 0 3

./server 50000 neighbor zht.cfg TCP kiran 0 1 4 /tmp ~/matrix-hpc/ > /home/kramamu1/matrix-hpc/$1 2>&1 &

./client 4 neighbor zht.cfg TCP 0 0 1 1 4 /tmp ~/matrix-hpc/ 0 10 > /home/kramamu1/matrix-hpc/$2 2>&1 &
