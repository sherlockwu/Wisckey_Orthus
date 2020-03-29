#!/bin/bash

make clean
make -j 4
make db_bench 
