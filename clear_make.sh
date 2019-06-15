#!/bin/bash
# Script to reset to first downloaded state

read -p "Delete all cmake/make/build. Are you sure? y/n: " -n 1 -r
echo    # (optional) move to a new line
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    exit 1
fi
rm Makefile
rm -rf CMakeFiles/
rm CMakeCache.txt
rm cmake_install.cmake
rm -rf build/