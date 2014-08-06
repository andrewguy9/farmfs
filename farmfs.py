#!/usr/bin/env python
import argparse

parser = argparse.ArgumentParser(description='Manage a farmfs instance.',)

"""Builds a symlink farm out of a directory.
Looks at a the directory and turns all the
files into symlinks into the md5 sums of the files
under .farmfs/data"""
def main():
  args = parser.parse_args();
  pass


