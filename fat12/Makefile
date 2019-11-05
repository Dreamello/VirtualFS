CC = gcc
CFLAGS = -Wall -g $(shell pkg-config fuse --cflags) -O3
LDLIBS = $(shell pkg-config fuse --libs) -O3

all: virtual_fat12fs virtual_fat12test

virtual_fat12fs: virtual_fat12fs.o virtual_fat12.o
virtual_fat12test: virtual_fat12test.o virtual_fat12.o

virtual_fat12fs.o: virtual_fat12fs.c virtual_fat12.h
virtual_fat12.o: virtual_fat12.c virtual_fat12.h

clean:
	-rm -rf virtual_fat12fs virtual_fat12test virtual_fat12fs.o virtual_fat12.o virtual_fat12test.o
tidy: clean
	-rm -rf *~
