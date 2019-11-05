CC = gcc
CFLAGS = -Wall -Wno-unused-function -g $(shell pkg-config fuse --cflags) -std=gnu11
LDLIBS = $(shell pkg-config fuse --libs)

all: virtual_ext2fs virtual_ext2test

virtual_ext2fs: virtual_ext2fs.o virtual_ext2.o
virtual_ext2test: virtual_ext2test.o virtual_ext2.o

virtual_ext2fs.o: virtual_ext2fs.c virtual_ext2.h
virtual_ext2.o: virtual_ext2.c virtual_ext2.h

clean:
	-rm -rf virtual_ext2fs virtual_ext2test virtual_ext2fs.o virtual_ext2.o virtual_ext2test.o
tidy: clean
	-rm -rf *~
