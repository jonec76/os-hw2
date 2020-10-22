CC=g++
CFLAGS=-Wall -O2 -pthread

.PHONY:clean

main:main.cpp
	$(CC) -o $@ $< $(CFLAGS)
clean:
	rm *.o
