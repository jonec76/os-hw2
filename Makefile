CC=g++
CFLAGS=-Wall -O2 -pthread

.PHONY:clean

main:main.cpp
	$(CC) -o $@ $< $(CFLAGS)
create_data:create_data.cpp
	$(CC) -o $@ $< 
clean:
	rm *.o
