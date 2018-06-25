CC=g++
CFLAGS = -Wvla -Wall -Wextra -std=c++11 -pthread -g

.DEFAULT_GOAL: libMapReduceFramework.a

# Libraries
libMapReduceFramework.a : Barrier.o MapReduceFramework.o
	ar rcs libMapReduceFramework.a Barrier.o MapReduceFramework.o

# Object files
Barrier.o:  Barrier.cpp Barrier.h
	$(CC) $(CFLAGS) -c Barrier.cpp 
	
MapReduceFramework.o:  MapReduceFramework.cpp MapReduceFramework.h
	$(CC) $(CFLAGS) -c MapReduceFramework.cpp
	
SampleClient:  SampleClient.cpp MapReduceClient.h MapReduceFramework.cpp MapReduceFramework.h Barrier.cpp Barrier.h
	$(CC) $(CFLAGS) SampleClient.cpp MapReduceFramework.cpp Barrier.cpp -o a

clean:
	rm -f *.o libMapReduceFramework.a ex3.tar

tar:
	tar -cvf ex3.tar MapReduceFramework.cpp Barrier.cpp Barrier.h Makefile README

valgrind:
	valgrind --leak-check=full --show-possibly-lost=yes --show-reachable=yes --track-origins=yes --undef-value-errors=yes a
	
drd:
	valgrind --tool=drd a

helgrind:
	valgrind --tool=helgrind a
