.PHONY: clean

CFLAGS  := -Wall -g -std=c++11
LD      := g++
LDLIBS  := ${LDLIBS} -lrdmacm -libverbs -lpthread

APPS    := rdma

HEADE   := rdma.h

all: ${APPS}


rdma: rdma.o
	${LD} -o $@ $^ ${LDLIBS}

%.o:%.cpp $(HEADE)
	echo 'compile'
	$(LD) -c $(CFLAGS) $< -o $@

clean:
	rm -f *.o ${APPS} *.txt

server:
	./rdma --server 12.12.12.2
client:
	./rdma --server 12.12.12.2 --client 12.12.12.2

syn:
	scp newplan@12.12.11.18:/home/newplan/rdma_test/src/Makefile ./
	scp newplan@12.12.11.18:/home/newplan/rdma_test/src/rdma.cpp ./
	scp newplan@12.12.11.18:/home/newplan/rdma_test/src/rdma.h ./


