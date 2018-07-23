.PHONY: clean

CFLAGS  := -Wall -g -std=c++11 
LD      := g++
LDLIBS  := ${LDLIBS} -lrdmacm -libverbs -lpthread -lglog

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
	clear
	./rdma --server 12.12.11.19
client:
	clear
	./rdma --server 12.12.11.19 --client 12.12.11.18

syn:
	scp newplan@12.12.11.18:/home/newplan/rdma_test/src/Makefile ./
	scp newplan@12.12.11.18:/home/newplan/rdma_test/src/rdma.cpp ./
	scp newplan@12.12.11.18:/home/newplan/rdma_test/src/rdma.h ./


