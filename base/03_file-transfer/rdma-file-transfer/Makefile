.PHONY: clean

CFLAGS  := -Wall  -g -std=c99
LD      := gcc
LDLIBS  := ${LDLIBS} -lrdmacm -libverbs -lpthread

APPS    := client server

all: ${APPS}

client: common.o client.o
	${LD} -o $@ $^ ${LDLIBS}

server: common.o server.o
	${LD} -o $@ $^ ${LDLIBS}

clean:
	rm -f *.o ${APPS}

