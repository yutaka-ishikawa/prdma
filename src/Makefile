MPICC	=	mpifccpx
CFLAGS	= -DFJ_MPI -O3 -Xg
#MPICC	=	mpicc
#CFLAGS	= -O3 -Wall -g
RM	=	rm

prdma.o: prdma.c prdma.h timesync.h version.h
	$(MPICC) $(CFLAGS) -c prdma.c

clean:
	$(RM) -f *.o
	$(RM) -f core.* run.sh.e* run.sh.i* run.sh.o*

tar:
	tar cf 123.tar *.h *.c Makefile run.sh run-original.sh RESULT*

