MPICC	=	mpifccpx
CFLAGS	= -DFJ_MPI -O3 -Wall
OBJ	= ../src/prdma.o
#MPICC	=	mpicc
#CFLAGS	= -O3 -Wall -g
#OBJ	= ../src/prdma.o ftest.o
RM	= rm
ALLPROGS= pobjtest1 pobjtest2 pobjtest3 pobjtest4

all: $(ALLPROGS)
pobjtest1: $(OBJ)
	$(MPICC) $(CFLAGS) pobjtest1.c -o pobjtest1-prdma $(OBJ)
	$(MPICC) $(CFLAGS) pobjtest1.c -o pobjtest1-org
pobjtest2: $(OBJ)
	$(MPICC) $(CFLAGS) pobjtest2.c -o pobjtest2-prdma $(OBJ)
	$(MPICC) $(CFLAGS) pobjtest2.c -o pobjtest2-org
pobjtest3: $(OBJ)
	$(MPICC) $(CFLAGS) pobjtest3.c -o pobjtest3-prdma $(OBJ)
	$(MPICC) $(CFLAGS) pobjtest3.c -o pobjtest3-org
pobjtest4: $(OBJ)
	$(MPICC) $(CFLAGS) pobjtest4.c -o pobjtest4-prdma $(OBJ)
	$(MPICC) $(CFLAGS) pobjtest4.c -o pobjtest4-org
ftest.o:ftest.c
	$(MPICC) $(CFLAGS) -c ftest.c
clean:
	$(RM) -f *.o
	$(RM) -f core.* *.sh.e* *.sh.i* *.sh.o*
	$(RM) -f probjtest1-*
#
tar:
	tar czf 123.tar.gz *.c Makefile *.sh
