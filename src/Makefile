CC		= mpifccpx
CFLAGS		= -Xg -g -O0 -G -KPIC
#CFLAGS		= -Xg    -O3 -G -KPIC
CPPFLAGS	= -DFJ_MPI

#CC		= mpicc
#CFLAGS		= -O3 -Wall -g -G -fPIC
#RM		= rm -f

# ======================================================

L1_SRCS		= prdma.c
L1_HDRS		= prdma.h timesync.h version.h
L1_OBJS		= $(L1_SRCS:.c=.o)
L1_LIB		= libprdma.so
L1_MAP		= libprdma.map
L1_LDFLAGS	= -Wl,--version-script,$(L1_MAP)

OBJS		+= $(L1_OBJS)
LIBS		+= $(L1_LIB)

# ======================================================

all:		$(LIBS) $(BINS)

$(L1_LIB):	$(L1_OBJS) $(L1_MAP)
		$(CC) $(CFLAGS) -o $(@) $(L1_OBJS) $(L1_LDFLAGS)

prdma.o:	$(L1_HDRS)

clean:
		$(RM) $(LIBS) $(BINS)
		$(RM) $(OBJS)
		$(RM) core.*

# ======================================================

tgz:
		tar czf 123.tar.gz $(L1_SRCS) $(L1_HDRS) $(L1_MAP) Makefile

