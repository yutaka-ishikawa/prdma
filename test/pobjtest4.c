#include <mpi.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

#define D_SIZE		(1024*1024)
#define MAX_PEER	32
#define ITER	100
/* #define VERIFY */
#define DEBUG if (1)

extern int PrdmaReserveRegion(void *addr, int size);
int		nprocs, myrank;
int		dlen;
int		npeers;
double		*recdata[MAX_PEER];
double		*snddata;
MPI_Request	*req;
MPI_Status	*stat;

#define RANK0_ONLY	0
#define RANK_ALL	1

void
message(int flag, FILE *fp, const char *fmt, ...)
{
    va_list ap;
    char buf[2048];

    if (flag == RANK0_ONLY && myrank != 0) return;
    va_start(ap, fmt);
    vsprintf(buf, fmt, ap);
    va_end(ap);
    fprintf(fp, "[%d]: %s", myrank, buf);
    fflush(fp);
}


void
init(int nc)
{
    int		i;

    snddata = malloc(sizeof(double)*dlen*(nprocs+1));
    PrdmaReserveRegion(snddata, sizeof(double)*dlen*(nprocs+1));
    for (i = 0; i < dlen; i++) {
	snddata[i] = (double) i;
    }
    for (i = 0; i < nc; i++) {
	recdata[i] = malloc(sizeof(double)*dlen*(nprocs+1));
	if (recdata[i] == 0) {
	    message(RANK_ALL, stdout, "Cannot allocate data whose size is %d\n",
		    sizeof(double)*dlen*(nprocs+1));
	    MPI_Abort(MPI_COMM_WORLD, -1);
	    exit(-1);
	}
	PrdmaReserveRegion(recdata[i], sizeof(double)*dlen*(nprocs+1));
	memset((void*) recdata[i], 0, dlen);
    }
    req = malloc(sizeof(MPI_Request)*nc*2);
    stat = malloc(sizeof(MPI_Status)*nc*2);
}

int
verify()
{
    int		i;
    double	*dp;

    if (myrank == 1) {
	for (i = 0; i < npeers; i++) {
	    dp = recdata[i];
	    for (i = 0; i < D_SIZE; i++) {
		if (dp[i] != (double)i) {
		    return -i;
		}
	    }
	}
    }
    return 0;
}

int
main(int argc, char **argv)
{
    int		i, ridx;
    int		length;
    double	time, time0, time1;

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &myrank);
    if (nprocs > MAX_PEER) {
	message(RANK0_ONLY, stderr, "Too many processes. max = %d\n", MAX_PEER);
	MPI_Finalize();
	exit(-1);
    }
    if (myrank == 0) {
	fprintf(stderr, "MPI_REQUEST_NULL = %p\n", MPI_REQUEST_NULL);
	fprintf(stderr, "MPI_UNDEFINED = %p\n", MPI_UNDEFINED);
    }

    dlen = D_SIZE;
    npeers = nprocs;
    --argc, ++argv;
    while (argc > 0) {
	if (strcmp(argv[0], "-len") == 0) {
	    length = atoi(argv[1]);
	    dlen = length/sizeof(double);
	    argc -= 2; argv += 2;
	} else {
	    break;
	}
    }
    if (myrank == 0) {
	--npeers;
	init(npeers);
	for (i = 1, ridx = 0; i <= npeers; i++, ridx += 2) {
	    MPI_Recv_init(recdata[i-i], dlen*i, MPI_DOUBLE,
			  i, 0, MPI_COMM_WORLD, &req[ridx]);
	    MPI_Send_init(snddata, dlen*i, MPI_DOUBLE,
			  i, 0, MPI_COMM_WORLD, &req[ridx+1]);
	}
    } else {
	npeers = 1;
	init(npeers);
	MPI_Recv_init(recdata[0], dlen*myrank, MPI_DOUBLE,
		      0, 0, MPI_COMM_WORLD, &req[0]);
	MPI_Send_init(snddata, dlen*myrank, MPI_DOUBLE,
		      0, 0, MPI_COMM_WORLD, &req[1]);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    /* message(RANK0_ONLY, stdout, "%d processes\n", npeers); */
    time0 = MPI_Wtime();
    for (i = 0; i < ITER; i++) {
#ifdef VERIFY
	int		cc;
#endif
	MPI_Startall(npeers*2, req);
	/* calculation */
	MPI_Waitall(npeers*2, req, stat);
#ifdef VERIFY
	cc = verify();
	if (cc < 0) {
	    message(RANK_ALL, stdout, "Failed in pos(%d).\n", -cc);
	}
#endif
    }
    time1 = MPI_Wtime();
    if (myrank == 0) {
	int	size;
	MPI_Type_size(MPI_DOUBLE, &size);
	size *= dlen;
	time = (time1 - time0)/(double)ITER;
	fprintf(stdout, "%d\t%g\t%g\n",
		size, time, ((double)size)/time);
	fflush(stdout);
    }
    for (i = 0; i < npeers*2; i++) {
	MPI_Request_free(&req[i]);
    }
    MPI_Finalize();
    return 0;
}

