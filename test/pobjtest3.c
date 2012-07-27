#include <mpi.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

#define D_SIZE		(1024*1024)
#define MAX_NCON	8
#define ITER	10
/* #define VERIFY */
#define DEBUG if (1)


int		nprocs, myrank;
int		dlen;
int		ncon;
double		*data[MAX_NCON];
MPI_Request	*req;
MPI_Status	*stat;

void
message(FILE *fp, const char *fmt, ...)
{
    va_list ap;
    char buf[2048];

    va_start(ap, fmt);
    vsprintf(buf, fmt, ap);
    va_end(ap);
    fprintf(fp, "[%d]: %s", myrank, buf);
    fflush(fp);
}


void
init()
{
    int		i, j;

    for (i = 0; i < ncon; i++) {
	data[i] = malloc(sizeof(double)*dlen);
	if (data[i] == 0) {
	    message(stdout, "Cannot allocate data whose size is %d\n",
		    sizeof(double)*dlen);
	    MPI_Abort(MPI_COMM_WORLD, -1);
	    exit(-1);
	}
	if (myrank == 0) {
	    for (j = 0; j < dlen; j++) {
		data[i][j] = (double) j;
	    }
	} else {
	    memset((void*) data[i], 0, dlen);
	}
    }
    req = malloc(sizeof(MPI_Request)*ncon);
    stat = malloc(sizeof(MPI_Status)*ncon);
}

int
verify()
{
    int		i;
    double	*dp;

    if (myrank == 1) {
	for (i = 0; i < ncon; i++) {
	    dp = data[i];
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
    int		i;
    int		length;
    double	time, time0, time1;

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &myrank);

    dlen = D_SIZE;
    ncon = 2;
    --argc, ++argv;
    while (argc > 0) {
	if (strcmp(argv[0], "-len") == 0) {
	    length = atoi(argv[1]);
	    dlen = length/sizeof(double);
	    argc -= 2; argv += 2;
	} else if (strcmp(argv[0], "-con") == 0) {
	    ncon = atoi(argv[1]);
	    argc -= 2; argv += 2;
	} else {
	    break;
	}
    }
    if (ncon > MAX_NCON) ncon = MAX_NCON;
    init();
    if (myrank == 0) {
	for (i = 0; i < ncon; i++) {
	    MPI_Send_init(data[i], dlen, MPI_DOUBLE,
			  1, 0, MPI_COMM_WORLD, &req[i]);
	}
    } else {
	for (i = 0; i < ncon; i++) {
	    MPI_Recv_init(data[i], dlen, MPI_DOUBLE,
			  0, 0, MPI_COMM_WORLD, &req[i]);
	}
    }

    time0 = MPI_Wtime();
    for (i = 0; i < ITER; i++) {
#ifdef VERIFY
	int		cc;
#endif
	MPI_Startall(ncon, req);
	/* calculation */
	MPI_Waitall(ncon, req, stat);
#ifdef VERIFY
	cc = verify();
	if (cc < 0) {
	    message(stdout, "Failed in pos(%d).\n", -cc);
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
    for (i = 0; i < ncon; i++) {
	MPI_Request_free(&req[i]);
    }
    MPI_Finalize();
    return 0;
}

