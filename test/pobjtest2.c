#include <mpi.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

#define D_SIZE	(1024*1024)
#define ITER	10
/* #define VERIFY */
#define DEBUG if (1)


int	nprocs, myrank;
int	dlen;
double	*data1;
double	*data2;

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
    int		i;
    double	*dp;

    data1 = malloc(sizeof(double)*dlen);
    data2 = malloc(sizeof(double)*dlen);
    if (data1 == 0 || data2 == 0) {
	message(stdout, "Cannot allocate data whose size is %d\n", sizeof(double)*dlen);
	MPI_Abort(MPI_COMM_WORLD, -1);
	exit(-1);
    }
    memset((void*) data1, 0, dlen);
    memset((void*) data2, 0, dlen);
    if (myrank == 0) dp = data1;
    else dp = data2;

    for (i = 0; i < dlen; i++) {
	    dp[i] = (double) i;
    }
}

int
verify()
{
    int		i;
    double	*dp;

    if (myrank == 0) dp = data1;
    else dp = data2;
    for (i = 0; i < D_SIZE; i++) {
	if (dp[i] != (double)i) {
	    return -i;
	}
    }
    return 0;
}

int
main(int argc, char **argv)
{
    int		i;
    int		length;
    MPI_Request	req[2];
    MPI_Status	stat[2];
    double	time, time0, time1;

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &myrank);

    dlen = D_SIZE;
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
    init();
	message(stderr, "STEP 1\n");
    if (myrank == 0) {
	message(stderr, "STEP 2\n");
	MPI_Recv_init(data2, dlen, MPI_DOUBLE, 1, 0, MPI_COMM_WORLD, &req[1]);
	message(stderr, "STEP 3\n");
	MPI_Send_init(data1, dlen, MPI_DOUBLE, 1, 0, MPI_COMM_WORLD, &req[0]);
    } else {
	message(stderr, "STEP 2\n");
	MPI_Recv_init(data1, dlen, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD, &req[0]);
	message(stderr, "STEP 3\n");
	MPI_Send_init(data2, dlen, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD, &req[1]);
    }
	message(stderr, "STEP 4\n");

    time0 = MPI_Wtime();
    for (i = 0; i < ITER; i++) {
#ifdef VERIFY
	int		cc;
#endif
	message(stderr, "MPI_Startall\n");
	MPI_Startall(2, req);
	/* calculation */
	message(stderr, "MPI_Waitall\n");
	MPI_Waitall(2, req, stat);
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
    MPI_Request_free(&req[0]);
    MPI_Request_free(&req[1]);
    MPI_Finalize();
    return 0;
}

