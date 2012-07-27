#include <mpi.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

#define D_SIZE	(1024*1024)
#define ITER	10
/* #define VERIFY 1 */
#define DEBUG if (1)


int	nprocs, myrank;
int	dlen;
double	*data;


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

    if (myrank == 0) {
	for (i = 0; i < dlen; i++) {
	    data[i] = (double) i;
	}
    } else {
	/* it is not 0.0 */
	memset((void*) data, 0, dlen);
    }
}

int
verify()
{
    int		i;

    for (i = 0; i < dlen; i++) {
	if (data[i] != (double)i) {
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
    MPI_Request	req;
    MPI_Status	stat;
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
    data = malloc(sizeof(double)*dlen);
    if (data == 0) {
	message(stdout, "Cannot allocate data whose size is %d\n", sizeof(double)*dlen);
	MPI_Abort(MPI_COMM_WORLD, -1);
	exit(-1);
    }
    init();
    if (myrank == 0) {
	MPI_Send_init(data, dlen, MPI_DOUBLE, 1, 0, MPI_COMM_WORLD, &req);
    } else {
	MPI_Recv_init(data, dlen, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD, &req);
    }

    time0 = MPI_Wtime();
    for (i = 0; i < ITER; i++) {
#ifdef VERIFY
	int		cc;
#endif
	MPI_Start(&req);
	/* calculation */
	MPI_Wait(&req, &stat);
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
    MPI_Request_free(&req);
    MPI_Finalize();
    return 0;
}

