#ifndef	_TIMESYNC_H
#define _TIMESYNC_H
/*
 * Persistent Communication based on RDMA
 *   31/08/2012	Written by Massa. Hatanaaka
 *		mhatanaka@riken.jp
 */

#include <stdio.h>
#include <stdint.h>
#include <time.h>
#include "mpi.h"

/* #define TIMESYNC_DEBUG */

#define TIMESYNC_ITER	500

#define MPI_IF_ERROR(RC)	if ((RC) != MPI_SUCCESS) { ln = __LINE__; goto bad; }

/*
 * read time stamp counter for sparc64 asm
 */
static inline uint64_t timesync_rdtsc(void)
{
#if	defined(__x86_64__)
	uint32_t hi, lo;
	asm volatile ("rdtsc" : "=a"(lo), "=d"(hi));
	return ( (uint64_t)lo) | (((uint64_t)hi) << 32);
#elif	 defined(__GNUC__) && (defined(__sparcv9__) || defined(__sparc_v9__))
	uint64_t rval;
	asm volatile("rd %%tick,%0" : "=r"(rval));
	return (rval);
#else
#error "unknown architecture for time stamp counter"
#endif
}

static inline double timesync_conv(uint64_t sl, uint64_t sr, uint64_t el, uint64_t er, uint64_t lv)
{
	double dv;
	dv = (double)(lv - sl); /* relative (from start-local-tsc) */
	dv *= ((double)(er - sr) / (double)(el - sl));
	dv /= (2.0 * 1000.0 * 1000.0 * 1000.0); /* 2 GHz or 1.848 GHz */
	return dv;
}

static int timesync_sync(uint64_t *lsynctimep, uint64_t *rsynctimep)
{
	int rc, ln = 0;
	int ii, jj, ni;
	MPI_Datatype type;
	int esiz;
	MPI_Aint msiz;
	MPI_Info info = MPI_INFO_NULL;
	uint64_t *tsctab = 0;
	int csiz;
	MPI_Comm comm = MPI_COMM_WORLD;
	uint64_t *syntab = 0;
	int rank = -1;
	uint64_t rsynctime = 0, lsynctime = 0;

	/* communicator size */
	rc = MPI_Comm_size(comm, &csiz);
	MPI_IF_ERROR(rc);

	/* my rank */
	rc = MPI_Comm_rank(comm, &rank);
	MPI_IF_ERROR(rc);

	/* type check for uint64_t */
	type = MPI_UNSIGNED_LONG;
	rc = MPI_Type_size(type, &esiz);
	MPI_IF_ERROR(rc);
	if (esiz != sizeof (uint64_t)) {
		fprintf(stderr, "[%03d] %d != %d\n", rank, esiz, sizeof (uint64_t));
		rc = MPI_ERR_OTHER;
		goto bad;
	}

	/* iterations */
	ni = TIMESYNC_ITER;

	/* alloc */
	msiz = esiz * (ni + 1);
	rc = MPI_Alloc_mem(msiz, info, &tsctab);
	MPI_IF_ERROR(rc);
	if (tsctab == 0) {
		rc = MPI_ERR_OTHER;
		goto bad;
	}

	/* alloc */
	if (rank == 0) {
		msiz = (esiz * 2) * csiz;
		rc = MPI_Alloc_mem(msiz, info, &syntab);
		MPI_IF_ERROR(rc);
		if (syntab == 0) {
			rc = MPI_ERR_OTHER;
			goto bad;
		}
	}

	/* warming up */
	for (ii = 0; ii < ni; ii++) {
		tsctab[ii] = timesync_rdtsc();
		rc = MPI_Barrier(comm);
		MPI_IF_ERROR(rc);
	}
	tsctab[ii] = timesync_rdtsc();

	/* measurement */
	for (ii = 0; ii < ni; ii++) {
		tsctab[ii] = timesync_rdtsc();
		rc = MPI_Barrier(comm);
		MPI_IF_ERROR(rc);
	}
	tsctab[ii] = timesync_rdtsc();

	/* time calibration */
	{
		int mini = -1;
		uint64_t dmin = 0;
		int root = 0;

		for (ii = 0; ii < ni; ii++) {
			rc = MPI_Gather(
				&tsctab[ii], 2, type,
				syntab, 2, type,
				root, comm);
			MPI_IF_ERROR(rc);
			if (rank == 0) {
				uint64_t tdif = 0;
				for (jj = 0; jj < csiz; jj++) {
					uint64_t dif;
					dif = syntab[(jj * 2) + 1] - syntab[(jj * 2)];
					tdif += dif;
				}
				if ((mini < 0) || (tdif < dmin)) {
					mini = ii;
					dmin = tdif;
				}
			}
		}
		rc = MPI_Bcast(&mini, 1, MPI_INT, root, comm);
		MPI_IF_ERROR(rc);
		if (mini < 0) {
			rc = MPI_ERR_OTHER;
			goto bad;
		}
#ifdef	TIMESYNC_DEBUG
		printf("[%03d] tsctab[%d] = %lu\n",
			rank, mini, tsctab[mini + 1] - tsctab[mini]);
#endif	/* TIMESYNC_DEBUG */

		lsynctime = tsctab[mini];

		rsynctime = 0;
		if (rank == 0) {
			rsynctime = tsctab[mini];
		}
		rc = MPI_Bcast(&rsynctime, 1, type, root, comm);
		MPI_IF_ERROR(rc);
#ifdef	TIMESYNC_DEBUG
		printf("[%03d] rsynctime %lu\n", rank, rsynctime);
#endif	/* TIMESYNC_DEBUG */
	}

	/*
	 * return
	 */
	if (lsynctimep != 0) {
		lsynctimep[0] = lsynctime;
	}
	if (rsynctimep != 0) {
		rsynctimep[0] = rsynctime;
	}
bad:
	if (syntab != 0) {
		rc = MPI_Free_mem(syntab);
		/* MPI_IF_ERROR(rc); */
		syntab = 0;
	}
	if (tsctab != 0) {
		rc = MPI_Free_mem(tsctab);
		/* MPI_IF_ERROR(rc); */
		tsctab = 0;
	}
	if (rc != MPI_SUCCESS) {
		fprintf(stderr, "[%03d] rc = %d at %d\n", rank, rc, ln);
	}

	return rc;
}

#ifdef	TIMESYNC_TEST

#define NEVENTS	5

int main(int argc, char *argv[])
{
	int rc;
	int ii;
	int rank = -1;
	uint64_t sl, sr, el, er;
	uint64_t events[NEVENTS];

	rc = MPI_Init(&argc, &argv);
	{
		MPI_Comm comm = MPI_COMM_WORLD;
		rc = MPI_Comm_rank(comm, &rank);
	}

	sl = sr = el = er = 0;

	rc = timesync_sync(&sl, &sr);

	for (ii = 0; ii < NEVENTS; ii++) {
		struct timespec ts;

		ts.tv_sec  = 2;
		ts.tv_nsec = 0;

		nanosleep(&ts, 0);

		events[ii] = timesync_rdtsc();
	}

	rc = timesync_sync(&el, &er);

	for (ii = 0; ii < NEVENTS; ii++) {
		double dv;
		dv = timesync_conv(sl, sr, el, er, events[ii]);
		printf("[%03d] %14.9f\t%d\n", rank, dv, ii);
	}
	
	rc = MPI_Finalize();

	return (rc != MPI_SUCCESS)? 1: 0;
}
#endif	/* TIMESYNC_TEST */

#endif	/* _TIMESYNC_H */
