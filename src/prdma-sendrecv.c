/*
 * Sendrecv to Persistent Communication
 *   26/01/2013	Written by Yutaka Ishikawa
 *		ishikawa@is.s.u-tokyo.ac.jp, yutaka.ishikawa@riken.jp
 */
/*
 * Usage:
 *	MPI_Attr_put(comm, prdma_sendrecv, "trans0,default");
 *	MPI_Sendrecv(......);
 *
 *	MPI_Attr_put(comm, prdma_sendrecv, "trans1,first");
 *	MPI_Sendrecv(......);
 *	MPI_Sendrecv(......);
 *	MPI_Attr_put(comm, prdma_sendrecv, "trans1,last");
 *	MPI_Sendrecv(......);
 *
 */
#include <strings.h>
#include <string.h>
#include <stdio.h>
#include <mpi.h>

#define DEBUG 1
#ifdef DEBUG
#define IFDEBUG	if (1)
#else
#define IFDEBUG if (0)
#endif
#define EAGER_LIMIT	(2048)
#define PSRSTAT_SIZE	128
#define MAX_REQ		10
typedef struct PrdmaSendrecvStatus {
    char		transid[128];
    long		comkey[MAX_REQ]; /* dest_rank*maxproc + src_rank */
    void		*snd_baddr[MAX_REQ];
    void		*rcv_baddr[MAX_REQ];
    MPI_Request		req[MAX_REQ*2];
    MPI_Request		curreq[MAX_REQ*2];
    MPI_Status		stat[MAX_REQ*2];
    int			maxprocs;
    int			nreqs;		/* number of sendrec */
    int			curnreqs;
    int			state;
    int			flag;
} PrdmaSendRecvStatus;
#define PSRSTATE_FIRST		0
#define PSRSTATE_REST		1
#define PSRSTATE_LAST		2
#define PSRFLAG_CREATED		0x00
#define PSRFLAG_NEEDSINIT	0x01
#define PSRFLAG_DEFAULT		0x02
#define PSRFLAG_NEEDSSTART	0x04
#define PSRFLAG_NEEDSWAIT	0x08

int	prdma_sendrecv = MPI_KEYVAL_INVALID;
static int	attrput_first = 0;
static int	sendrecv_first = 0;
static int	myrank;
static PrdmaSendRecvStatus	psrstat[PSRSTAT_SIZE];
static int			psrstsize;

static void
attrinit()
{
    PMPI_Comm_rank(MPI_COMM_WORLD, &myrank);
    fprintf(stderr, "[%d] MY MPI_Attr_put: prdma_sendrecv = %d\n",
	    myrank, prdma_sendrecv);
}

static void
sendrecv_init()
{
    int		i, j;

    for (i = 0; i < PSRSTAT_SIZE; i++) {
	psrstat[i].flag = 0;
	psrstat[i].nreqs = 0;
	for (j = 0; j < MAX_REQ*2; j++) {
	    psrstat[i].req[j] = MPI_REQUEST_NULL;
	}
    }
    psrstsize = 0;
    if (attrput_first == 0) {
	/* default action */
	attrinit();
	attrput_first = 1;
    }
}

static void
reqlookup(MPI_Comm comm, int dst, int src, void *saddr, void *raddr,
	  PrdmaSendRecvStatus **pstat, int *reqent, int size)
{
    char	keyval[1024];
    long	lkey;
    char	*val = 0;
    char	*cp, *cmd;
    int		pent, kent;
    int		flag, cc;

    *pstat = NULL;
    IFDEBUG {
	fprintf(stderr, "\t[%d] reqlookup: dst(%d) src(%d)\n",
		myrank, dst, src);
    }
    cc = MPI_Attr_get(comm, prdma_sendrecv, (void*)&val, &flag);
    if (cc != MPI_SUCCESS || flag == 0) {
	return;
    }
    strncpy(keyval, val, 1024);
    cp = index(keyval, ',');
    if (cp == NULL) {
	return;
    }
    *cp = 0; cmd = cp + 1;
    if(!strcmp(cmd, "skip")) {
	fprintf(stderr, "\t[%d] skip\n", myrank);
	*reqent = 0;
	return;
    }
    for (pent = 0; pent < psrstsize; pent++) {
	if (!strcmp(keyval, psrstat[pent].transid)) {
	    long	*cky;
	    void	**snd, **rcv;
	    lkey = dst * psrstat[pent].maxprocs + src;
	    cky = psrstat[pent].comkey;
	    snd = psrstat[pent].snd_baddr;
	    rcv = psrstat[pent].rcv_baddr;
	    for (kent = 0; kent < psrstat[pent].nreqs; kent++)
		if (cky[kent] == lkey
		    && snd[kent] == saddr
		    && rcv[kent] == raddr) goto find;
	    goto newcom;
	}
    }
    /* not found */
    if (psrstsize == PSRSTAT_SIZE) {
	/* Too many transactions are being registered */
	return;
    }
    /* register */
    strcpy(psrstat[pent].transid, keyval);
    MPI_Comm_rank(comm, &psrstat[pent].maxprocs);
    lkey = dst * psrstat[pent].maxprocs + src;
    psrstsize++;
    kent = 0;
newcom:
    if (size <= EAGER_LIMIT) {
	*pstat = 0; *reqent = 0;
	goto setstate;
    }
    if (kent >= MAX_REQ) {
	/* Too many sendrecv */
	return;
    }
    psrstat[pent].comkey[kent] = lkey;
    psrstat[pent].snd_baddr[kent] = saddr;
    psrstat[pent].rcv_baddr[kent] = raddr;
    psrstat[pent].nreqs = kent + 1;
find:
    *pstat = &psrstat[pent];
    *reqent = kent;
    /* command */
    if (psrstat[pent].req[kent*2] == MPI_REQUEST_NULL) {
	/* Needs initialize */
	psrstat[pent].flag = PSRFLAG_NEEDSINIT;
    }
setstate:
    if (!strcmp(cmd, "default")) {
	psrstat[pent].flag |= PSRFLAG_DEFAULT;
	psrstat[pent].curnreqs = 0;
    } else if (!strcmp(cmd, "first")) {
	psrstat[pent].state = PSRSTATE_FIRST;
	psrstat[pent].curnreqs = 0;
	psrstat[pent].flag &= ~PSRFLAG_NEEDSSTART;
	psrstat[pent].flag &= ~PSRFLAG_NEEDSWAIT;
	/* nothing */
    } else if (!strcmp(cmd, "rest")) {
	psrstat[pent].state = PSRSTATE_REST;
    } else if (!strcmp(cmd, "last")) {
	psrstat[pent].state = PSRSTATE_LAST;
	psrstat[pent].flag |= PSRFLAG_NEEDSSTART;
	psrstat[pent].flag |= PSRFLAG_NEEDSWAIT;
    } else {
	;
	/* Unknown command */
    }
    return;
}


int
MPI_Attr_put(MPI_Comm comm, int keyval, void* aval)
{
    int		cc;

    if (attrput_first == 0) {
	attrinit();
	attrput_first = 1;
    }
    cc = PMPI_Attr_put(comm, keyval, aval);
    return cc;
}

int
MPI_Sendrecv(void *sbuf, int scount, MPI_Datatype stype, int dst, int stag,
	     void *rbuf, int rcount, MPI_Datatype rtype, int src, int rtag,
	     MPI_Comm comm, MPI_Status *stat)
{
    int			cc;
    PrdmaSendRecvStatus	*pstat;
    int			reqent;
    int			size;

    if (sendrecv_first == 0) {
	sendrecv_first = 1;
	sendrecv_init();
    }
    PMPI_Type_size(stype, &size);
    size *= scount;
    IFDEBUG {
	fprintf(stderr,
		"[%d] MPI_Sendrecv: dst(%d) src(%d) sbuf(%p) rbuf(%p) size(%d)\n", 
		myrank, dst, src, sbuf, rbuf, size);
    }
    reqlookup(comm, dst, src, sbuf, rbuf, &pstat, &reqent, size);
    if (pstat == NULL) goto notakeover;

    IFDEBUG {
	fprintf(stderr, "\t[%d] MPI_Sendrecv: pstat(%p) reqent(%d) flag(%x) size(%d)\n",
		myrank, pstat, reqent, pstat->flag, size);
    }
    if (pstat->flag & PSRFLAG_NEEDSINIT) {
	IFDEBUG {
	    fprintf(stderr,
		    "\t[%d] MPI_Sendrecv: init(%p) reqent(%d) "
		    "dst(%d) src(%d) scount(%d)\n",
		    myrank, pstat, reqent, dst, src, scount);
	}
	MPI_Send_init(sbuf, scount, stype, dst, stag, comm,
		      &pstat->req[reqent*2]);
	MPI_Recv_init(rbuf, rcount, rtype, src, rtag, comm,
		      &pstat->req[reqent*2 + 1]);
	pstat->flag &= ~PSRFLAG_NEEDSINIT;
    }
    pstat->curreq[pstat->curnreqs] = pstat->req[reqent*2];
    pstat->curreq[pstat->curnreqs + 1] = pstat->req[reqent*2 + 1];
    pstat->curnreqs += 2;
    IFDEBUG {
	int	i;
	fprintf(stderr,
		"\t[%d] MPI_Sendrecv: flag(%x) reqent(%d) curnreqs(%d)\n",
		myrank, pstat->flag, reqent, pstat->curnreqs);
	for (i = 0; i < pstat->curnreqs; i++) {
	    fprintf(stderr,
		    "\t[%d] MPI_Sendrecv: curreq[%d] = %p\n",
		    myrank, i, pstat->curreq[i]);
	}
    }
    if (pstat->flag & PSRFLAG_NEEDSSTART) {
	IFDEBUG {
	    fprintf(stderr, "\t[%d] MPI_Sendrecv: start(%p) curnreqs(%d)\n",
		    myrank, pstat, pstat->curnreqs);
	}
	MPI_Startall(pstat->curnreqs, pstat->curreq);
	pstat->flag &= ~PSRFLAG_NEEDSSTART;
    }
    if (pstat->flag & PSRFLAG_NEEDSWAIT) {
	IFDEBUG {
	    fprintf(stderr, "\t[%d] MPI_Sendrecv: wait(%p) curnreqs(%d)\n",
		    myrank, pstat, pstat->curnreqs);
	}
	MPI_Waitall(pstat->curnreqs, pstat->curreq, pstat->stat);
	pstat->flag &= ~PSRFLAG_NEEDSWAIT;
    }
    if (pstat->flag & PSRFLAG_DEFAULT) {
	IFDEBUG {
	    fprintf(stderr, "\t[%d] MPI_Sendrecv: default(%p) curnreqent(%d)\n",
		    myrank, pstat, pstat->curnreqs);
	}
	MPI_Startall(2, pstat->curreq);
	MPI_Waitall(2, pstat->curreq, pstat->stat);
    }
    return MPI_SUCCESS;
notakeover:
    cc = PMPI_Sendrecv(sbuf, scount, stype, dst, stag,
		       rbuf, rcount, rtype, src, rtag, comm, stat);
	return cc;
}
