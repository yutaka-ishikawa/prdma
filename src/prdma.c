/*
 * Persistent Communication based on RDMA
 *   20/02/2012	Written by Yutaka Ishikawa
 *		ishikawa@is.s.u-tokyo.ac.jp, yutaka.ishikawa@riken.jp
 */
/*
 * _prdmaSync: lower 24 bits are used for sync index
 *  MSB +----+----+----+----+----+----+----+----+ LSB
 *	|0000|0000|0000|0000|0000|0000|0000|0000|
 *	+----+----+----+----+----+----+----+----+
 *         fu|   t|         sync index          |
 *	 u : This bit indicates busy of this entry
 *	 f : This bit indicates the first time of transmission.
 *	 t : This bit is flip/flop each transaction
 */
/*
 * Sender			Receiver
 *    MPI_Start			   MPI_Start
 *	state = PREPARED
 *					_prdmaSync[lsync] = receiver sync index
 *	_prdmaSync[lsync] <-----	_prdmaSync[lsync]
 *	rsync = _prdmaSyncc[lsync]
 *	sending SYNC_MARKER
 *          --------------------------->_prdmaSync[lsync]
 *    MPI_Wait			MPI_Wait
 *       send done flag			while _prdmaSync[lsync] != SYNC_MARKER
 *
 *    MPI_Start			   MPI_Start
 *	state = RESTART
 *					_prdmaSync[lsync] = transaction id
 *	_prdmaync[lsync] <-----	_prdmaSync[lsync]
 * 
 */
#define DEBUG_ON	1
#define PRDMA_SIZE	13000
#define PRDMA_TRUNK_THR	(1024*1024)
#define PRDMA_N_NICS	4
#ifdef	USE_PRDMA_MSGSTAT
#define PRDMA_MSGSTAT_SIZE	1024
#endif	/* USE_PRDMA_MSGSTAT */
#include "prdma.h"

#ifdef	USE_PRDMA_MSGSTAT
typedef struct PrdmaMsgStat {
    size_t	size;
    int		count;
    struct PrdmaMsgStat	*next;
} PrdmaMsgStat;
#endif	/* USE_PRDMA_MSGSTAT */

int	_prdmaDebug;
int	_prdmaStat;
int	_prdmaNosync = 0;
int	_prdmaNoTrunk;
int	_prdmaVerbose;
int	_prdmaWaitTag;
int	_prdmaRdmaSize;
int	_prdmaMTU = 1024*1024;

static MPI_Comm		_prdmaInfoCom;
static MPI_Comm		_prdmaMemidCom;
static PrdmaDmaRegion	*_prdmaDmaregs[PRDMA_DMA_HTABSIZE];
static PrdmaReq		*_prdmaReqTable[PRDMA_REQ_HTABSIZE];
static int		_prdmaInitialized = 0;
static uint16_t		_prdmaRequid;
static int		_prdmaNumReq;	/* Number of on-the-fly requests */
static int		_prdmaMemid;
volatile uint32_t	 *_prdmaSync;	/* this entry is also used
					 * to pass the remote sync entry */
static uint32_t		_prdmaSyncConst[PRDMA_SYNC_CNSTSIZE];
static uint64_t		_prdmaDmaSyncConst;
static int		_prdmaSyncNumEntry;
static int		_prdmaMaxSync;
static int		_prdmaNprocs;
static int		_prdmaMyrank;
uint64_t		_prdmaDmaThisSync;
volatile uint64_t	*_prdmaRdmaSync;
static PrdmaReq		*_prdmaTagTab[PRDMA_TAG_MAX];
#ifdef	USE_PRDMA_MSGSTAT
static PrdmaMsgStat	_prdmaSendstat[PRDMA_MSGSTAT_SIZE];
static PrdmaMsgStat	_prdmaRecvstat[PRDMA_MSGSTAT_SIZE];
#endif	/* USE_PRDMA_MSGSTAT */

#define PRDMA_NIC_NPAT	4
static int _prdmaNICID[PRDMA_NIC_NPAT] = {
     FJMPI_RDMA_NIC0,FJMPI_RDMA_NIC1,FJMPI_RDMA_NIC2,FJMPI_RDMA_NIC3
};
static int	_prdmaDMAFlag[PRDMA_NIC_NPAT] = {
     FJMPI_RDMA_LOCAL_NIC0 | FJMPI_RDMA_REMOTE_NIC0 | FJMPI_RDMA_PATH0,
     FJMPI_RDMA_LOCAL_NIC1 | FJMPI_RDMA_REMOTE_NIC1 | FJMPI_RDMA_PATH0,
     FJMPI_RDMA_LOCAL_NIC2 | FJMPI_RDMA_REMOTE_NIC2 | FJMPI_RDMA_PATH0,
     FJMPI_RDMA_LOCAL_NIC3 | FJMPI_RDMA_REMOTE_NIC3 | FJMPI_RDMA_PATH0
};
static int	_prdmaDMAFent;
static PrdmaReq	*_PrdmaCQpoll();

void
_PrdmaPrintf(FILE *fp, const char *fmt, ...)
{
    va_list ap;
    char buf[2048];

    va_start(ap, fmt);
    vsprintf(buf, fmt, ap);
    va_end(ap);
    fprintf(fp, "[%d]: %s", _prdmaMyrank, buf);
    fflush(fp);
}

int
_PrdmaGetCommWorldRank(MPI_Comm mycomm, int myrank)
{
    MPI_Group	worldgrp, mygrp;
    int		worldrank;

    MPI_Comm_group(MPI_COMM_WORLD, &worldgrp);
    MPI_Comm_group(mycomm, &mygrp);

    MPI_Group_translate_ranks(mygrp, 1, &myrank, worldgrp, &worldrank);

    MPI_Group_free(&mygrp);
    MPI_Group_free(&worldgrp);

    return worldrank;
}


void
_prdmaErrorExit(int type)
{
    _PrdmaPrintf(stderr, "%03d ", type);
    switch (type) {
    case 1:
	_PrdmaPrintf(stderr, "No more space for request structuret\n");
	break;
    case 2:
	_PrdmaPrintf(stderr, "No more space for synchronization entry\n");
	break;
    case 3:
	_PrdmaPrintf(stderr, "RDMA communication error\n");
	break;
    case 10:
	_PrdmaPrintf(stderr, "No more space for ReserveRegion Management\n");
	break;
    case 11:
	_PrdmaPrintf(stderr, "No more space for ReserveRegion Management\n");
    default:
	_PrdmaPrintf(stderr, "Internal Error\n");
    }
    MPI_Abort(MPI_COMM_WORLD, -type);
}
#ifdef	USE_PRDMA_MSGSTAT

static void
_PrdmaStatMessage(PrdmaReq *top)
{
    int		dsize;
    size_t	transsize;
    int		hent;
    PrdmaMsgStat	*sp;

    MPI_Type_size(top->dtype, &dsize);
    transsize = dsize*top->count;
    hent = transsize % PRDMA_MSGSTAT_SIZE;
    for (sp = (top->type == PRDMA_RTYPE_SEND)
	     ? &_prdmaSendstat[hent] : &_prdmaRecvstat[hent];
	 sp; sp = sp->next) {
	if (sp->size == 0) {
	    sp->size = transsize;
	    sp->count++;
	    break;
	} else if (sp->size == transsize) {
	    sp->count++;
	    break;
	} else if (sp->next == 0) {
	    sp->next = (PrdmaMsgStat*) malloc(sizeof(PrdmaMsgStat));
	    memset(sp->next, 0, sizeof(PrdmaMsgStat));
	}
    }
}
#endif	/* USE_PRDMA_MSGSTAT */

static int
_PrdmaTagGet(PrdmaReq *pr)
{
    int		i;
    int		count = 0;

retry:
    count++;
    for (i = PRDMA_TAG_START; i < PRDMA_TAG_MAX; i++) {
	if (_prdmaTagTab[i] == 0) {
	    _prdmaTagTab[i] = pr;
	    return i;
	}
    }
    /* no more tag */
    _PrdmaCQpoll();
    if (count < 10000) {
	_prdmaWaitTag++;
	usleep(1);
	goto retry;
    }
    _PrdmaPrintf(stderr, "_PrdmaTagGet: no more tag\n");
    PMPI_Abort(MPI_COMM_WORLD, -1);
    return -1;
}

static void
_PrdmaTagFree(int ent)
{
    _prdmaTagTab[ent] = 0;
}

static PrdmaReq	*
_PrdmaTag2Req(int ent)
{
    return _prdmaTagTab[ent];
}

static int
_PrdmaGetmemid()
{
    ++_prdmaMemid;
    if (_prdmaMemid > PRDMA_MEMID_MAX) {
	/* no more memid is allocated */
	return -1;
    }
    return _prdmaMemid;
}

static int
_PrdmaReqHashKey(uint32_t key)
{
    key = key & (PRDMA_REQ_HTABSIZE - 1);
    return key;
}

static int
_PrdmaAddrHashKey(void *addr)
{
    int	key = ((uint64_t)addr >> 4) & 0xffffffff;
    key = key & (PRDMA_DMA_HTABSIZE - 1);
    return key;
}

static void
_PrdmaCheckRdmaSync(int peer)
{
    if (_prdmaRdmaSync[peer] == 0) {
	_prdmaRdmaSync[peer]
	    = FJMPI_Rdma_get_remote_addr(peer,PRDMA_MEMID_SYNC);
    }
}

static int
_PrdmaSyncGetEntry()
{
    static int	hint = 0;
    int		idx;

    /* _prdmaSyncNumEntry is updated if more room is found */
    if (_prdmaSyncNumEntry >= _prdmaMaxSync) {
	/* No more synchronization structure can be allocated */
	_prdmaErrorExit(2);
	return -1;
    }
    idx = hint;
    while (_prdmaSync[idx] != PRDMA_SYNC_NOTUSED) {
	idx = PRDMA_ROUND_INC(idx, _prdmaMaxSync);
    }
    _prdmaSync[idx] = PRDMA_SYNC_USED;
    _prdmaSyncNumEntry++;
    hint = PRDMA_ROUND_INC(idx, _prdmaMaxSync);
    return idx;
}

static void
_PrdmaSyncFreeEntry(int ent)
{
    _prdmaSync[ent] = PRDMA_SYNC_NOTUSED;
    --_prdmaSyncNumEntry;
}


static int
_PrdmaReqRegister(PrdmaReq *pr)
{
    uint16_t	uid;
    int		key;
    PrdmaReq	*pq, *opq;

    if (_prdmaNumReq > PRDMA_REQ_MAXREQ) {
	MPI_Abort(MPI_COMM_WORLD, -1);
	return -1;
    }
retry:
    PRDMA_REQUID_INC;
    uid = _prdmaRequid;
    key = _PrdmaReqHashKey(uid);
    pq = _prdmaReqTable[key];
    if (pq == NULL) {
	_prdmaReqTable[key] = pr;
    } else {
	/* checking if the uid is stil used */
	for (opq = pq; pq != NULL; opq = pq, pq = pq->next) {
	    if (pq->uid == uid) {
		/* still the uid has been used */
		goto retry;
	    }
	}
	opq->next = pr;
    }
    pr->uid = uid;
    pr->next = 0;
    return uid;
}

static void
_PrdmaReqUnregister(PrdmaReq *req)
{
    uint16_t	uid;
    int		key;
    PrdmaReq	*pq, *opq;

    uid = req->uid;
    key = _PrdmaReqHashKey(uid);
    pq = _prdmaReqTable[key];
    if (pq == NULL) {
	/* internal error !!! */
	/* have to handle this error  */
	return;
    }
    for (opq = pq; pq != NULL; opq = pq, pq = pq->next) {
	if (pq->uid == uid) goto find;
    }
    /* internal error !!! */
    return;
find:
    if (pq == _prdmaReqTable[key]) {
	_prdmaReqTable[key] = NULL;
    } else {
	opq->next = pq->next;
    }
    /* decrement ing the number of uids */
    --_prdmaNumReq;
}

static PrdmaReq	*
_PrdmaReqAlloc(PrdmaRtype type)
{
    PrdmaReq	*pq;

    pq = malloc(sizeof(PrdmaReq));
    if (pq == 0) {
	_prdmaErrorExit(1);
	return 0; /* never here */
    }
    memset(pq, 0, sizeof(PrdmaReq));
    pq->type = type;
    pq->state = PRDMA_RSTATE_INIT;
    pq->uid = _PrdmaReqRegister(pq);
    return pq;
}

static void
_PrdmaReqfree(PrdmaReq *top)
{
    PrdmaReq	*pq, *npq;

    for (pq = top; pq != NULL; pq = npq) {
	_PrdmaReqUnregister(pq);
	_PrdmaSyncFreeEntry(pq->lsync);
	npq = pq->trunks;
	free(pq);
    }
}

static PrdmaReq *
_PrdmaReqFind(uint64_t id)
{
    int		key;
    uint16_t	uid;
    PrdmaReq	*pq;

    /* This is only applicable for OpenMPI */
    if (id > 0xffff) { /* Original Request ID */
	return NULL;
    }
    uid = id;
    key = _PrdmaReqHashKey(uid);
    pq = _prdmaReqTable[key];
    while (pq) {
	if (pq->uid == uid) {
	    return pq;
	}
	pq = pq->next;
    }
    return NULL;
}


static int
_PrdmaReserveRegion(uint64_t *dmaaddr, void *addr, int size)
{
    PrdmaDmaRegion	*pdr;
    int			key;
    int			ent;
    int			memid;

    ent = key = _PrdmaAddrHashKey(addr);
    do {
	pdr = _prdmaDmaregs[ent];
	if (pdr == 0) goto newent;
	if (pdr->start == addr) {
	    if (size <= pdr->size) {
		goto find;
	    } else { /* Must be enlarged */
		_PrdmaPrintf(stderr, "_PrdmaReserveRegion: Enlarging\n");
		goto newent;
	    }
	}
	ent = PRDMA_ROUND_INC(ent, PRDMA_DMA_HTABSIZE);
    } while (ent != key);
    /* runnout: no more region can be allocated */
    _prdmaErrorExit(11);
    return -1; /* never return */
newent:
    pdr = (PrdmaDmaRegion*) malloc(sizeof(PrdmaDmaRegion));
    if (pdr == NULL) {
	_prdmaErrorExit(10);
	return -1; /* never return */
    }
    if (_prdmaDmaregs[ent]) { /* old entry is pushed */
	pdr->next = _prdmaDmaregs[ent];
	_prdmaDmaregs[ent] = pdr;
    }
    memid = _PrdmaGetmemid();
    pdr->start = addr;
    pdr->memid = memid;
    pdr->size = size;
    pdr->dmaaddr = FJMPI_Rdma_reg_mem(memid, (char *)addr, size);
    if (pdr->dmaaddr == FJMPI_RDMA_ERROR) {
	_PrdmaPrintf(stderr, "FJMPI_Rdma_reg_mem failed\n");
	MPI_Abort(MPI_COMM_WORLD, -1);
	return -1;
    }
find:
    *dmaaddr = pdr->dmaaddr;
    return pdr->memid;
}

int
PrdmaReserveRegion(void *addr, int size)
{
    int         lbid;
    uint64_t    lbaddr;

    lbid = _PrdmaReserveRegion(&lbaddr, addr, size);
    return lbid;
}

struct PrdmaOptions {
    char	*sym;
    int		*var;
};

static struct PrdmaOptions _poptions[] = {
    { "PRDMA_DEBUG", &_prdmaDebug },
    { "PRDMA_NOSYNC", &_prdmaNosync },
    { "PRDMA_NOTRUNK", &_prdmaNoTrunk },
    { "PRDMA_VERBOSE", &_prdmaVerbose },
    { "PRDMA_STATISTIC", &_prdmaStat },
    { "PRDMA_RDMASIZE", &_prdmaRdmaSize },
    { 0, 0 }
};

static void
_PrdmaOptions()
{
    struct PrdmaOptions	*po;
    char		*cp;

    for (po = _poptions; po->sym; po++) {
	cp = getenv(po->sym);
	if (cp) {
	    *po->var = atoi(cp);
	}
    }
    if (_prdmaVerbose && _prdmaMyrank == 0) {
	for (po = _poptions; po->sym; po++) {
	    if (*po->var) {
		_PrdmaPrintf(stderr, "%s is %d\n", po->sym, *po->var);
	    } else {
		_PrdmaPrintf(stderr, "%s is OFF\n", po->sym);
	    }
	}
    }
}

static void
_PrdmaFinalize()
{
    int		mintime, maxtime;

    if (_prdmaInitialized == 0) return;
    if (_prdmaStat) {
	MPI_Reduce((void*)&_prdmaWaitTag, (void*)&mintime, 1, MPI_INT,
		   MPI_MIN, 0, MPI_COMM_WORLD);
	MPI_Reduce((void*)&_prdmaWaitTag, (void*)&maxtime, 1, MPI_INT,
		   MPI_MAX, 0, MPI_COMM_WORLD);
	if (_prdmaMyrank == 0) {
	    fprintf(stderr, "**********************************************\n");
	    fprintf(stderr, "Max total waiting time to obtain tags: %d usec\n",
		    maxtime);
	    fprintf(stderr, "Min total waiting time to obtain tags: %d usec\n",
		    mintime);
	    fprintf(stderr, "**********************************************\n");
	}
#ifdef	USE_PRDMA_MSGSTAT
	if (_prdmaMyrank < 8) {
	    PrdmaMsgStat	*pm;
	    int			i;
	    for (i = 0; i < PRDMA_MSGSTAT_SIZE; i++) {
		for (pm = &_prdmaSendstat[i]; pm; pm = pm->next) {
		    if (pm->size > 0) {
			fprintf(stderr, "[%d] send(%d) count(%d)\n",
				_prdmaMyrank, pm->size, pm->count);
		    }
		}
	    }
	    fflush(stderr);
	}
#endif	/* USE_PRDMA_MSGSTAT */
    }

    FJMPI_Rdma_finalize();
    _prdmaInitialized = 0;
}

static void
_PrdmaInit()
{
    int		size;

    if (_prdmaInitialized == 1) return;
    MPI_Comm_size(MPI_COMM_WORLD, &_prdmaNprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &_prdmaMyrank);
    /* Used for exhange data between sender and receiver */
    MPI_Comm_dup(MPI_COMM_WORLD, &_prdmaInfoCom);
    MPI_Comm_dup(MPI_COMM_WORLD, &_prdmaMemidCom);
    FJMPI_Rdma_init();
    /* Synchronization structure is initialized */
    _prdmaMaxSync = _prdmaNprocs * PRDMA_NSYNC_PERPROC;
    size = sizeof(uint32_t)*_prdmaMaxSync;
    _prdmaSync = malloc(size);
    _prdmaRdmaSync= malloc(sizeof(uint64_t)*_prdmaNprocs);
    if (_prdmaSync == 0 || _prdmaRdmaSync == 0) {
	MPI_Abort(MPI_COMM_WORLD, -1);
	return;
    }
    memset((void*) _prdmaSync, PRDMA_SYNC_NOTUSED, size);
    _prdmaSyncNumEntry = 0;
    _prdmaDmaThisSync = FJMPI_Rdma_reg_mem(PRDMA_MEMID_SYNC,
					   (char*) _prdmaSync, size);
    /**/
    _prdmaSyncConst[PRDMA_SYNC_CNSTMARKER] = PRDMA_SYNC_MARKER;
    _prdmaSyncConst[PRDMA_SYNC_CNSTFF_0] = PRDMA_SYNC_USED | PRDMA_SYNC_EVEN;
    _prdmaSyncConst[PRDMA_SYNC_CNSTFF_1] = PRDMA_SYNC_USED | PRDMA_SYNC_ODD;
    _prdmaDmaSyncConst = FJMPI_Rdma_reg_mem(PRDMA_MEMID_SCONST,
					     (char*) &_prdmaSyncConst,
					     sizeof(_prdmaSyncConst));
    memset((void*) _prdmaRdmaSync, 0, sizeof(uint64_t)*_prdmaNprocs);
    /* misc initializations */
    _prdmaMemid = PRDMA_MEMID_START;
    _prdmaSyncNumEntry = 0;
    memset(_prdmaDmaregs, 0, sizeof(_prdmaDmaregs));
    _prdmaRequid = PRDMA_REQ_STARTUID;
    _prdmaNumReq = 0;
    memset(_prdmaTagTab, 0, sizeof(_prdmaTagTab));
    _prdmaRdmaSize = PRDMA_SIZE;
    _PrdmaOptions();

    atexit(_PrdmaFinalize);
    _prdmaInitialized = 1;

}

PrdmaReq	*
_PrdmaCQpoll()
{
    int				i;
    int				cc;
    struct FJMPI_Rdma_cq	cq;
    PrdmaReq	*preq = 0;

    for (i = 0; i < PRDMA_NIC_NPAT; i++) {
	cc = FJMPI_Rdma_poll_cq(_prdmaNICID[i], &cq);
	switch (cc) {
	case FJMPI_RDMA_NOTICE:
	    preq = _PrdmaTag2Req(cq.tag);
	    if (preq == 0) break;
	    if (preq->type == PRDMA_RTYPE_SEND) {
		if (preq->state == PRDMA_RSTATE_START) {
		    preq->state = PRDMA_RSTATE_SENDER_SENT_DATA;
		} else if (preq->state == PRDMA_RSTATE_SENDER_SENT_DATA) {
		    preq->state = PRDMA_RSTATE_SENDER_SEND_DONE;
		} else {
		    /* unknown state */
		}
	    } else {
		/* receiver has sent sync entry to sender */
		if (preq->state == PRDMA_RSTATE_START) {
		    preq->state = PRDMA_RSTATE_RECEIVER_SYNC_SENT;
		}
	    }
	    _PrdmaTagFree(cq.tag);
	    break;
	case FJMPI_RDMA_REMOTE_NOTICE:
	case 0:
	    break;
	}
    }
    return preq;
}

int
_PrdmaTest(PrdmaReq *preq, int wait)
{
    int		cc = 0;

    if (preq->state == PRDMA_RSTATE_DONE) {
	return 1;
    }
retry:
    _PrdmaCQpoll();
    switch (preq->type) {
    case PRDMA_RTYPE_SEND:
	if (preq->state != PRDMA_RSTATE_SENDER_SEND_DONE) goto retry;
	/* send done */
	preq->state = PRDMA_RSTATE_DONE;
	cc = 1;
	break;
    case PRDMA_RTYPE_RECV:
	if (preq->state != PRDMA_RSTATE_RECEIVER_SYNC_SENT) {
	    if(wait == 0) return 0;
	    goto retry;
	}
	if (wait && _prdmaSync[preq->lsync] != PRDMA_SYNC_MARKER) {
	    /* now waiting  */
	    do {
		/* we have to change */
		usleep(1);
	    } while (_prdmaSync[preq->lsync] != PRDMA_SYNC_MARKER);
	}
	if (_prdmaSync[preq->lsync] == PRDMA_SYNC_MARKER) {
	    /* reset the variable */
	    _prdmaSync[preq->lsync] = PRDMA_SYNC_USED;
	    preq->state = PRDMA_RSTATE_DONE;
	    cc = 1;
	}
	break;
    default:
	break;
    }
    return cc;
}



int
_PrdmaMultiTest0(MPI_Request *req, PrdmaReq *top, int *flag)
{
    PrdmaReq	*preq;
    int		nent;
    int		cc = 0;
    int		found = 0;

    for (preq = top, nent = 0; preq != NULL; preq = preq->trunks, nent++) {
	if (preq->state == PRDMA_RSTATE_ERROR) {
	    /* It has been the error state */
	    _PrdmaReqfree(preq);
	    *req = MPI_REQUEST_NULL;
	    cc = MPI_ERR_INTERN;
	    _prdmaErrorExit(3);
	    continue;
	}
	/* PRDMA transaction */
	if (preq->state != PRDMA_RSTATE_DONE) {
	    if (_PrdmaTest(preq, 0) == 0) continue;  /* still progress */
	}
	/* found */
	found++;
    }
    if (nent == found) {
	*flag = 1;
    } else {
	*flag = 0;
    }
    return cc;
}

int
_PrdmaMultiTest(int wait, int cond, int *indices, int *fnum,
		int count, MPI_Request *reqs, MPI_Status *stats)
{
    PrdmaReq	*preq;
    int		cc = MPI_SUCCESS;
    int		found;
    int		i;

retry:
    found = 0;
    for (i = 0; i < count; i++) {
	int	flag = 0;

	preq = _PrdmaReqFind((uint64_t) reqs[i]);
	if (preq == 0) {/* Regular Request */
	    cc = PMPI_Test(&reqs[i], &flag, stats);
	} else {
	    cc = _PrdmaMultiTest0(&reqs[i], preq, &flag);
	}
	if (flag == 0) continue;
	/* one entry found */
	found++;
	switch (cond) {
	case PRDMA_FIND_ANY: /* Waitany and Testany */
	    indices[0] = i; /* in case of PRDMA_FIND_ANY, one entry */
	    goto ret;
	case PRDMA_FIND_ALL: /* Waitall and Testall */
	    break;
	case PRDMA_FIND_SOME: /* Waitsome and Testsome */
	    indices[i] = 1;
	    break;
	}
    }
    if (wait) { /* MPI_Testany, MPI_Testsome, MPI_Testall */
	if ((cond == PRDMA_FIND_ALL && found == count)
	    || (cond == PRDMA_FIND_SOME && found > 0)) goto ret;
	/* Conidition of keeping polling
	   (PRDMA_FIND_ALL && found < count) || (cond == PRDMA_FIND_SOME && found == 0)
	   || (PRDMA_FIND_ANY && found == 0 */
	usleep(1);
	goto retry;
    }
ret:
    if (fnum != 0) *fnum = found;
    return cc;
}


static int
_PrdmaOneCount(int count, int dsize, size_t transsize)
{
    int		onecnt;

    if (transsize <= _prdmaRdmaSize) {
	return 0;
    }
    /*onecnt = _prdmaMTU/dsize;*/
    if (_prdmaNoTrunk) {
	onecnt = count;
    } else {
	if (transsize < PRDMA_TRUNK_THR) {
	    onecnt = count;
	} else {
	    onecnt = count/PRDMA_N_NICS;
	}
    }
    return onecnt;
}


PrdmaReq	*
_PrdmaReqCommonSetup(PrdmaRtype type, int worldrank, size_t transsize,
		     int transcount, int lbid, uint64_t	lbaddr,
		     void *buf, int count,
		     MPI_Datatype datatype, int peer, int tag,
		     MPI_Comm comm, MPI_Request *request)
{
    PrdmaReq	*preq;
    int		lsync;

    lsync = _PrdmaSyncGetEntry();
    if (lsync < 0) { /* never here */
	return 0;
    }
    preq = _PrdmaReqAlloc(type);
    if (preq == NULL) { /* never here */
	return 0;
    }
    /* checking if the synchronization structure has been obtained */
    _PrdmaCheckRdmaSync(worldrank);
    /*
     * All required resources have been allocated.
     */
    /* MPI arguments are stored */
    PRDMA_SET_REQ(preq, buf, count, datatype, peer, tag, comm, request);
    preq->size = transsize;	/* transfer size in byte */
    preq->worldrank = worldrank;/* peer rank in COMM_WORLD_COMM */
    preq->transcnt = transcount;/* actual count in this request */
    preq->lbid = lbid;		/* memid of local comm. buffer */
    preq->lbaddr = lbaddr;	/* dma address of local comm. buffer */
    preq->lsync = lsync;	/* synchronization entry (index) */
    preq->raddr = (uint64_t) -1;/* marker */
    preq->transff = 0;		/* for synchronization */
    preq->trunks = 0;		/* for divided data transfer if needed */
    return preq;
}

static PrdmaReq *
_PrdmaSendInit0(int worlddest, size_t transsize, int transcount,
		int lbid, uint64_t lbaddr,
		void *buf, int count, MPI_Datatype datatype,
		int dest, int tag, MPI_Comm comm, MPI_Request *request)
{
    PrdmaReq	*preq;
    int		flag = 0;
    MPI_Status	stat;

    preq = _PrdmaReqCommonSetup(PRDMA_RTYPE_SEND,
				worlddest, transsize, transcount, lbid, lbaddr,
				buf, count, datatype,
				dest, tag, comm, request);
    if (preq == NULL) {
	return 0;
    }
    /* Needs remote memid to get the remote DMA address */
    MPI_Irecv(&preq->rinfo, sizeof(struct recvinfo), MPI_BYTE, dest,
	      preq->tag, _prdmaInfoCom, &preq->negreq);
    /* Send the DMA address of synch entry to dest. */
    MPI_Bsend(&preq->lsync, sizeof(int),
	      MPI_BYTE, dest, preq->tag, _prdmaMemidCom);
    /* now testing the previous request to get the remote memid */
    MPI_Test(&preq->negreq, &flag, &stat);
    if (flag) { 
	/* The remote memid has been received */
	preq->state = PRDMA_RSTATE_PREPARED;
    } else {
	preq->state = PRDMA_RSTATE_WAITRMEMID;
    }
    return preq;
}



static int
_PrdmaSendInit(int *tover, void *buf, int count, MPI_Datatype datatype,
	       int dest, int tag, MPI_Comm comm, MPI_Request *req)
{
    PrdmaReq	*top, *next, *preq;
    size_t	transsize;
    uint64_t	lbaddr;
    int		lbid, worlddest, result, dsize;
    int		onecnt, rest, tcnt;

    /* NEEDS Checking whether or not basic type !!!! */
    /* Checking data transfer size */
    MPI_Type_size(datatype, &dsize);
    transsize = dsize*count;
    onecnt = _PrdmaOneCount(count, dsize, transsize);
    if (onecnt == 0) {
	/* original communication is taken in case of small message size */
	goto notake;
    }
    /* rank in MPI_COMM_WORLD */
    MPI_Comm_compare(MPI_COMM_WORLD, comm, &result);
    if (result == MPI_IDENT || result == MPI_CONGRUENT) {
	worlddest = dest;
    } else {
	worlddest = _PrdmaGetCommWorldRank(comm, dest);
    }
    lbid = _PrdmaReserveRegion(&lbaddr, buf, transsize);
    /*
     * Now constructing chunk of messages
     */
    top = next = 0;
    rest = count;
    tcnt = (rest >= onecnt) ? onecnt : rest;
    transsize = dsize*tcnt;
    top = next = _PrdmaSendInit0(worlddest, transsize, tcnt, lbid, lbaddr,
				 buf, tcnt, datatype, dest, tag, comm, req);
    lbaddr += transsize; buf = (void*)(((char*)buf) + transsize);
    rest -= tcnt;
    while (rest > 0) {
	tcnt = (rest >= onecnt) ? onecnt : rest;
	transsize = dsize*tcnt;
	preq = _PrdmaSendInit0(worlddest, transsize, tcnt, lbid, lbaddr,
			       buf, tcnt, datatype, dest, tag, comm, req);
	if (preq == NULL) goto notake; /* never in this case */
	lbaddr += transsize; buf = (void*)(((char*)buf) + transsize);
	rest -= tcnt;
	next->trunks = preq;
	next = preq;
    }
    *tover = 1;
    *req = (MPI_Request) top->uid;
    return MPI_SUCCESS;
notake:
    *tover = 0;
    return MPI_SUCCESS;
}


/*
 * MPI functions replacement
 */

int
MPI_Init(int *argc, char ***argv)
{
    int		cc;

    cc = PMPI_Init(argc, argv);
    if (cc != MPI_SUCCESS) return cc;
    _PrdmaInit();
    return MPI_SUCCESS;
}

int
MPI_Finalize()
{
    int		cc;

    _PrdmaFinalize();
    cc = PMPI_Finalize();
    return cc;
}

int
MPI_Send_init(void *buf, int count, MPI_Datatype datatype,
	      int dest, int tag, MPI_Comm comm,
	      MPI_Request *request)
{
    int		tover;
    int		cc;

    cc = _PrdmaSendInit(&tover,
			buf, count, datatype, dest, tag, comm, request);
    if (tover == 0) {
	cc = PMPI_Send_init(buf, count, datatype, dest, tag, comm, request);
    }
    return cc;
}

static PrdmaReq *
_PrdmaRecvInit0(int worlddest, size_t transsize, int transcount, int lbid,
		uint64_t lbaddr,
		void *buf, int count, MPI_Datatype datatype,
		int source, int tag, MPI_Comm comm, MPI_Request *request)
{
    struct recvinfo	info;
    PrdmaReq	*preq;
    int		flag = 0;
    MPI_Status	stat;

    preq = _PrdmaReqCommonSetup(PRDMA_RTYPE_RECV,
				worlddest, transsize, transcount, lbid, lbaddr,
				buf, count, datatype,
				source, tag, comm, request);
    if (preq == NULL) {
	return NULL;
    }
    /*
     * memid of buf is sent to the sender
     */
    info._rbid = preq->lbid;
    info._rsync = preq->lsync;
    MPI_Bsend(&info, sizeof(struct recvinfo), MPI_BYTE, source,
	      preq->tag, _prdmaInfoCom);
    /*
     * Needs memid of the synchronization variable in the sender
     */
    MPI_Irecv(&preq->rsync, sizeof(int), MPI_BYTE, source,
	      preq->tag, _prdmaMemidCom, &preq->negreq);
    MPI_Test(&preq->negreq, &flag, &stat);
    if (flag) {
	/* The memid of the synchronization variable has been received */
	preq->state = PRDMA_RSTATE_PREPARED;
    } else {
	preq->state = PRDMA_RSTATE_WAITRMEMID;
    }
    return preq;
}

int
MPI_Recv_init(void *buf, int count, MPI_Datatype datatype,
	      int source, int tag, MPI_Comm comm,
	      MPI_Request *request)
{
    PrdmaReq	*top, *next, *preq;
    size_t	transsize;
    uint64_t	lbaddr;
    int		lbid, worlddest, result, dsize;
    int		onecnt, rest, tcnt;
    int		cc;

     /* Checking data transfer size */
    MPI_Type_size(datatype, &dsize);
    transsize = dsize*count;
    onecnt = _PrdmaOneCount(count, dsize, transsize);
    if (onecnt == 0) {
	goto notake;
    }
    /* rank in MPI_COMM_WORLD */
    MPI_Comm_compare(MPI_COMM_WORLD, comm, &result);
    if (result == MPI_IDENT || result == MPI_CONGRUENT) {
	worlddest = source;
    } else {
	worlddest = _PrdmaGetCommWorldRank(comm, source);
    }
    lbid = _PrdmaReserveRegion(&lbaddr, buf, transsize);
    /*
     * Now constructing chunk of messages
     */
    top = next = 0;
    rest = count;
    tcnt = (rest >= onecnt) ? onecnt : rest;
    transsize = dsize*tcnt;
    top = next = _PrdmaRecvInit0(worlddest, transsize, tcnt, lbid, lbaddr,
				 buf, tcnt, datatype,
				 source, tag, comm, request);
    lbaddr += transsize; buf = (void*)(((char*)buf) + transsize);
    rest -= tcnt;
    while (rest > 0) {
	tcnt = (rest >= onecnt) ? onecnt : rest;
	transsize = dsize*tcnt;
	preq = _PrdmaRecvInit0(worlddest, transsize, tcnt, lbid, lbaddr,
			       buf, tcnt, datatype,
			       source, tag, comm, request);
	if (preq == NULL) goto notake;  /* never in this case */
	lbaddr += transsize; buf = (void*)(((char*)buf) + transsize);
	next->trunks = preq;
	next = preq;
	rest -= tcnt;
    }
    *request = (MPI_Request) top->uid;
    return MPI_SUCCESS;
notake:
    cc = PMPI_Recv_init(buf, count, datatype, source, tag, comm, request);
    return cc;
}

int
_PrdmaStart0(PrdmaReq *preq)
{
    int		cc1, cc2;
    int		flag;
    int		idx;
    uint32_t	transid;
    uint64_t	raddr;
    int		tag;
    MPI_Status	stat;

    if (preq->state == PRDMA_RSTATE_WAITRMEMID) {
	/*
	 * In case of sender, local buf remote id(rbid) has not arrive.
	 * In case of receiver, snch variable index(rsync) has not arrive.
	 */
	PMPI_Wait(&preq->negreq, &stat);
	preq->state = PRDMA_RSTATE_PREPARED;
    } else if (preq->state == PRDMA_RSTATE_DONE) {
	/* restart */
	preq->state = PRDMA_RSTATE_RESTART;
    } else if (preq->state != PRDMA_RSTATE_PREPARED) {
	_PrdmaPrintf(stderr, "MPI_Start is invoked before MPI_Wait\n");
	MPI_Abort(MPI_COMM_WORLD, -1);
	return MPI_ERR_INTERN;
    }
/*
    flag = FJMPI_RDMA_LOCAL_NIC0 | FJMPI_RDMA_REMOTE_NIC0 | FJMPI_RDMA_PATH0;
*/
    flag = _prdmaDMAFlag[_prdmaDMAFent];
    _prdmaDMAFent = (_prdmaDMAFent + 1) % PRDMA_NIC_NPAT;
    switch (preq->type) {
    case PRDMA_RTYPE_SEND:
	/* remote address */
	idx = preq->lsync;
	if (preq->raddr == (uint64_t) -1) {
	    preq->raddr = FJMPI_Rdma_get_remote_addr(preq->peer, preq->rbid);
	}
	preq->transff ^= PRDMA_SYNC_FLIP;
	if (_prdmaNosync == 0) {
	    /* Synchronization */
	    transid = _prdmaSyncConst[preq->transff + PRDMA_SYNC_CNSTFF_0];
	    while (_prdmaSync[idx] != transid) {
		usleep(1);
	    }
	}
	/* start DMA */
	tag = _PrdmaTagGet(preq);
	cc1 = FJMPI_Rdma_put(preq->peer, tag,
			     preq->raddr, preq->lbaddr,
			     preq->size, flag);
	/*
	 * Make sure the ordering of the above transaction and the following
	 * transaction
	 */
	tag = _PrdmaTagGet(preq);
	cc2 = FJMPI_Rdma_put(preq->peer, tag,
		_prdmaRdmaSync[preq->peer] + preq->rsync*sizeof(uint32_t),
	        _prdmaDmaSyncConst + sizeof(uint32_t)*PRDMA_SYNC_CNSTMARKER,
		sizeof(int), flag);
	if (cc1 == 0 && cc2 == 0) {
	    preq->state = PRDMA_RSTATE_START;
	} else {
	    _PrdmaPrintf(stderr,
		    "FJMPI_Rdma_put error in the sender side (%d, %d)\n",
		    cc1, cc2);
	    preq->state = PRDMA_RSTATE_ERROR;
	}
	break;
    case PRDMA_RTYPE_RECV:
	preq->transff ^= PRDMA_SYNC_FLIP;
	if (_prdmaNosync == 1) { /* no synchronization */
	    preq->state = PRDMA_RSTATE_RECEIVER_SYNC_SENT;
	} else {
	    /* Synchronization */
	    tag = _PrdmaTagGet(preq);
	    raddr = _prdmaDmaSyncConst
		+ (preq->transff + PRDMA_SYNC_CNSTFF_0)*sizeof(uint32_t);
	    cc1 = FJMPI_Rdma_put(preq->peer, tag,
		 _prdmaRdmaSync[preq->peer] + preq->rsync*sizeof(uint32_t),
				 raddr,  sizeof(int), flag);
	    if (cc1 == 0) {
		preq->state = PRDMA_RSTATE_START;
	    } else {
		_PrdmaPrintf(stderr, "FJMPI_Rdma_put error in the receiver side\n");
		preq->state = PRDMA_RSTATE_ERROR;
	    }
	}
	break;
    default:
	_PrdmaPrintf(stderr, "Internal error\n");
    }
    return MPI_SUCCESS;
}

int
_PrdmaStart(PrdmaReq *top)
{
    PrdmaReq	*preq;
    int		cc = MPI_SUCCESS;

#ifdef	USE_PRDMA_MSGSTAT
    if (_prdmaStat) {
	_PrdmaStatMessage(top);
    }
#endif	/* USE_PRDMA_MSGSTAT */
    for (preq = top; preq != NULL; preq = preq->trunks) {
	cc = _PrdmaStart0(preq);
    }
    return cc;
}

int
MPI_Start(MPI_Request *request)
{
    uint16_t	reqid;
    PrdmaReq	*preq;
    int		cc;

    reqid = (uint16_t) ((uint64_t)*request) & 0xffff;
    preq = _PrdmaReqFind(reqid);
    if (preq == 0) {/* Regular Request */
	cc = PMPI_Start(request);
    } else {
	cc = _PrdmaStart(preq);
    }
    return cc;
}

int
MPI_Startall(int count, MPI_Request *reqs)
{
    uint16_t	reqid;
    PrdmaReq	*preq;
    int		i, ret;
    int		cc = MPI_SUCCESS;

    for (i = 0; i < count; i++) {
	reqid = (uint16_t) ((uint64_t)reqs[i]) & 0xffff;
	preq = _PrdmaReqFind(reqid);
	if (preq == 0) { /* Regular Request */
	    ret = PMPI_Start(&reqs[i]);
	} else {
	    ret = _PrdmaStart(preq);
	}
	if (ret != MPI_SUCCESS) cc = ret;
    }
    return cc;
}

/*
 * MPI_Status must be setup !!!
 */
int
MPI_Wait(MPI_Request *request, MPI_Status *status)
{
    PrdmaReq	*preq;
    int		flag;
    int		cc;

    preq = _PrdmaReqFind((uint64_t) *request);
    if (preq == 0) {
	/* Regular Request */
	cc = PMPI_Wait(request, status);
	return cc;
    }
    /* wait */
    flag = 0;
    do {
	_PrdmaMultiTest0(request, preq, &flag);
    } while (flag == 0);
    /*
     *¡¡Because this is a persistent communication, the internal structure,
     *  PrdmaReq, must be hold.
     */
    return MPI_SUCCESS;
}

int
MPI_Test(MPI_Request *request, int *flag, MPI_Status *status)
{
    int		cc;
    PrdmaReq	*preq;

    preq = _PrdmaReqFind((uint64_t) *request);
    if (preq == 0) {
	/* Regular Request */
	cc = PMPI_Test(request, flag, status);
	return cc;
    }
    if (preq->state == PRDMA_RSTATE_ERROR) {
	/* It has been the error state */
	_PrdmaReqfree(preq);
	*request = MPI_REQUEST_NULL;
	return MPI_ERR_INTERN;
    }
    /* test */
    _PrdmaMultiTest0(request, preq, flag);
    return MPI_SUCCESS;
}

int
MPI_Request_free(MPI_Request *request)
{
    PrdmaReq	*preq;
    int		cc;

    preq = _PrdmaReqFind((uint64_t) *request);
    if (preq == 0) {
	/* Regular Request */
	cc = PMPI_Request_free(request);
	return cc;
    }
    /* Free the internal stcuture of persistent communication */
    _PrdmaReqfree(preq);
    *request = MPI_REQUEST_NULL;
    
    return MPI_SUCCESS;
}


int
MPI_Waitany(int count, MPI_Request *reqs,
	    int *index, MPI_Status *stats)
{
    int		cc;

    cc = _PrdmaMultiTest(1, PRDMA_FIND_ANY, index, 0, count, reqs, stats);
    return cc;
}

int
MPI_Testany(int count, MPI_Request *reqs,
	    int *index, int *flag, MPI_Status *stats)
{
    int		cc;
    cc = _PrdmaMultiTest(0, PRDMA_FIND_ANY, index, flag, count, reqs, stats);
    return cc;
}

int
MPI_Waitall(int count, MPI_Request *reqs, MPI_Status *stats)
{
    int		cc;
    int		fnum;

    cc = _PrdmaMultiTest(1, PRDMA_FIND_ALL, 0, &fnum, count, reqs, stats);
    return cc;
}

int
MPI_Testall(int count, MPI_Request *reqs,int *flag, MPI_Status *stats)
{
    int		cc;
    int		fnum;

    cc = _PrdmaMultiTest(0, PRDMA_FIND_ALL, 0, &fnum, count, reqs, stats);
    if (fnum == count) *flag = 1;
    else *flag = 0;
    return cc;
}

int
MPI_Waitsome(int incount, MPI_Request *reqs,
	     int *outcount, int *indices, MPI_Status *stats)
{
    int		cc;
    cc = _PrdmaMultiTest(1, PRDMA_FIND_SOME, indices, outcount,
			 incount, reqs, stats);
    return cc;
}

int
MPI_Testsome(int count, MPI_Request *reqs,
	     int *outcount, int *indices, MPI_Status *stats)
{
    int		cc;
    cc = _PrdmaMultiTest(0, PRDMA_FIND_SOME, indices, outcount,
			 count, reqs, stats);
    return cc;
}

int
MPI_Bsend_init(void *buf, int count, MPI_Datatype datatype,
	       int dest, int tag, MPI_Comm comm, MPI_Request *request)
{
    int		tover;
    int		cc;
    cc = _PrdmaSendInit(&tover,
			buf, count, datatype, dest, tag, comm, request);
    if (tover == 0) {
	cc = PMPI_Bsend_init(buf, count, datatype, dest, tag, comm, request);
    }
    return cc;
}

int
MPI_Ssend_init(void *buf, int count, MPI_Datatype datatype,
	       int dest, int tag, MPI_Comm comm, MPI_Request *request)
{
    int		tover;
    int		cc;
    cc = _PrdmaSendInit(&tover,	buf, count, datatype, dest, tag, comm, request);
    if (tover == 0) {
	cc = PMPI_Ssend_init(buf, count, datatype, dest, tag, comm, request);
    }
    return cc;
}

int
MPI_Rsend_init(void *buf, int count, MPI_Datatype datatype,
	       int dest, int tag, MPI_Comm comm, MPI_Request *request)
{
    int		tover;
    int		cc;
    cc = _PrdmaSendInit(&tover,	buf, count, datatype, dest, tag, comm, request);
    if (tover == 0) {
	cc = PMPI_Rsend_init(buf, count, datatype, dest, tag, comm, request);
    }
    return cc;
}

MPI_Fint
MPI_Request_c2f(MPI_Request request)
{
    uint16_t	reqid;
    PrdmaReq	*preq;
    MPI_Fint	val;

    reqid = (uint16_t) ((uint64_t)request) & 0xffff;
    preq = _PrdmaReqFind(reqid);
    if (preq == 0) {
	val = PMPI_Request_c2f(request);
	return val;
    } else {
	return (MPI_Fint) reqid;
    }
}

MPI_Request MPI_Request_f2c(MPI_Fint request)
{
    uint16_t	reqid;
    PrdmaReq	*preq;

    reqid = (uint16_t) ((uint64_t)request) & 0xffff;
    preq = _PrdmaReqFind(reqid);
    if (preq == 0) {
	return PMPI_Request_f2c(request);
    } else {
	return (MPI_Request) request;
    }
}

