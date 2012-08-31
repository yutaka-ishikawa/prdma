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

/* interconnect nic selection */
#define MOD_PRDMA_NIC_SEL
/* #define MOD_PRDMA_NIC_SEL_CD00 */
/* #define MOD_PRDMA_NIC_SEL_CD01 */
/* #define MOD_PRDMA_NIC_SEL_CD02 */
/* #define MOD_PRDMA_NIC_SEL_CD03 */
#define MOD_PRDMA_NIC_SEL_CD04
/* determine the order of nic usage */
#define MOD_PRDMA_NIC_ORD
#define MOD_PRDMA_NIC_ORD_BYTYPE
/* multi-request busy loop for synchronization */
#define MOD_PRDMA_SYN_MBL
/* busy loop for mpi_waitall() */
#define MOD_PRDMA_BSY_WAIT
/* extended get-tag */
#define MOD_PRDMA_TAG_GET
/* #define MOD_PRDMA_TAG_GET_CD00 */
/* #define MOD_PRDMA_TAG_GET_CD01 */
#define MOD_PRDMA_TAG_GET_CD02
/* MPI_Test() with no wait */
#define MOD_PRDMA_TST_NWT
/* light-weight and high precision prdma-protocol trace */
#define MOD_PRDMA_LHP_TRC
#define MOD_PRDMA_LHP_TRC_TIMESYNC
#define MOD_PRDMA_LHP_TRC_CD00
/* fix of MPI_Request_f2c() */
#define MOD_PRDMA_F2C_FIX
#define MOD_PRDMA_F2C_FIX_NP	/* non-portable hacking */

#include "prdma.h"
#ifdef	MOD_PRDMA_LHP_TRC_TIMESYNC
#include "timesync.h"
#endif	/* MOD_PRDMA_LHP_TRC_TIMESYNC */

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
#ifdef	MOD_PRDMA_LHP_TRC
int	_prdmaTraceSize;
#endif	/* MOD_PRDMA_LHP_TRC */

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
#ifndef	MOD_PRDMA_TAG_GET
static PrdmaReq		*_prdmaTagTab[PRDMA_TAG_MAX];
#endif	/* MOD_PRDMA_TAG_GET */
#ifdef	USE_PRDMA_MSGSTAT
static PrdmaMsgStat	_prdmaSendstat[PRDMA_MSGSTAT_SIZE];
static PrdmaMsgStat	_prdmaRecvstat[PRDMA_MSGSTAT_SIZE];
#endif	/* USE_PRDMA_MSGSTAT */
#ifdef	MOD_PRDMA_LHP_TRC_TIMESYNC
static uint64_t		_prdma_sl, _prdma_sr, _prdma_el, _prdma_er;
#endif	/* MOD_PRDMA_LHP_TRC_TIMESYNC */
#if	defined(MOD_PRDMA_F2C_FIX) && defined(MOD_PRDMA_F2C_FIX_NP)
/*
 * dummy MPI_Request structure for MPI_Request_f2c()
 */
struct dummy_mreq {
     uint64_t	ul[16]; /* 128 Bytes */
};
#define DUMMY_REQUEST_COUNT	256
static unsigned int	_prdma_mreqi = 0;
static struct dummy_mreq	_prdma_mreqs[DUMMY_REQUEST_COUNT];
#endif	/* defined(MOD_PRDMA_F2C_FIX) && defined(MOD_PRDMA_F2C_FIX_NP) */

#define PRDMA_NIC_NPAT	4
static int _prdmaNICID[PRDMA_NIC_NPAT] = {
     FJMPI_RDMA_NIC0,FJMPI_RDMA_NIC1,FJMPI_RDMA_NIC2,FJMPI_RDMA_NIC3
};
#ifndef	MOD_PRDMA_NIC_SEL
static int	_prdmaDMAFlag[PRDMA_NIC_NPAT] = {
     FJMPI_RDMA_LOCAL_NIC0 | FJMPI_RDMA_REMOTE_NIC0 | FJMPI_RDMA_PATH0,
     FJMPI_RDMA_LOCAL_NIC1 | FJMPI_RDMA_REMOTE_NIC1 | FJMPI_RDMA_PATH0,
     FJMPI_RDMA_LOCAL_NIC2 | FJMPI_RDMA_REMOTE_NIC2 | FJMPI_RDMA_PATH0,
     FJMPI_RDMA_LOCAL_NIC3 | FJMPI_RDMA_REMOTE_NIC3 | FJMPI_RDMA_PATH0
};
static int	_prdmaDMAFent;
#endif	/* MOD_PRDMA_NIC_SEL */
static PrdmaReq	*_PrdmaCQpoll();
#ifdef	MOD_PRDMA_NIC_SEL
static void	_PrdmaNICinit(void);
#endif	/* MOD_PRDMA_NIC_SEL */
#ifdef	MOD_PRDMA_SYN_MBL
static void	_PrdmaSynMBLinit(void);
#endif	/* MOD_PRDMA_SYN_MBL */
#ifdef	MOD_PRDMA_LHP_TRC
static void	_PrdmaChangeState_wrapped(PrdmaReq *preq,
			PrdmaRstate new, int newsub, int line);
#define _PrdmaChangeState(PREQ, NSTA, NSUB) \
		_PrdmaChangeState_wrapped(PREQ, NSTA, NSUB, __LINE__)
static void	_PrdmaTrcinit(void);
#endif	/* MOD_PRDMA_LHP_TRC */

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
#ifndef	MOD_PRDMA_TAG_GET

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
#else	/* MOD_PRDMA_TAG_GET */

static int	_PrdmaTagGet(PrdmaReq *pr);
static void	_PrdmaTagFree(int nic, int tag /* ent */, int pid);
static PrdmaReq	*_PrdmaTag2Req(int nic, int tag /* ent */, int pid);
static void	_PrdmaTagInit(void);
#endif	/* MOD_PRDMA_TAG_GET */

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
#ifndef	MOD_PRDMA_F2C_FIX
    int	key = ((uint64_t)addr >> 4) & 0xffffffff;
#else	/* MOD_PRDMA_F2C_FIX */
    int	key = ((uint64_t)(unsigned long)addr >> 4) & 0xffffffff;
#endif	/* MOD_PRDMA_F2C_FIX */
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
#ifndef	MOD_PRDMA_LHP_TRC
    pq->state = PRDMA_RSTATE_INIT;
#else	/* MOD_PRDMA_LHP_TRC */
    _PrdmaChangeState(pq, PRDMA_RSTATE_INIT, -1);
#endif	/* MOD_PRDMA_LHP_TRC */
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
#ifdef	MOD_PRDMA_LHP_TRC
	if (_prdma_trc_rlog != NULL) {
	    (*_prdma_trc_rlog)(pq, __LINE__);
	}
#endif	/* MOD_PRDMA_LHP_TRC */
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
#if	!defined(MOD_PRDMA_F2C_FIX) || !defined(MOD_PRDMA_F2C_FIX_NP)
	return NULL;
#else	/* !defined(MOD_PRDMA_F2C_FIX) || !defined(MOD_PRDMA_F2C_FIX_NP) */
	if (
	    ((struct dummy_mreq *)id >= &_prdma_mreqs[0])
	    && ((struct dummy_mreq *)id < &_prdma_mreqs[DUMMY_REQUEST_COUNT])
	) {
	    int *c_req = (int *)id;
	    id = c_req[21]; /* c_req->req_f_to_c_index */
	}
	else {
	    return NULL;
	}
#endif	/* !defined(MOD_PRDMA_F2C_FIX) || !defined(MOD_PRDMA_F2C_FIX_NP) */
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
#ifdef	MOD_PRDMA_LHP_TRC
    { "PRDMA_TRACESIZE", &_prdmaTraceSize },
#endif	/* MOD_PRDMA_LHP_TRC */
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
#ifdef	MOD_PRDMA_LHP_TRC
    if (_prdma_trc_fini != NULL) {
	(*_prdma_trc_fini)(_prdmaTraceSize);
    }
#endif	/* MOD_PRDMA_LHP_TRC */

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
#ifndef	MOD_PRDMA_TAG_GET
    memset(_prdmaTagTab, 0, sizeof(_prdmaTagTab));
#else	/* MOD_PRDMA_TAG_GET */
    _PrdmaTagInit();
#endif	/* MOD_PRDMA_TAG_GET */
    _prdmaRdmaSize = PRDMA_SIZE;
#ifdef	MOD_PRDMA_LHP_TRC
    _prdmaTraceSize = 0;
    _PrdmaTrcinit();
#endif	/* MOD_PRDMA_LHP_TRC */
    _PrdmaOptions();
#ifdef	MOD_PRDMA_NIC_SEL
    _PrdmaNICinit();
#endif	/* MOD_PRDMA_NIC_SEL */
#ifdef	MOD_PRDMA_SYN_MBL
    _PrdmaSynMBLinit();
#endif	/* MOD_PRDMA_SYN_MBL */
#ifdef	MOD_PRDMA_LHP_TRC
    if (_prdmaTraceSize > 0) {
	if (_prdma_trc_init != NULL) {
	    (*_prdma_trc_init)(_prdmaTraceSize);
#ifdef	MOD_PRDMA_LHP_TRC_TIMESYNC
	    timesync_sync(&_prdma_sl, &_prdma_sr);
#endif	/* MOD_PRDMA_LHP_TRC_TIMESYNC */
	}
    }
#endif	/* MOD_PRDMA_LHP_TRC */

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
#ifndef	MOD_PRDMA_TAG_GET
	    preq = _PrdmaTag2Req(cq.tag);
#else	/* MOD_PRDMA_TAG_GET */
	    preq = _PrdmaTag2Req(i /* nic */, cq.tag, cq.pid);
#endif	/* MOD_PRDMA_TAG_GET */
	    if (preq == 0) break;
	    if (preq->type == PRDMA_RTYPE_SEND) {
		if (preq->state == PRDMA_RSTATE_START) {
#ifndef	MOD_PRDMA_LHP_TRC
		    preq->state = PRDMA_RSTATE_SENDER_SENT_DATA;
#else	/* MOD_PRDMA_LHP_TRC */
		    _PrdmaChangeState(preq, PRDMA_RSTATE_SENDER_SENT_DATA, -1);
#endif	/* MOD_PRDMA_LHP_TRC */
		} else if (preq->state == PRDMA_RSTATE_SENDER_SENT_DATA) {
#ifndef	MOD_PRDMA_LHP_TRC
		    preq->state = PRDMA_RSTATE_SENDER_SEND_DONE;
#else	/* MOD_PRDMA_LHP_TRC */
		    _PrdmaChangeState(preq, PRDMA_RSTATE_SENDER_SEND_DONE, -1);
#endif	/* MOD_PRDMA_LHP_TRC */
		} else {
#ifndef	MOD_PRDMA_LHP_TRC
		    /* unknown state */
#else	/* MOD_PRDMA_LHP_TRC */
		    _PrdmaChangeState(preq, PRDMA_RSTATE_UNKNOWN, -1);
#endif	/* MOD_PRDMA_LHP_TRC */
		}
	    } else {
		/* receiver has sent sync entry to sender */
		if (preq->state == PRDMA_RSTATE_START) {
#ifndef	MOD_PRDMA_LHP_TRC
		    preq->state = PRDMA_RSTATE_RECEIVER_SYNC_SENT;
#else	/* MOD_PRDMA_LHP_TRC */
		    _PrdmaChangeState(preq, PRDMA_RSTATE_RECEIVER_SYNC_SENT, -1);
#endif	/* MOD_PRDMA_LHP_TRC */
		}
#ifdef	MOD_PRDMA_LHP_TRC
		else {
		    _PrdmaChangeState(preq, PRDMA_RSTATE_UNKNOWN, -1);
		}
#endif	/* MOD_PRDMA_LHP_TRC */
	    }
#ifndef	MOD_PRDMA_TAG_GET
	    _PrdmaTagFree(cq.tag);
#else	/* MOD_PRDMA_TAG_GET */
	    _PrdmaTagFree(i /* nic */, cq.tag, cq.pid);
#endif	/* MOD_PRDMA_TAG_GET */
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
#ifndef	MOD_PRDMA_TST_NWT
	if (preq->state != PRDMA_RSTATE_SENDER_SEND_DONE) goto retry;
#else	/* MOD_PRDMA_TST_NWT */
	if (preq->state != PRDMA_RSTATE_SENDER_SEND_DONE) {
	    if (wait == 0) break;
	    goto retry;
	}
#endif	/* MOD_PRDMA_TST_NWT */
	/* send done */
#ifndef	MOD_PRDMA_LHP_TRC
	preq->state = PRDMA_RSTATE_DONE;
#else	/* MOD_PRDMA_LHP_TRC */
	_PrdmaChangeState(preq, PRDMA_RSTATE_DONE, -1);
	preq->done++;
#endif	/* MOD_PRDMA_LHP_TRC */
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
#ifndef	MOD_PRDMA_LHP_TRC
	    preq->state = PRDMA_RSTATE_DONE;
#else	/* MOD_PRDMA_LHP_TRC */
	    _PrdmaChangeState(preq, PRDMA_RSTATE_DONE, -1);
	    preq->done++;
#endif	/* MOD_PRDMA_LHP_TRC */
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

#ifndef	MOD_PRDMA_F2C_FIX
	preq = _PrdmaReqFind((uint64_t) reqs[i]);
#else	/* MOD_PRDMA_F2C_FIX */
	preq = _PrdmaReqFind((uint64_t)(unsigned long)reqs[i]);
#endif	/* MOD_PRDMA_F2C_FIX */
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
#ifndef	MOD_PRDMA_BSY_WAIT
	usleep(1);
#endif	/* MOD_PRDMA_BSY_WAIT */
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
#ifdef	MOD_PRDMA_NIC_SEL
    if (_prdma_nic_init != NULL) {
	(*_prdma_nic_init)(preq);
    }
#endif	/* MOD_PRDMA_NIC_SEL */
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
#ifndef	MOD_PRDMA_LHP_TRC
	preq->state = PRDMA_RSTATE_PREPARED;
#else	/* MOD_PRDMA_LHP_TRC */
	_PrdmaChangeState(preq, PRDMA_RSTATE_PREPARED, -1);
#endif	/* MOD_PRDMA_LHP_TRC */
#ifdef	MOD_PRDMA_NIC_SEL
	if (_prdma_nic_sync != NULL) {
	    (*_prdma_nic_sync)(preq);
	}
#endif	/* MOD_PRDMA_NIC_SEL */
    } else {
#ifndef	MOD_PRDMA_LHP_TRC
	preq->state = PRDMA_RSTATE_WAITRMEMID;
#else	/* MOD_PRDMA_LHP_TRC */
	_PrdmaChangeState(preq, PRDMA_RSTATE_WAITRMEMID, -1);
#endif	/* MOD_PRDMA_LHP_TRC */
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
#if	defined(MOD_PRDMA_NIC_SEL) && defined(MOD_PRDMA_NIC_SEL_CD04)
    info._rfidx = preq->fidx;
#endif	/* MOD_PRDMA_NIC_SEL */
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
#ifndef	MOD_PRDMA_LHP_TRC
	preq->state = PRDMA_RSTATE_PREPARED;
#else	/* MOD_PRDMA_LHP_TRC */
	_PrdmaChangeState(preq, PRDMA_RSTATE_PREPARED, -1);
#endif	/* MOD_PRDMA_LHP_TRC */
#ifdef	MOD_PRDMA_NIC_SEL
	if (_prdma_nic_sync != NULL) {
	    (*_prdma_nic_sync)(preq);
	}
#endif	/* MOD_PRDMA_NIC_SEL */
    } else {
#ifndef	MOD_PRDMA_LHP_TRC
	preq->state = PRDMA_RSTATE_WAITRMEMID;
#else	/* MOD_PRDMA_LHP_TRC */
	_PrdmaChangeState(preq, PRDMA_RSTATE_WAITRMEMID, -1);
#endif	/* MOD_PRDMA_LHP_TRC */
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
#ifndef	MOD_PRDMA_LHP_TRC
	preq->state = PRDMA_RSTATE_PREPARED;
#else	/* MOD_PRDMA_LHP_TRC */
	_PrdmaChangeState(preq, PRDMA_RSTATE_PREPARED, -1);
#endif	/* MOD_PRDMA_LHP_TRC */
#ifdef	MOD_PRDMA_NIC_SEL
	if (_prdma_nic_sync != NULL) {
	    (*_prdma_nic_sync)(preq);
	}
#endif	/* MOD_PRDMA_NIC_SEL */
    } else if (preq->state == PRDMA_RSTATE_DONE) {
	/* restart */
#ifndef	MOD_PRDMA_LHP_TRC
	preq->state = PRDMA_RSTATE_RESTART;
#else	/* MOD_PRDMA_LHP_TRC */
	_PrdmaChangeState(preq, PRDMA_RSTATE_RESTART, -1);
#endif	/* MOD_PRDMA_LHP_TRC */
    } else if (preq->state != PRDMA_RSTATE_PREPARED) {
	_PrdmaPrintf(stderr, "MPI_Start is invoked before MPI_Wait\n");
	MPI_Abort(MPI_COMM_WORLD, -1);
	return MPI_ERR_INTERN;
    }
#ifndef	MOD_PRDMA_NIC_SEL
/*
    flag = FJMPI_RDMA_LOCAL_NIC0 | FJMPI_RDMA_REMOTE_NIC0 | FJMPI_RDMA_PATH0;
*/
    flag = _prdmaDMAFlag[_prdmaDMAFent];
    _prdmaDMAFent = (_prdmaDMAFent + 1) % PRDMA_NIC_NPAT;
#else	/* MOD_PRDMA_NIC_SEL */
    flag = (*_prdma_nic_getf)(preq);
#endif	/* MOD_PRDMA_NIC_SEL */
    switch (preq->type) {
    case PRDMA_RTYPE_SEND:
#ifdef	MOD_PRDMA_SYN_MBL
	if (_prdma_syn_send != NULL) {
	    /* remote address */
	    if (preq->raddr == (uint64_t) -1) {
		preq->raddr = FJMPI_Rdma_get_remote_addr(preq->peer, preq->rbid);
	    }
	    preq->transff ^= PRDMA_SYNC_FLIP;
	    preq->sndst = 0; /* dosync */
	    (*_prdma_syn_send)(preq);
	    return MPI_SUCCESS;
	}
#endif	/* MOD_PRDMA_SYN_MBL */
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
#ifdef	MOD_PRDMA_LHP_TRC
	_PrdmaChangeState(preq, PRDMA_RSTATE_UNKNOWN, 1 /* dosend */);
#endif	/* MOD_PRDMA_LHP_TRC */
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
#ifndef	MOD_PRDMA_LHP_TRC
	    preq->state = PRDMA_RSTATE_START;
#else	/* MOD_PRDMA_LHP_TRC */
	    _PrdmaChangeState(preq, PRDMA_RSTATE_START, -1);
#endif	/* MOD_PRDMA_LHP_TRC */
	} else {
	    _PrdmaPrintf(stderr,
		    "FJMPI_Rdma_put error in the sender side (%d, %d)\n",
		    cc1, cc2);
#ifndef	MOD_PRDMA_LHP_TRC
	    preq->state = PRDMA_RSTATE_ERROR;
#else	/* MOD_PRDMA_LHP_TRC */
	    _PrdmaChangeState(preq, PRDMA_RSTATE_ERROR, -1);
#endif	/* MOD_PRDMA_LHP_TRC */
	}
	break;
    case PRDMA_RTYPE_RECV:
	preq->transff ^= PRDMA_SYNC_FLIP;
	if (_prdmaNosync == 1) { /* no synchronization */
#ifndef	MOD_PRDMA_LHP_TRC
	    preq->state = PRDMA_RSTATE_RECEIVER_SYNC_SENT;
#else	/* MOD_PRDMA_LHP_TRC */
	    _PrdmaChangeState(preq, PRDMA_RSTATE_RECEIVER_SYNC_SENT, -1);
#endif	/* MOD_PRDMA_LHP_TRC */
	} else {
	    /* Synchronization */
	    tag = _PrdmaTagGet(preq);
	    raddr = _prdmaDmaSyncConst
		+ (preq->transff + PRDMA_SYNC_CNSTFF_0)*sizeof(uint32_t);
	    cc1 = FJMPI_Rdma_put(preq->peer, tag,
		 _prdmaRdmaSync[preq->peer] + preq->rsync*sizeof(uint32_t),
				 raddr,  sizeof(int), flag);
	    if (cc1 == 0) {
#ifndef	MOD_PRDMA_LHP_TRC
		preq->state = PRDMA_RSTATE_START;
#else	/* MOD_PRDMA_LHP_TRC */
		_PrdmaChangeState(preq, PRDMA_RSTATE_START, -1);
#endif	/* MOD_PRDMA_LHP_TRC */
	    } else {
		_PrdmaPrintf(stderr, "FJMPI_Rdma_put error in the receiver side\n");
#ifndef	MOD_PRDMA_LHP_TRC
		preq->state = PRDMA_RSTATE_ERROR;
#else	/* MOD_PRDMA_LHP_TRC */
		_PrdmaChangeState(preq, PRDMA_RSTATE_ERROR, -1);
#endif	/* MOD_PRDMA_LHP_TRC */
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
#if	!defined(MOD_PRDMA_NIC_ORD) || !defined(MOD_PRDMA_NIC_ORD_BYTYPE)
    for (preq = top; preq != NULL; preq = preq->trunks) {
	cc = _PrdmaStart0(preq);
    }
#else	/* !defined(MOD_PRDMA_NIC_ORD) || !defined(MOD_PRDMA_NIC_ORD_BYTYPE) */
    for (preq = top; preq != NULL; preq = preq->trunks) {
	if (preq->type == PRDMA_RTYPE_RECV) {
	    cc = _PrdmaStart0(preq);
	}
    }
    for (preq = top; preq != NULL; preq = preq->trunks) {
	if (preq->type != PRDMA_RTYPE_RECV) {
	    cc = _PrdmaStart0(preq);
	}
    }
#endif	/* !defined(MOD_PRDMA_NIC_ORD) || !defined(MOD_PRDMA_NIC_ORD_BYTYPE) */

    return cc;
}

int
MPI_Start(MPI_Request *request)
{
#ifndef	MOD_PRDMA_F2C_FIX
    uint16_t	reqid;
#endif	/* MOD_PRDMA_F2C_FIX */
    PrdmaReq	*preq;
    int		cc;

#ifndef	MOD_PRDMA_F2C_FIX
    reqid = (uint16_t) ((uint64_t)*request) & 0xffff;
    preq = _PrdmaReqFind(reqid);
#else	/* MOD_PRDMA_F2C_FIX */
    preq = _PrdmaReqFind((uint64_t)(unsigned long)request[0]);
#endif	/* MOD_PRDMA_F2C_FIX */
    if (preq == 0) {/* Regular Request */
	cc = PMPI_Start(request);
    } else {
	cc = _PrdmaStart(preq);
    }
#ifdef	MOD_PRDMA_SYN_MBL
    if (_prdma_syn_wait != NULL) {
	(*_prdma_syn_wait)(1, request);
    }
#endif	/* MOD_PRDMA_SYN_MBL */
    return cc;
}

int
MPI_Startall(int count, MPI_Request *reqs)
{
#ifndef	MOD_PRDMA_F2C_FIX
    uint16_t	reqid;
#endif	/* MOD_PRDMA_F2C_FIX */
    PrdmaReq	*preq;
    int		i, ret;
    int		cc = MPI_SUCCESS;

#if	!defined(MOD_PRDMA_NIC_ORD) || !defined(MOD_PRDMA_NIC_ORD_BYTYPE)
    for (i = 0; i < count; i++) {
#ifndef	MOD_PRDMA_F2C_FIX
	reqid = (uint16_t) ((uint64_t)reqs[i]) & 0xffff;
	preq = _PrdmaReqFind(reqid);
#else	/* MOD_PRDMA_F2C_FIX */
	preq = _PrdmaReqFind((uint64_t)(unsigned long)reqs[i]);
#endif	/* MOD_PRDMA_F2C_FIX */
	if (preq == 0) { /* Regular Request */
	    ret = PMPI_Start(&reqs[i]);
	} else {
	    ret = _PrdmaStart(preq);
	}
	if (ret != MPI_SUCCESS) cc = ret;
    }
#else	/* !defined(MOD_PRDMA_NIC_ORD) || !defined(MOD_PRDMA_NIC_ORD_BYTYPE) */
    for (i = 0; i < count; i++) {
#ifndef	MOD_PRDMA_F2C_FIX
	reqid = (uint16_t) ((uint64_t)reqs[i]) & 0xffff;
	preq = _PrdmaReqFind(reqid);
#else	/* MOD_PRDMA_F2C_FIX */
	preq = _PrdmaReqFind((uint64_t)(unsigned long)reqs[i]);
#endif	/* MOD_PRDMA_F2C_FIX */
	if (preq == 0) { /* Regular Request */
	    ret = PMPI_Start(&reqs[i]);
	} else {
	    if (preq->type != PRDMA_RTYPE_RECV) {
		continue;
	    }
	    ret = _PrdmaStart(preq);
	}
	if (ret != MPI_SUCCESS) cc = ret;
    }
    for (i = 0; i < count; i++) {
#ifndef	MOD_PRDMA_F2C_FIX
	reqid = (uint16_t) ((uint64_t)reqs[i]) & 0xffff;
	preq = _PrdmaReqFind(reqid);
#else	/* MOD_PRDMA_F2C_FIX */
	preq = _PrdmaReqFind((uint64_t)(unsigned long)reqs[i]);
#endif	/* MOD_PRDMA_F2C_FIX */
	if (preq == 0) { /* Regular Request */
	    continue;
	} else {
	    if (preq->type == PRDMA_RTYPE_RECV) {
		continue;
	    }
	    ret = _PrdmaStart(preq);
	}
	if (ret != MPI_SUCCESS) cc = ret;
    }
#endif	/* !defined(MOD_PRDMA_NIC_ORD) || !defined(MOD_PRDMA_NIC_ORD_BYTYPE) */
#ifdef	MOD_PRDMA_SYN_MBL
    if (_prdma_syn_wait != NULL) {
	(*_prdma_syn_wait)(count, reqs);
    }
#endif	/* MOD_PRDMA_SYN_MBL */
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

#ifndef	MOD_PRDMA_F2C_FIX
    preq = _PrdmaReqFind((uint64_t) *request);
#else	/* MOD_PRDMA_F2C_FIX */
    preq = _PrdmaReqFind((uint64_t)(unsigned long)request[0]);
#endif	/* MOD_PRDMA_F2C_FIX */
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

#ifndef	MOD_PRDMA_F2C_FIX
    preq = _PrdmaReqFind((uint64_t) *request);
#else	/* MOD_PRDMA_F2C_FIX */
    preq = _PrdmaReqFind((uint64_t)(unsigned long)request[0]);
#endif	/* MOD_PRDMA_F2C_FIX */
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

#ifndef	MOD_PRDMA_F2C_FIX
    preq = _PrdmaReqFind((uint64_t) *request);
#else	/* MOD_PRDMA_F2C_FIX */
    preq = _PrdmaReqFind((uint64_t)(unsigned long)request[0]);
#endif	/* MOD_PRDMA_F2C_FIX */
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
#ifndef	MOD_PRDMA_F2C_FIX
    uint16_t	reqid;
#endif	/* MOD_PRDMA_F2C_FIX */
    PrdmaReq	*preq;
    MPI_Fint	val;

#ifndef	MOD_PRDMA_F2C_FIX
    reqid = (uint16_t) ((uint64_t)request) & 0xffff;
    preq = _PrdmaReqFind(reqid);
#else	/* MOD_PRDMA_F2C_FIX */
    preq = _PrdmaReqFind((uint64_t)(unsigned long)request);
#endif	/* MOD_PRDMA_F2C_FIX */
    if (preq == 0) {
	val = PMPI_Request_c2f(request);
	return val;
    } else {
#ifndef	MOD_PRDMA_F2C_FIX
	return (MPI_Fint) reqid;
#else	/* MOD_PRDMA_F2C_FIX */
	return (MPI_Fint)(unsigned long)request;
#endif	/* MOD_PRDMA_F2C_FIX */
    }
}

MPI_Request MPI_Request_f2c(MPI_Fint request)
{
#ifndef	MOD_PRDMA_F2C_FIX
    uint16_t	reqid;
#endif	/* MOD_PRDMA_F2C_FIX */
    PrdmaReq	*preq;

#ifndef	MOD_PRDMA_F2C_FIX
    reqid = (uint16_t) ((uint64_t)request) & 0xffff;
    preq = _PrdmaReqFind(reqid);
#else	/* MOD_PRDMA_F2C_FIX */
    preq = _PrdmaReqFind((uint64_t)request);
#endif	/* MOD_PRDMA_F2C_FIX */
    if (preq == 0) {
	return PMPI_Request_f2c(request);
    } else {
#if	!defined(MOD_PRDMA_F2C_FIX) || !defined(MOD_PRDMA_F2C_FIX_NP)
	return (MPI_Request) request;
#else	/* !defined(MOD_PRDMA_F2C_FIX) || !defined(MOD_PRDMA_F2C_FIX_NP) */
	int ir, *c_req;
	
	if (_prdma_mreqi >= DUMMY_REQUEST_COUNT) {
	    _prdma_mreqi = 0;
#ifdef	notdef
	    _PrdmaPrintf(stderr, "[%03d] MPI_Request_c2f() : "
		"dummy MPI_Request structure wrap around to zero.\n",
		_prdmaMyrank);
#endif	/* notdef */
	}
	ir = _prdma_mreqi++;
	/*
	 * openmpi-1.6.1/ompi/mpi/f77/wait_f.c :
	 *   c_req->req_f_to_c_index
	 *     offsetof(struct ompi_request_t, req_f_to_c_index) = 84
	 */
	c_req = (int *)&_prdma_mreqs[ir];
	c_req[21] = (int)preq->uid; /* XXX magic number [84/4] */
	return (MPI_Request)c_req;
#endif	/* !defined(MOD_PRDMA_F2C_FIX) || !defined(MOD_PRDMA_F2C_FIX_NP) */
    }
}
#ifdef	MOD_PRDMA_LHP_TRC_TIMESYNC

int
MPI_Initialized(int *flag)
{
    int cc;
    cc = PMPI_Initialized(flag);
    if (_prdmaTraceSize > 0) {
	if (_prdma_trc_init != NULL) {
	    timesync_sync(&_prdma_el, &_prdma_er);
	}
    }
    return cc;
}
#endif	/* MOD_PRDMA_LHP_TRC_TIMESYNC */

#ifdef	MOD_PRDMA_NIC_SEL

/*
 * callback functions
 */
prdma_nic_cb_f   _prdma_nic_init = NULL;
prdma_nic_cb_f   _prdma_nic_sync = NULL;
prdma_nic_cb_f   _prdma_nic_getf = NULL;

#if	defined(MOD_PRDMA_NIC_SEL_CD00)
/*
 * interconnect nic selection - Candidate 00
 */
static int	_prdmaDMAFlag[PRDMA_NIC_NPAT] = {
     FJMPI_RDMA_LOCAL_NIC0 | FJMPI_RDMA_REMOTE_NIC0 | FJMPI_RDMA_PATH0,
     FJMPI_RDMA_LOCAL_NIC1 | FJMPI_RDMA_REMOTE_NIC1 | FJMPI_RDMA_PATH0,
     FJMPI_RDMA_LOCAL_NIC2 | FJMPI_RDMA_REMOTE_NIC2 | FJMPI_RDMA_PATH0,
     FJMPI_RDMA_LOCAL_NIC3 | FJMPI_RDMA_REMOTE_NIC3 | FJMPI_RDMA_PATH0
};
static int	_prdmaDMAFent;

static int
_Prdma_NIC_getf_cd00(PrdmaReq *preq)
{
    int flag;
    flag = _prdmaDMAFlag[_prdmaDMAFent];
    _prdmaDMAFent = (_prdmaDMAFent + 1) % PRDMA_NIC_NPAT;
    return flag;
}

static void
_PrdmaNICinit(void)
{
	_prdma_nic_init = NULL;
	_prdma_nic_sync = NULL;
	_prdma_nic_getf = _Prdma_NIC_getf_cd00;
}

#elif	defined(MOD_PRDMA_NIC_SEL_CD01)

/*
 * interconnect nic selection - Candidate 01
 */
static int	_prdmaDMAFlag[PRDMA_NIC_NPAT] = {
     FJMPI_RDMA_LOCAL_NIC0 | FJMPI_RDMA_REMOTE_NIC0 | FJMPI_RDMA_PATH0,
     FJMPI_RDMA_LOCAL_NIC1 | FJMPI_RDMA_REMOTE_NIC1 | FJMPI_RDMA_PATH0,
     FJMPI_RDMA_LOCAL_NIC2 | FJMPI_RDMA_REMOTE_NIC2 | FJMPI_RDMA_PATH0,
     FJMPI_RDMA_LOCAL_NIC3 | FJMPI_RDMA_REMOTE_NIC3 | FJMPI_RDMA_PATH0
};
static int	_prdmaDMAFent_s;
static int	_prdmaDMAFent_r;

static int
_Prdma_NIC_getf_cd01(PrdmaReq *preq)
{
    int flag;
    if (preq->type == PRDMA_RTYPE_SEND) {
	flag = _prdmaDMAFlag[_prdmaDMAFent_s];
	_prdmaDMAFent_s = (_prdmaDMAFent_s + 1) % PRDMA_NIC_NPAT;
    }
    else {
	flag = _prdmaDMAFlag[_prdmaDMAFent_r];
	_prdmaDMAFent_r = (_prdmaDMAFent_r + 1) % PRDMA_NIC_NPAT;
    }
    return flag;
}

static void
_PrdmaNICinit(void)
{
	_prdma_nic_init = NULL;
	_prdma_nic_sync = NULL;
	_prdma_nic_getf = _Prdma_NIC_getf_cd01;
}

#elif	defined(MOD_PRDMA_NIC_SEL_CD02)

/*
 * interconnect nic selection - Candidate 02
 */
static int	_prdmaDMAFlag[PRDMA_NIC_NPAT] = {
     FJMPI_RDMA_LOCAL_NIC0 | FJMPI_RDMA_REMOTE_NIC0 | FJMPI_RDMA_PATH0,
     FJMPI_RDMA_LOCAL_NIC1 | FJMPI_RDMA_REMOTE_NIC1 | FJMPI_RDMA_PATH0,
     FJMPI_RDMA_LOCAL_NIC2 | FJMPI_RDMA_REMOTE_NIC2 | FJMPI_RDMA_PATH0,
     FJMPI_RDMA_LOCAL_NIC3 | FJMPI_RDMA_REMOTE_NIC3 | FJMPI_RDMA_PATH0
};
static int	_prdmaDMAFent_s;
static int	_prdmaDMAFent_r;

static int
_Prdma_NIC_init_cd02(PrdmaReq *preq)
{
    if (preq->type == PRDMA_RTYPE_SEND) {
	preq->fidx = _prdmaDMAFent_s;
	_prdmaDMAFent_s = (_prdmaDMAFent_s + 1) % PRDMA_NIC_NPAT;
    }
    else {
	preq->fidx = _prdmaDMAFent_r;
	_prdmaDMAFent_r = (_prdmaDMAFent_r + 1) % PRDMA_NIC_NPAT;
    }
    return 0;
}

static int
_Prdma_NIC_getf_cd02(PrdmaReq *preq)
{
    int flag;
    flag = _prdmaDMAFlag[preq->fidx];
    return flag;
}

static void
_PrdmaNICinit(void)
{
	_prdma_nic_init = _Prdma_NIC_init_cd02;
	_prdma_nic_sync = NULL;
	_prdma_nic_getf = _Prdma_NIC_getf_cd02;
}

#elif	defined(MOD_PRDMA_NIC_SEL_CD03)

/*
 * interconnect nic selection - Candidate 03
 */
static int	_prdmaDMAFlag[PRDMA_NIC_NPAT] = {
     FJMPI_RDMA_LOCAL_NIC0 | FJMPI_RDMA_REMOTE_NIC0 | FJMPI_RDMA_PATH0,
     FJMPI_RDMA_LOCAL_NIC1 | FJMPI_RDMA_REMOTE_NIC1 | FJMPI_RDMA_PATH0,
     FJMPI_RDMA_LOCAL_NIC2 | FJMPI_RDMA_REMOTE_NIC2 | FJMPI_RDMA_PATH0,
     FJMPI_RDMA_LOCAL_NIC3 | FJMPI_RDMA_REMOTE_NIC3 | FJMPI_RDMA_PATH0
};

static int
_Prdma_NIC_init_cd03(PrdmaReq *preq)
{
    if (preq->peer == (_prdmaMyrank - 1)) {		/* West */
        if (preq->type == PRDMA_RTYPE_SEND) {
            preq->fidx = 0;
        }
        else /* if (preq->type == PRDMA_RTYPE_RECV) */ {
            preq->fidx = 1;
        }
    }
    else if (preq->peer == (_prdmaMyrank + 1)) {	/* East */
        if (preq->type == PRDMA_RTYPE_SEND) {
            preq->fidx = 1;
        }
        else /* if (preq->type == PRDMA_RTYPE_RECV) */ {
            preq->fidx = 0;
        }
    }
    else if (preq->peer < _prdmaMyrank) {		/* North */
        if (preq->type == PRDMA_RTYPE_SEND) {
            preq->fidx = 2;
        }
        else /* if (preq->type == PRDMA_RTYPE_RECV) */ {
            preq->fidx = 3;
        }
    }
    else /* if (preq->peer > _prdmaMyrank) */ {		/* South */
        if (preq->type == PRDMA_RTYPE_SEND) {
            preq->fidx = 3;
        }
        else /* if (preq->type == PRDMA_RTYPE_RECV) */ {
            preq->fidx = 2;
        }
    }
    return 0;
}

static int
_Prdma_NIC_getf_cd03(PrdmaReq *preq)
{
    int flag;
    flag = _prdmaDMAFlag[preq->fidx];
    return flag;
}

static void
_PrdmaNICinit(void)
{
	_prdma_nic_init = _Prdma_NIC_init_cd03;
	_prdma_nic_sync = NULL;
	_prdma_nic_getf = _Prdma_NIC_getf_cd03;
}

#elif	defined(MOD_PRDMA_NIC_SEL_CD04)

/*
 * interconnect nic selection - Candidate 04
 */
static int	_prdmaDMAFlag_local[PRDMA_NIC_NPAT] = {
    FJMPI_RDMA_LOCAL_NIC0,
    FJMPI_RDMA_LOCAL_NIC1,
    FJMPI_RDMA_LOCAL_NIC2,
    FJMPI_RDMA_LOCAL_NIC3
};
static int	_prdmaDMAFlag_remote[PRDMA_NIC_NPAT] = {
    FJMPI_RDMA_REMOTE_NIC0,
    FJMPI_RDMA_REMOTE_NIC1,
    FJMPI_RDMA_REMOTE_NIC2,
    FJMPI_RDMA_REMOTE_NIC3
};
static int	_prdmaDMAFent_s;
static int	_prdmaDMAFent_r;

static int
_Prdma_NIC_init_cd04(PrdmaReq *preq)
{
    if (preq->type == PRDMA_RTYPE_SEND) {
	preq->fidx = _prdmaDMAFent_s;
	_prdmaDMAFent_s = (_prdmaDMAFent_s + 1) % PRDMA_NIC_NPAT;
    }
    else {
	preq->fidx = _prdmaDMAFent_r;
	_prdmaDMAFent_r = (_prdmaDMAFent_r + 1) % PRDMA_NIC_NPAT;
    }
    return 0;
}

static int
_Prdma_NIC_sync_cd04(PrdmaReq *preq)
{
    if (preq->type == PRDMA_RTYPE_SEND) {
	preq->flag =	_prdmaDMAFlag_local[preq->fidx]
			| _prdmaDMAFlag_remote[preq->rfidx]
			| FJMPI_RDMA_PATH0
			;
    }
    else {
	preq->flag =	_prdmaDMAFlag_local[preq->fidx]
			| _prdmaDMAFlag_remote[preq->fidx]
			| FJMPI_RDMA_PATH0
			;
    }
    return 0;
}

static int
_Prdma_NIC_getf_cd04(PrdmaReq *preq)
{
    return preq->flag;
}

static void
_PrdmaNICinit(void)
{
	_prdma_nic_init = _Prdma_NIC_init_cd04;
	_prdma_nic_sync = _Prdma_NIC_sync_cd04;
	_prdma_nic_getf = _Prdma_NIC_getf_cd04;
}

#endif	/* defined(*) */

#endif	/* MOD_PRDMA_NIC_SEL */

#ifdef	MOD_PRDMA_SYN_MBL

/*
 * multi-requst busy loop in synchronization
 */
#ifndef	MOD_PRDMA_NIC_SEL
#error	"Need to define MOD_PRDMA_NIC_SEL"
#endif	/* MOD_PRDMA_NIC_SEL */

/*
 * callback function hooks
 */
prdma_syn_cb_f   _prdma_syn_send = NULL;
prdma_syn_wt_f   _prdma_syn_wait = NULL;

static int
_Prdma_Syn_send(PrdmaReq *preq)
{
    int		ret = 0;
    int		cc1, cc2;
    int		flag;
    int		tag;
    int		idx;
    uint32_t	transid;
    int		giveup, nloops;

    if (
	(preq->state == PRDMA_RSTATE_PREPARED)
	|| (preq->state == PRDMA_RSTATE_RESTART)
    ) {
	/* */;
    }
    else if (
	(preq->state == PRDMA_RSTATE_START)
    ) {
	ret = 1;
	goto bad;
    }
    else {
	_PrdmaPrintf(stderr, "Bad state %d\n", preq->state);
#ifndef	MOD_PRDMA_LHP_TRC
	preq->state = PRDMA_RSTATE_ERROR;
#else	/* MOD_PRDMA_LHP_TRC */
	_PrdmaChangeState(preq, PRDMA_RSTATE_ERROR, -1);
#endif	/* MOD_PRDMA_LHP_TRC */
	ret = -1;
	goto bad;
    }
    switch (preq->sndst) {
    case 0: /* dosync */
	if (_prdmaNosync == 0) {
	    giveup = 50; /* XXX */
	    nloops = 0;
	    /* Synchronization */
	    idx = preq->lsync;
	    transid = _prdmaSyncConst[preq->transff + PRDMA_SYNC_CNSTFF_0];
	    while (_prdmaSync[idx] != transid) {
		if (nloops++ >= giveup) {
		    goto bad; /* XXX is not an error */
		}
	    }
	}
	preq->sndst = 1; /* dosend */
	break;
    case 1: /* dosend */
#ifdef	MOD_PRDMA_LHP_TRC
	_PrdmaChangeState(preq, PRDMA_RSTATE_UNKNOWN, 1 /* dosend */);
#endif	/* MOD_PRDMA_LHP_TRC */
	flag = (*_prdma_nic_getf)(preq); /* MOD_PRDMA_NIC_SEL */
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
#ifndef	MOD_PRDMA_LHP_TRC
	    preq->state = PRDMA_RSTATE_START;
#else	/* MOD_PRDMA_LHP_TRC */
	    _PrdmaChangeState(preq, PRDMA_RSTATE_START, -1);
#endif	/* MOD_PRDMA_LHP_TRC */
	    ret = 1;
	} else {
	    _PrdmaPrintf(stderr,
		    "FJMPI_Rdma_put error in the sender side (%d, %d)\n",
		    cc1, cc2);
#ifndef	MOD_PRDMA_LHP_TRC
	    preq->state = PRDMA_RSTATE_ERROR;
#else	/* MOD_PRDMA_LHP_TRC */
	    _PrdmaChangeState(preq, PRDMA_RSTATE_ERROR, -1);
#endif	/* MOD_PRDMA_LHP_TRC */
	}
	preq->sndst = 0; /* dosync */
	break;
    }
bad:
    return ret;
}

static int
_Prdma_Syn_wait(int nreq, MPI_Request *reqs)
{
    int ir;
    int doretry;

retry:
    doretry = 0;
    for (ir = 0; ir < nreq; ir++) {
	PrdmaReq	*head, *preq;
#ifndef	MOD_PRDMA_F2C_FIX
	uint16_t	reqid;
#endif	/* MOD_PRDMA_F2C_FIX */
	
#ifndef	MOD_PRDMA_F2C_FIX
	reqid = (uint16_t) ((uint64_t)reqs[ir]) & 0xffff;
	head = _PrdmaReqFind(reqid);
#else	/* MOD_PRDMA_F2C_FIX */
	head = _PrdmaReqFind((uint64_t)(unsigned long)reqs[ir]);
#endif	/* MOD_PRDMA_F2C_FIX */
	if (head == 0) { /* Regular Request */
	    continue;
	}
	for (preq = head; preq != NULL; preq = preq->trunks) {
	    if (preq->type == PRDMA_RTYPE_RECV) {
		continue;
	    }
	    if (
		(preq->state == PRDMA_RSTATE_ERROR)
		|| (preq->state == PRDMA_RSTATE_START)
	    ) {
		continue;
	    }
	    /*
	     * _Prdma_Syn_wait() -> _Prdma_Syn_send()
	     *   -> _PrdmaTagGet() -> _PrdmaCQpoll()
	     */
	    if (
		(preq->state == PRDMA_RSTATE_SENDER_SENT_DATA)
		|| (preq->state == PRDMA_RSTATE_SENDER_SEND_DONE)
	    ) {
		continue;
	    }
	    _Prdma_Syn_send(preq);
	    if (preq->state == PRDMA_RSTATE_ERROR) {
		continue;
	    }
	    if (preq->state != PRDMA_RSTATE_START) {
		doretry++;
	    }
	}
	
    }
    if (doretry > 0) {
	goto retry;
    }
    return MPI_SUCCESS;
}

static void
_PrdmaSynMBLinit(void)
{
	_prdma_syn_send = _Prdma_Syn_send;
	_prdma_syn_wait = _Prdma_Syn_wait;
}

#endif	/* MOD_PRDMA_SYN_MBL */
#ifdef	MOD_PRDMA_TAG_GET

#if	defined(MOD_PRDMA_TAG_GET_CD00)
/* variables */
static PrdmaReq		*_prdmaTagTab[PRDMA_TAG_MAX];

/* functions */
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
_PrdmaTagFree(int nic, int tag, int pid)
{
    _prdmaTagTab[tag] = 0;
}

static PrdmaReq	*
_PrdmaTag2Req(int nic, int tag, int pid)
{
    return _prdmaTagTab[tag];
}

static void
_PrdmaTagInit()
{
    memset(_prdmaTagTab, 0, sizeof(_prdmaTagTab));
}
#elif	defined(MOD_PRDMA_TAG_GET_CD01)
/* variables */
static PrdmaReq		*_prdmaTagTab[PRDMA_TAG_MAX];

/* functions */
static int
_PrdmaTagGet(PrdmaReq *pr)
{
    int		ent, cent;
    int		retries = 0;

    ent = ((pr->fidx & 0x03) + 1) * ((pr->peer & 0x03) + 1);
    if (ent >= PRDMA_TAG_MAX) {
	ent = 0;
    }
retry:
    retries++;
    cent = ent;
    do {
	PrdmaReq *preq;
	PrdmaReq **prev = &_prdmaTagTab[cent];
	
	while ((preq = *prev) != 0) {
	    if (preq == pr) {
		break;
	    }
	    if (
		(preq->peer == pr->peer)
		&& (preq->fidx == pr->fidx)
	    ) {
		break;
	    }
	    prev = &preq->tnxt[cent]; /* tag next */
	}
	if (preq == 0) {
	    pr->tnxt[cnet] = 0;
	    prev[0] = pr;
	    return cent;
	}
	cent++;
	if (cent >= PRDMA_TAG_MAX) {
	    cent = 0;
	}
    } while (cent != ent);

    /* no more tag */
    _PrdmaCQpoll();
    if (retries < 10000) {
	_prdmaWaitTag++;
	usleep(1);
	goto retry;
    }
    _PrdmaPrintf(stderr, "_PrdmaTagGet: no more tag\n");
    PMPI_Abort(MPI_COMM_WORLD, -1);
    return -1;
}

static void
_PrdmaTagFree(int nic, int tag, int pid)
{
    PrdmaReq	*preq, **prev, **found = 0;

    prev = &_prdmaTagTab[tag /* ent */];
    while ((preq = *prev) != 0) {
	if (
	    (preq->peer == pid)
	    && (preq->fidx == nic)
	) {
#ifdef	notyet
	    found = prev;
	    break;
#else	/* notyet */
	    if (found != 0) {
		_PrdmaPrintf(stderr, "_PrdmaTagFree: duplicated\n");
		PMPI_Abort(MPI_COMM_WORLD, -1);
	    }
	    found = prev;
#endif	/* notyet */
	}
	prev = &preq->tnxt[tag]; /* tag next */
    }
    if (found != 0) {
	preq = *found;
	found[0] = preq->tnxt[tag];
	preq->tnxt[tag] = 0;
    }
#ifndef	notyet
    else {
	_PrdmaPrintf(stderr, "_PrdmaTagFree: not found\n");
	PMPI_Abort(MPI_COMM_WORLD, -1);
    }
#endif	/* notyet */
    return ;
}

static PrdmaReq	*
_PrdmaTag2Req(int nic, int tag, int pid)
{
    PrdmaReq	*preq, **prev, *found = 0;

    prev = &_prdmaTagTab[tag /* ent */];
    while ((preq = *prev) != 0) {
	if (
	    (preq->peer == pid)
	    && (preq->fidx == nic)
	) {
#ifdef	notyet
	    found = preq;
	    break;
#else	/* notyet */
	    if (found != 0) {
		_PrdmaPrintf(stderr, "_PrdmaTag2Req: duplicated\n");
		PMPI_Abort(MPI_COMM_WORLD, -1);
	    }
	    found = preq;
#endif	/* notyet */
	}
	prev = &preq->tnxt[tag]; /* tag next */
    }
#ifndef	notyet
    if (found == 0) {
	_PrdmaPrintf(stderr, "_PrdmaTag2Req: not found\n");
	/* PMPI_Abort(MPI_COMM_WORLD, -1); */
    }
#endif	/* notyet */
    return found;
}

static void
_PrdmaTagInit()
{
    memset(_prdmaTagTab, 0, sizeof(_prdmaTagTab));
}
#elif	defined(MOD_PRDMA_TAG_GET_CD02)
/* variables */
static PrdmaReq		*_prdmaTagTab[PRDMA_NIC_NPAT][PRDMA_TAG_MAX];

/* functions */
static int
_PrdmaTagGet(PrdmaReq *pr)
{
    int		ent, tag, nic;
    int		retries = 0;

    nic = pr->fidx;
#ifndef	notyet
    if ((nic < 0) || (nic >= PRDMA_NIC_NPAT)) {
	_PrdmaPrintf(stderr, "_PrdmaTagGet: bad nic %d\n", nic);
	PMPI_Abort(MPI_COMM_WORLD, -1);
    }
#endif	/* notyet */
    ent =
	  ((pr->peer & 0x0000000f) >>  0)
	+ ((pr->peer & 0x000f0000) >> 16)
	+ ((pr->peer & 0x0f000000) >> 24)
	;
    ent &= 0x0f;
    if ((ent < 0) || (ent >= PRDMA_TAG_MAX)) {
	ent = 0;
    }
retry:
    retries++;
    tag = ent;
    do {
	PrdmaReq *preq;
	PrdmaReq **prev = &_prdmaTagTab[nic][tag];
	
	while ((preq = *prev) != 0) {
	    if (preq == pr) {
		break;
	    }
	    if (
		(preq->peer == pr->peer)
		/* && (preq->fidx == nic) */
	    ) {
		break;
	    }
	    prev = &preq->tnxt[tag]; /* tag next */
	}
	if (preq == 0) {
#ifdef	notyet
	    pr->tnxt[tag] = 0;
#else	/* notyet */
	    if (pr->tnxt[tag] != 0) {
		_PrdmaPrintf(stderr, "_PrdmaTagGet: req busy\n");
		PMPI_Abort(MPI_COMM_WORLD, -1);
	    }
#endif	/* notyet */
	    prev[0] = pr;
	    return tag;
	}
	tag++;
	if (tag >= PRDMA_TAG_MAX) {
	    tag = 0;
	}
    } while (tag != ent);

    /* no more tag */
    _PrdmaCQpoll();
    if (retries < 10000) {
	_prdmaWaitTag++;
	usleep(1);
	goto retry;
    }
    _PrdmaPrintf(stderr, "_PrdmaTagGet: no more tag\n");
    PMPI_Abort(MPI_COMM_WORLD, -1);
    return -1;
}

static void
_PrdmaTagFree(int nic, int tag, int pid)
{
    PrdmaReq	*preq, **prev, **found = 0;

#ifndef	notyet
    if ((nic < 0) || (nic >= PRDMA_NIC_NPAT)) {
	_PrdmaPrintf(stderr, "_PrdmaTagFree: bad nic %d\n", nic);
	PMPI_Abort(MPI_COMM_WORLD, -1);
    }
    if ((tag < 0) || (tag >= PRDMA_TAG_MAX)) {
	_PrdmaPrintf(stderr, "_PrdmaTagFree: bad tag %d\n", tag);
	PMPI_Abort(MPI_COMM_WORLD, -1);
    }
#endif	/* notyet */
    prev = &_prdmaTagTab[nic][tag /* ent */];
    while ((preq = *prev) != 0) {
	if (
	    (preq->peer == pid)
	    /* && (preq->fidx == nic) */
	) {
#ifdef	notyet
	    found = prev;
	    break;
#else	/* notyet */
	    if (found != 0) {
		_PrdmaPrintf(stderr, "_PrdmaTagFree: duplicated\n");
		PMPI_Abort(MPI_COMM_WORLD, -1);
	    }
	    found = prev;
#endif	/* notyet */
	}
	prev = &preq->tnxt[tag]; /* tag next */
    }
    if (found != 0) {
	preq = found[0];
	found[0] = preq->tnxt[tag];
	preq->tnxt[tag] = 0;
    }
#ifndef	notyet
    else {
	_PrdmaPrintf(stderr, "_PrdmaTagFree: not found\n");
	PMPI_Abort(MPI_COMM_WORLD, -1);
    }
#endif	/* notyet */
    return ;
}

static PrdmaReq	*
_PrdmaTag2Req(int nic, int tag, int pid)
{
    PrdmaReq	*preq, **prev, *found = 0;

#ifndef	notyet
    if ((nic < 0) || (nic >= PRDMA_NIC_NPAT)) {
	_PrdmaPrintf(stderr, "_PrdmaTag2Req: bad nic %d\n", nic);
	PMPI_Abort(MPI_COMM_WORLD, -1);
    }
    if ((tag < 0) || (tag >= PRDMA_TAG_MAX)) {
	_PrdmaPrintf(stderr, "_PrdmaTag2Req: bad tag %d\n", tag);
	PMPI_Abort(MPI_COMM_WORLD, -1);
    }
#endif	/* notyet */
    prev = &_prdmaTagTab[nic][tag /* ent */];
    while ((preq = *prev) != 0) {
	if (
	    (preq->peer == pid)
	    /* && (preq->fidx == nic) */
	) {
#ifdef	notyet
	    found = preq;
	    break;
#else	/* notyet */
	    if (found != 0) {
		_PrdmaPrintf(stderr, "_PrdmaTag2Req: duplicated, "
		"tag %d nic %d, "
		"%c for %d with nic %d, "
		"%c for %d with nic %d "
		"\n",
		tag, nic,
		(found->type == PRDMA_RTYPE_SEND)? 'S': 'R',
			found->peer, found->fidx,
		(preq->type == PRDMA_RTYPE_SEND)? 'S': 'R',
			preq->peer, preq->fidx
		);
		PMPI_Abort(MPI_COMM_WORLD, -1);
	    }
	    found = preq;
#endif	/* notyet */
	}
	prev = &preq->tnxt[tag]; /* tag next */
    }
#ifndef	notyet
    if (found == 0) {
	_PrdmaPrintf(stderr, "_PrdmaTag2Req: not found\n");
	/* PMPI_Abort(MPI_COMM_WORLD, -1); */
    }
#endif	/* notyet */
    return found;
}

static void
_PrdmaTagInit()
{
    memset(_prdmaTagTab, 0, sizeof(_prdmaTagTab));
}
#endif	/* defined(MOD_PRDMA_TAG_GET_*) */

#endif	/* MOD_PRDMA_TAG_GET */
#ifdef	MOD_PRDMA_LHP_TRC
/*
 * Light-weight and High Precision Trace
 */
/*
 * callback functions
 */
prdma_trc_cb_f   _prdma_trc_init = NULL;
prdma_trc_cb_f   _prdma_trc_fini = NULL;
prdma_trc_pt_f   _prdma_trc_wlog = NULL;
prdma_trc_pt_f   _prdma_trc_rlog = NULL;

static void
_PrdmaChangeState_wrapped(PrdmaReq *preq, PrdmaRstate new, int newsub, int line)
{
    /* PRDMA_RSTATE_UNKNOWN is information-only state */
    if (new != PRDMA_RSTATE_UNKNOWN) {
	preq->state = new;
    }
    if (newsub >= 0) {
    }
    if (_prdma_trc_wlog != NULL) {
	(*_prdma_trc_wlog)(preq, line);
    }
}

#ifdef	MOD_PRDMA_LHP_TRC_CD00

typedef struct PrdmaTrace {
    uint64_t		 time;
    PrdmaReq		*preq;
    unsigned int	 line;
    unsigned int	 done;
} PrdmaTrace;

static PrdmaTrace	*_prdmaTrace = 0;
static unsigned int	 _prdmaTraceIdx;
static unsigned int	 _prdmaTraceMax;

static int
_Prdma_Trc_init_cd00(int tracesize)
{
    if (_prdmaTrace != 0) {
	free(_prdmaTrace); _prdmaTrace = 0;
	_prdmaTraceIdx = _prdmaTraceMax = 0;
    }
    if (tracesize > 0) {
	size_t msiz;
	msiz = sizeof (_prdmaTrace[0]) * tracesize;
	_prdmaTrace = malloc(msiz);
        if (_prdmaTrace != 0) {
	    _prdmaTraceIdx = 0;
	    _prdmaTraceMax = tracesize;
	}
    }
    return (_prdmaTrace != 0)? 0: 1;
}

static int
_Prdma_Trc_fini_cd00(int tracesize)
{
    if (_prdmaTrace != 0) {
	free(_prdmaTrace); _prdmaTrace = 0;
	_prdmaTraceIdx = _prdmaTraceMax = 0;
    }
    return 0;
}

static int
_Prdma_Trc_wlog_cd00(PrdmaReq *preq, int line)
{
    PrdmaTrace *ptrc;
    int ix;

    if ((_prdmaTraceMax <= 0) || (_prdmaTrace == 0)) {
	return -1;
    }

    if (_prdmaTraceIdx >= _prdmaTraceMax) {
	_prdmaTraceIdx = 0;
    }
    ix = _prdmaTraceIdx++;

    ptrc = &_prdmaTrace[ix];
#ifndef	MOD_PRDMA_LHP_TRC_TIMESYNC
    ptrc->time = 0;
#else	/* MOD_PRDMA_LHP_TRC_TIMESYNC */
    ptrc->time = timesync_rdtsc();
#endif	/* MOD_PRDMA_LHP_TRC_TIMESYNC */
    ptrc->preq = preq;
    ptrc->line = line;
    ptrc->done = preq->done;

    return 0;
}

static int
_Prdma_Trc_rlog_cd00(PrdmaReq *preq, int line)
{
    int ix, ii;
    double dv;

    if ((_prdmaTraceMax <= 0) || (_prdmaTrace == 0)) {
	return -1;
    }

    ii = ix = _prdmaTraceIdx;
    if (ix == 0) {
	ix = _prdmaTraceMax;
    }
    do {
	if (ii >= _prdmaTraceMax) {
	    ii = 0;
	}
	if (_prdmaTrace[ii].preq != preq) {
	    continue;
	}
	dv = 0.0;
#ifdef	MOD_PRDMA_LHP_TRC_TIMESYNC
	if (_prdma_el != 0) {
	    dv = timesync_conv(_prdma_sl, _prdma_sr,
		_prdma_el, _prdma_er, _prdmaTrace[ii].time);
	}
#endif	/* MOD_PRDMA_LHP_TRC_TIMESYNC */
	printf("%14.9f evnt %4d rank %2d ruid %2d type  %c "
	    "done %2d peer %2d flcl %2d frmt %2d\n",
	    dv, _prdmaTrace[ii].line, _prdmaMyrank, preq->uid,
	    (preq->type == PRDMA_RTYPE_SEND)? 'S': 'R',
	    _prdmaTrace[ii].done, preq->peer,
	    preq->fidx, preq->rfidx);
    } while (++ii != ix);

    return 0;
}

static void
_PrdmaTrcinit(void)
{
    _prdma_trc_init = _Prdma_Trc_init_cd00;
    _prdma_trc_fini = _Prdma_Trc_fini_cd00;
    _prdma_trc_wlog = _Prdma_Trc_wlog_cd00;
    _prdma_trc_rlog = _Prdma_Trc_rlog_cd00;
    return ;
}

#endif	/* MOD_PRDMA_LHP_TRC_CD00 */

#endif	/* MOD_PRDMA_LHP_TRC */
