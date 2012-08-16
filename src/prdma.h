/*
 * Persistent Communication based on RDMA
 *   20/02/2012	Written by Yutaka Ishikawa
 *		ishikawa@is.s.u-tokyo.ac.jp, yutaka.ishikawa@riken.jp
 */
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <stdarg.h>
#include <mpi.h>
#ifdef FJ_MPI
#include <mpi-ext.h>
#else
struct FJMPI_Rdma_cq {
    int	pid;
    int	tag;
};
extern int	FJMPI_Rdma_reg_mem(int id, char *buf, int size);
extern int	FJMPI_Rdma_put(int dest, int tag,
			       uint64_t laddr, uint64_t raddr,
			       int size, int flag);
extern int	FJMPI_Rdma_get_remote_addr(int, int);
extern int	FJMPI_Rdma_init();
extern int	FJMPI_Rdma_finalize();
extern int	FJMPI_Rdma_poll_cq(int nic, struct FJMPI_Rdma_cq *cq);
#define FJMPI_RDMA_ERROR	-1
#define FJMPI_RDMA_NOTICE	1
#define FJMPI_RDMA_REMOTE_NOTICE 2
#define FJMPI_RDMA_LOCAL_NIC0	0x001
#define FJMPI_RDMA_LOCAL_NIC1	0x002
#define FJMPI_RDMA_LOCAL_NIC2	0x004
#define FJMPI_RDMA_LOCAL_NIC3	0x008
#define FJMPI_RDMA_REMOTE_NIC0	0x010
#define FJMPI_RDMA_REMOTE_NIC1	0x020
#define FJMPI_RDMA_REMOTE_NIC2	0x040
#define FJMPI_RDMA_REMOTE_NIC3	0x080
#define	FJMPI_RDMA_PATH0	0x100
#define	FJMPI_RDMA_PATH1	0x200
#define	FJMPI_RDMA_PATH2	0x400
#define	FJMPI_RDMA_PATH3	0x800
#define FJMPI_RDMA_NIC0		0x001
#define FJMPI_RDMA_NIC1		0x002
#define FJMPI_RDMA_NIC2		0x004
#define FJMPI_RDMA_NIC3		0x008
#endif

typedef enum {
    PRDMA_RTYPE_SEND	= 1,
    PRDMA_RTYPE_RECV	= 2,
} PrdmaRtype;

typedef enum {
    PRDMA_RSTATE_INIT = 1,
    PRDMA_RSTATE_WAITRMEMID = 2,
    PRDMA_RSTATE_PREPARED = 3,
    PRDMA_RSTATE_START = 4,
    PRDMA_RSTATE_SENDER_SENT_DATA = 5,
    PRDMA_RSTATE_SENDER_SEND_DONE = 6,
    PRDMA_RSTATE_RECEIVER_SYNC_SENT = 7,
    PRDMA_RSTATE_DONE = 8,
    PRDMA_RSTATE_RESTART = 9,
    PRDMA_RSTATE_ERROR = -1
} PrdmaRstate;

/* offset is memid */
typedef struct PrdmaDmaRegion {
    void		*start;
    uint64_t		dmaaddr;
    uint64_t		size;
    int			memid;
    struct PrdmaDmaRegion *next;
} PrdmaDmaRegion;

struct recvinfo {
    int			_rbid;		/* memid of remote buf */
    int			_rsync;		/* index of synchronization */
#if	defined(MOD_PRDMA_NIC_SEL) && defined(MOD_PRDMA_NIC_SEL_CD04)
    int			_rfidx;		/* index of remote nic */
#endif	/* MOD_PRDMA_NIC_SEL */
};
#define rbid	rinfo._rbid
#define rsync	rinfo._rsync
#if	defined(MOD_PRDMA_NIC_SEL) && defined(MOD_PRDMA_NIC_SEL_CD04)
#define rfidx	rinfo._rfidx
#endif	/* MOD_PRDMA_NIC_SEL */
#ifdef	MOD_PRDMA_TAG_GET
/* tag for FJMPI_Rdma_put() */
#define PRDMA_TAG_MAX		15
#define PRDMA_TAG_START		1
#endif	/* MOD_PRDMA_TAG_GET */

typedef struct PrdmaReq {
    struct PrdmaReq	*next;		/* link to the same hash key */
    struct recvinfo	rinfo;
    int			lsync;		/* index of synchronization */
    uint32_t		transff;	/* flip/flop */
    PrdmaRtype		type;		/* request type (send/recv) */
    PrdmaRstate		state;		/* state */
    uint16_t		uid;		/* uid of this request */
    int			lbid;		/* memid of local buf */
    uint64_t		lbaddr;		/* local buf DMA address */
    uint64_t		raddr;		/* remote buf DMA address in sender
					 * remote sync start address in recv */
    MPI_Request		negreq;		/* for negotiation */
    size_t		size;		/* size in byte of this MPI message */
    int			worldrank;	/* rank in MPI_COMM_WORLD */
    int			transcnt;	/* count in this message */
    struct PrdmaReq	*trunks;	/* packetized */
    /* The following entries are parameters of send/recv_init function */
    void		*buf;
    int			count;
    MPI_Datatype	dtype;
    int			peer;
    int			tag;
    MPI_Comm		comm;
    MPI_Request		*req;
#ifdef	MOD_PRDMA_NIC_SEL
    int			fidx;
    int			flag;
#endif	/* MOD_PRDMA_NIC_SEL */
#ifdef	MOD_PRDMA_SYN_MBL
    int			sndst;
#endif	/* MOD_PRDMA_SYN_MBL */
#ifdef	MOD_PRDMA_TAG_GET
    struct PrdmaReq	*tnxt[PRDMA_TAG_MAX];	/* tag next */
#endif	/* MOD_PRDMA_TAG_GET */
} PrdmaReq;

#define PRDMA_MEMID_MAX		510
#define PRDMA_MEMID_SYNC	1
#define PRDMA_MEMID_SCONST	2
#define PRDMA_MEMID_START	3
#define PRDMA_DMA_REGSSTART	(PRDMA_MEMID_SYNC + 1)
#define PRDMA_DMA_HTABSIZE	512
#define PRDMA_DMA_MAXSIZE	(16777216 - 4)	/* 2^24 - 4 */
#ifndef	MOD_PRDMA_TAG_GET
#define PRDMA_TAG_MAX		15
#define PRDMA_TAG_START		1
#endif	/* MOD_PRDMA_TAG_GET */

#define PRDMA_REQ_HTABSIZE	1024
#define PRDMA_REQ_STARTUID	10  /* uid must exclude MPI_REQUEST_NULL */
#define PRDMA_REQ_MAXREQ	1024
#define PRDMA_SYNC_NOTUSED	0x00000000
#define PRDMA_SYNC_USED		0x10000000
#define PRDMA_SYNC_EVEN		0x00000000
#define PRDMA_SYNC_ODD		0x01000000
#define PRDMA_SYNC_IDXMASK	0x00ffffff
#define PRDMA_SYNC_MARKER	0x5953434e	/* SYNC */
#define PRDMA_SYNC_FLIP		0x1
#define PRDMA_SYNC_CNSTMARKER	0
#define PRDMA_SYNC_CNSTFF_0	1
#define PRDMA_SYNC_CNSTFF_1	2
#define PRDMA_SYNC_CNSTSIZE	4
#define PRDMA_NSYNC_PERPROC	8		/* O(N) */

#define PRDMA_FIND_ANY		1
#define PRDMA_FIND_ALL		2
#define PRDMA_FIND_SOME		3


#ifdef DEBUG_ON
#define DEBUG if (1)
#else
#define DEBUG if (0)
#endif

#define PRDMA_ROUND_INC(id, max) ( ((id + 1) == max) ? 0 : (id + 1))
#define PRDMA_REQUID_INC				\
{							\
    ++_prdmaRequid;					\
    if (_prdmaRequid == 0) _prdmaRequid = PRDMA_REQ_STARTUID; \
}

#define PRDMA_INIT					\
{							\
    if (_prdmaInitialized == 0) _prdmaInit();		\
}


#define PRDMA_SET_REQ(pq, bf, ct, dt, dst, tg, cm, rq)	\
{							\
    pq->buf = bf;					\
    pq->count = ct;					\
    pq->dtype = dt;					\
    pq->peer = dst;					\
    pq->tag = tg;					\
    pq->comm = cm;					\
    pq->req = rq;					\
}

#ifdef	MOD_PRDMA_NIC_SEL
/*
 * interconnect nic selection
 */
typedef int (*prdma_nic_cb_f)(PrdmaReq *preq);

/*
 * callback functions
 */
extern prdma_nic_cb_f	_prdma_nic_init;
extern prdma_nic_cb_f	_prdma_nic_sync;
extern prdma_nic_cb_f	_prdma_nic_getf;

#endif	/* MOD_PRDMA_NIC_SEL */

#ifdef	MOD_PRDMA_SYN_MBL
/*
 * synchronization by multi-request busy loop
 */
typedef int (*prdma_syn_cb_f)(PrdmaReq *preq);
typedef int (*prdma_syn_wt_f)(int nreq, MPI_Request *reqs);

/*
 * callback functions
 */
extern prdma_syn_cb_f	_prdma_syn_send;
extern prdma_syn_wt_f	_prdma_syn_wait;

#endif	/* MOD_PRDMA_SYN_MBL */
