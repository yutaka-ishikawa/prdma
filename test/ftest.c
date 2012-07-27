#include "../src/prdma.h"

int
FJMPI_Rdma_reg_mem(int id, char *buf, int size)
{
    fprintf(stderr, "fake FJMPI_Rdma_reg_mem(%d, %p, %d)\n", id, buf, size);
    return 0;
}

int
FJMPI_Rdma_put(int dest, int tag,
	       uint64_t laddr, uint64_t raddr,
	       int size, int flag)
{
    fprintf(stderr, "fake FJMPI_Rdma_put(%d, %d, %llx, %llx, %d, %d)\n",
	    dest, tag, laddr, raddr, size, flag);
    return 0;
}

int
FJMPI_Rdma_get_remote_addr(int remote, int id)
{
    fprintf(stderr, "fake FJMPI_Rdma_get_remote_addr(%d, %d)\n", remote, id);
    return 12345;
}

int
FJMPI_Rdma_poll_cq(int nic, struct FJMPI_Rdma_cq *cq)
{
    fprintf(stderr, "fake FJMPI_Rdma_poll_cq(%d, %p)\n", nic, cq);
    return 0;
}


int
FJMPI_Rdma_init()
{
    fprintf(stderr, "fake FJMPI_Rdma_init()\n");
    return 0;
}

int
FJMPI_Rdma_finalize()
{
    fprintf(stderr, "fake FJMPI_Rdma_Finalize()\n");
    return 0;
}

