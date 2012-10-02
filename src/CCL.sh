#!/bin/sh

#
# Code CLeaning script
#

FILE=${1}
if [ -z "${FILE}" ]; then
	FILE=prdma.c
fi

my_unifdef()
{
    unifdef \
	-DMOD_PRDMA_TUN_PRM \
	-DMOD_PRDMA_TUN_PRM_SYN \
	-DMOD_PRDMA_NIC_SEL \
	-UMOD_PRDMA_NIC_SEL_CD00 \
	-UMOD_PRDMA_NIC_SEL_CD01 \
	-UMOD_PRDMA_NIC_SEL_CD02 \
	-UMOD_PRDMA_NIC_SEL_CD03 \
	-DMOD_PRDMA_NIC_SEL_CD04 \
	-DMOD_PRDMA_NIC_ORD \
	-DMOD_PRDMA_NIC_ORD_BYTYPE \
	-DMOD_PRDMA_SYN_MBL \
	-DMOD_PRDMA_BSY_WAIT \
	-DMOD_PRDMA_TAG_GET \
	-UMOD_PRDMA_TAG_GET_CD00 \
	-UMOD_PRDMA_TAG_GET_CD01 \
	-DMOD_PRDMA_TAG_GET_CD02 \
	-DMOD_PRDMA_TST_NWT \
	-DMOD_PRDMA_LHP_TRC \
	-DMOD_PRDMA_LHP_TRC_TIMESYNC \
	-DMOD_PRDMA_LHP_TRC_CD00 \
	-DMOD_PRDMA_LHP_TRC_CD00A \
	-DMOD_PRDMA_LHP_TRC_CD00B \
	-DMOD_PRDMA_LHP_TRC_PST \
	-DMOD_PRDMA_LHP_TRC_TS2 \
	-DMOD_PRDMA_F2C_FIX \
	-DMOD_PRDMA_F2C_FIX_NP \
	-DMOD_PRDMA_F2C_FIX_NP2 \
	-DMOD_PRDMA_REL_INF \
	-DMOD_PRDMA_SYN_PPD \
	${1}
}

my_sed()
{
    sed \
	-e '/#[ 	]*define[ 	][ 	]*MOD_PRDMA_TUN_PRM/d' \
	-e '/#[ 	]*define[ 	][ 	]*MOD_PRDMA_TUN_PRM_SYN/d' \
	-e '/#[ 	]*define[ 	][ 	]*MOD_PRDMA_NIC_SEL/d' \
	-e '/#[ 	]*define[ 	][ 	]*MOD_PRDMA_NIC_SEL_CD00/d' \
	-e '/#[ 	]*define[ 	][ 	]*MOD_PRDMA_NIC_SEL_CD01/d' \
	-e '/#[ 	]*define[ 	][ 	]*MOD_PRDMA_NIC_SEL_CD02/d' \
	-e '/#[ 	]*define[ 	][ 	]*MOD_PRDMA_NIC_SEL_CD03/d' \
	-e '/#[ 	]*define[ 	][ 	]*MOD_PRDMA_NIC_SEL_CD04/d' \
	-e '/#[ 	]*define[ 	][ 	]*MOD_PRDMA_NIC_ORD/d' \
	-e '/#[ 	]*define[ 	][ 	]*MOD_PRDMA_NIC_ORD_BYTYPE/d' \
	-e '/#[ 	]*define[ 	][ 	]*MOD_PRDMA_SYN_MBL/d' \
	-e '/#[ 	]*define[ 	][ 	]*MOD_PRDMA_BSY_WAIT/d' \
	-e '/#[ 	]*define[ 	][ 	]*MOD_PRDMA_TAG_GET/d' \
	-e '/#[ 	]*define[ 	][ 	]*MOD_PRDMA_TAG_GET_CD00/d' \
	-e '/#[ 	]*define[ 	][ 	]*MOD_PRDMA_TAG_GET_CD01/d' \
	-e '/#[ 	]*define[ 	][ 	]*MOD_PRDMA_TAG_GET_CD02/d' \
	-e '/#[ 	]*define[ 	][ 	]*MOD_PRDMA_TST_NWT/d' \
	-e '/#[ 	]*define[ 	][ 	]*MOD_PRDMA_LHP_TRC/d' \
	-e '/#[ 	]*define[ 	][ 	]*MOD_PRDMA_LHP_TRC_TIMESYNC/d' \
	-e '/#[ 	]*define[ 	][ 	]*MOD_PRDMA_LHP_TRC_CD00/d' \
	-e '/#[ 	]*define[ 	][ 	]*MOD_PRDMA_LHP_TRC_CD00A/d' \
	-e '/#[ 	]*define[ 	][ 	]*MOD_PRDMA_LHP_TRC_CD00B/d' \
	-e '/#[ 	]*define[ 	][ 	]*MOD_PRDMA_LHP_TRC_PST/d' \
	-e '/#[ 	]*define[ 	][ 	]*MOD_PRDMA_LHP_TRC_TS2/d' \
	-e '/#[ 	]*define[ 	][ 	]*MOD_PRDMA_F2C_FIX/d' \
	-e '/#[ 	]*define[ 	][ 	]*MOD_PRDMA_F2C_FIX_NP/d' \
	-e '/#[ 	]*define[ 	][ 	]*MOD_PRDMA_F2C_FIX_NP2/d' \
	-e '/#[ 	]*define[ 	][ 	]*MOD_PRDMA_REL_INF/d' \
	-e '/#[ 	]*define[ 	][ 	]*MOD_PRDMA_SYN_PPD/d' \
	${1}
}

my_unifdef	${FILE}		>._ccl-01
my_sed		._ccl-01	>._ccl-02

cat ._ccl-02

rm -f ._ccl-01 ._ccl-02

exit 0

