# https://ftp.gnu.org/old-gnu/Manuals/make-3.80/html_node/make_17.html
d := $(dir $(lastword $(MAKEFILE_LIST)))

$(info   d is $(d))

SRCS += $(addprefix $(d), \
		replica-run.cc replica.cc)

PROTOS += $(addprefix $(d), \
	    nezha-proto.proto)



$(b)nezha-replica: $(o)replica-run.o $(o)replica.o  $(o)nezha-proto.o $(LIB-udp-socket) 


BINS += $(b)nezha-replica 

# include $(d)tests/Rules.mk