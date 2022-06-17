# https://ftp.gnu.org/old-gnu/Manuals/make-3.80/html_node/make_17.html
d := $(dir $(lastword $(MAKEFILE_LIST)))

$(info   d is $(d))

SRCS += $(addprefix $(d), \
		replica-run.cc replica.cc proxy-run.cc proxy.cc client-run.cc client.cc)

PROTOS += $(addprefix $(d), \
	    nezha-proto.proto)


$(b)nezha-proxy: $(o)proxy-run.o $(o)proxy.o  $(o)nezha-proto.o $(LIB-udp-socket) $(LIB-utils)

$(b)nezha-replica: $(o)replica-run.o $(o)replica.o  $(o)nezha-proto.o $(LIB-udp-socket) $(LIB-utils)

$(b)nezha-client: $(o)client-run.o $(o)client.o  $(o)nezha-proto.o $(LIB-udp-socket) $(LIB-utils)


BINS += $(b)nezha-replica $(b)nezha-proxy  $(b)nezha-client 

# include $(d)tests/Rules.mk