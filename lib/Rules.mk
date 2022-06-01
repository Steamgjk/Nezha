d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	address.cc utils.cc udp_socket_endpoint.cc)

LIB-address :=  $(o)address.o

LIB-utils := $(o)utils.o

LIB-udp-socket := $(o)udp_socket_endpoint.o $(LIB-address) $(LIB-utils)


$(info LIB-udp-socket is $(LIB-udp-socket)) 

# include $(d)tests/Rules.mk