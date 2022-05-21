d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	address.cc udp_socket_endpoint.cc)

LIB-address :=  $(o)address.o

LIB-udp-socket := $(o)udp_socket_endpoint.o $(LIB-address)


$(info LIB-udp-socket is $(LIB-udp-socket))

# include $(d)tests/Rules.mk