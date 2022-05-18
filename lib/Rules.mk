d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	address.cc udp_socket_endpont.cc)

LIB-address :=  $(o)address.o

LIB-udp-socket := $(o)udp_socket_endpont.o $(LIB-address)


# include $(d)tests/Rules.mk