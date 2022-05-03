# https://ftp.gnu.org/old-gnu/Manuals/make-3.80/html_node/make_17.html
d := $(dir $(lastword $(MAKEFILE_LIST)))

$(info   d is $(d))

SRCS += $(addprefix $(d), \
	main.cc foo.cc bar.cc)

# PROTOS += $(addprefix $(d), \
# 	    vr-proto.proto)


OBJS-foo := $(o)foo.o 

OBJS-bar := $(o)bar.o 


$(d)main: $(o)main.o  $(OBJS-foo) $(OBJS-bar)


BINS += $(d)main 

# include $(d)tests/Rules.mk