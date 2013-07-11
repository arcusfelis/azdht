PROJECT = azdht

DEPS = gproc lager

dep_gproc = https://github.com/uwiger/gproc.git master
dep_lager = https://github.com/basho/lager.git 2.0.0

ERLC_OPTS = +debug_info +'{parse_transform, lager_transform}'

include erlang.mk
