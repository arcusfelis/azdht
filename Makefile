PROJECT = azdht

DEPS = gproc lager hackney

dep_gproc = https://github.com/uwiger/gproc.git master
dep_lager = https://github.com/basho/lager.git 2.0.0
dep_hackney = https://github.com/benoitc/hackney.git master

ERLC_OPTS = +debug_info +'{parse_transform, lager_transform}'

include erlang.mk
