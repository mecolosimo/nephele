
if COND_HAVE_MPI
bin_PROGRAMS = panjo panjo-serial
panjo_SOURCES = panjo.c
noinst_PROGRAMS = panjo
endif

EXTRA_DIST=lltemplate

panjo:	panjo.c 
	$(MPICC) $(DEFS) $(DEFAULT_INCLUDES) $(INCLUDES) \
		$(AM_CPPFLAGS) $(CPPFLAGS) $(AM_CFLAGS) $(CFLAGS) \
		-o $@ *.c \
		$(AM_LDFLAGS) $(LDFLAGS) $(LDADD) $(LIBS) -D__USE_MPI__


panjo-serial:	panjo.c 
	$(CC) $(DEFS) $(DEFAULT_INCLUDES) $(INCLUDES) \
		$(AM_CPPFLAGS) $(CPPFLAGS) $(AM_CFLAGS) $(CFLAGS) \
		-o $@ *.c \
		$(AM_LDFLAGS) $(LDFLAGS) $(LDADD) $(LIBS)

