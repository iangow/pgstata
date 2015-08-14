PLUGIN_NAME=pg
PLUGIN_SYS=APPLEMAC

# Set this to where you want 'make install' to install to.

INSTALL_LOCATION=~/Library/Application\ Support/Stata/ado/personal
INSTALL_LIST=pg.plugin pgload.ado pgload.hlp

# Distribution in tarball form
DIST_NAME=pgload
DIST_VERSION=0.1
DIST_LIST=pg.c Makefile pgload.ado pgload.hlp LICENSE-2.0.txt lgpl-3.0.txt README gpl-3.0.txt


# Set this to the location of your downloaded stplugin.h and stplugin.c
# Get these from http://www.stata.com/plugins/ .

STATAPLUG_INC=..
STATAPLUG_C=$(STATAPLUG_INC)/stplugin.c

# Settings for building under Debian GNU/Linux "Sarge" (3.1) on amd64 (x86-64)

PQ_INC=/opt/local/include/postgresql94
PG_SERVER_INC=/opt/local/include/postgresql94/server

# Different systems will require different linker options. The following should
# work for both ia32 (x86-32, i386) and amd64 (x86-64).

LDOPTS=-shared -fPIC -l pq

############################################################################

CC=gcc
CFLAGS=-std=c99 -Wall -pedantic -DSYSTEM=$(PLUGIN_SYS)	\
	-I $(PQ_INC) -I $(PG_SERVER_INC)		\
	-I $(STATAPLUG_INC)

############################################################################

all: $(PLUGIN_NAME).plugin

$(PLUGIN_NAME).plugin: pg.c $(STATAPLUG_C) Makefile
	$(CC) -DSYSTEM=APPLEMAC $(STATAPLUG_C) $< -o $@ $(CFLAGS) $(LDOPTS)

clean:
	rm -f $(PLUGIN_NAME).plugin

distclean: clean
	rm -f *.tar *.tar.gz

maintclean: distclean changelog.txt

changelog.txt: $(DIST_LIST)
	test -d CVS && cvs2cl -f $@

dist: distclean $(DIST_LIST) changelog.txt
	mkdir -p $(DIST_NAME)-$(DIST_VERSION)
	cp -a $(DIST_LIST) changelog.txt $(DIST_NAME)-$(DIST_VERSION)
	tar -cpf $(DIST_NAME)-$(DIST_VERSION).tar $(DIST_NAME)-$(DIST_VERSION)
	rm -fr $(DIST_NAME)-$(DIST_VERSION)
	gzip $(DIST_NAME)-$(DIST_VERSION).tar

tarball: dist

install: $(PLUGIN_NAME).plugin
	mkdir -p ${INSTALL_LOCATION} && install ${INSTALL_LIST} ${INSTALL_LOCATION}
