DATASETS = testds test98 test80 test78 test74 test254
SERVICE_DATASETS = error duplicates
TESTDIR = ../test

conf:
	arki-mergeconf $(DATASETS) $(SERVICE_DATASETS) > $@ || rm -f $@

clean:
	for ds in $(DATASETS) $(SERVICE_DATASETS); do \
		arki-check --remove-all -f $$ds ; \
	done
	rm -f conf

# import_jpeg:
# 	for file in test/data/fixture.jpeg/*.jpg.gz; do


import_grib: conf
	arki-scan --dispatch=conf --yaml $(TESTDIR)/data/fixture.grib1
