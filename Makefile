parseTree: parseTree.o heap_storage.o
	g++ -L/usr/local/db6/lib -o $(EXECUTABLE_FILE) parseTree.o heap_storage.o -ldb_cxx -lsqlparser

parseTree.o : heap_storage.h storage_engine.h
heap_storage.o: heap_storage.h storage_engine.h

%.o : %.cpp
	g++ -I/usr/local/db6/include -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -fpermissive -O3 -std=c++11 -c -o $@ $<

clean : 
	rm -f parseTree *.o
