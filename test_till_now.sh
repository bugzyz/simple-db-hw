#！/bin/zsh

#1
ant runtest -Dtest=TupleTest
ant runtest -Dtest=TupleDescTest

#2
ant runtest -Dtest=CatalogTest

#3

#4
ant runtest -Dtest=HeapPageIdTest
ant runtest -Dtest=RecordIdTest
ant runtest -Dtest=HeapPageReadTest

#5
ant runtest -Dtest=HeapFileReadTest

#6
ant runsystest -Dtest=ScanTest
