# #！/bin/zsh

#===================================================
#lab1_1
echo "========lab1_1==========\n"
ant runtest -Dtest=TupleTest
ant runtest -Dtest=TupleDescTest

#lab1_2
echo "========lab1_2==========\n"
ant runtest -Dtest=CatalogTest

#lab1_3

#lab1_4
echo "========lab1_4==========\n"
ant runtest -Dtest=HeapPageIdTest
ant runtest -Dtest=RecordIdTest
ant runtest -Dtest=HeapPageReadTest

#lab1_5
echo "========lab1_5==========\n"
ant runtest -Dtest=HeapFileReadTest

#lab1_6
echo "========lab1_6==========\n"
ant runsystest -Dtest=ScanTest

#===================================================
#lab2_1
echo "========lab2_1==========\n"
ant runtest -Dtest=PredicateTest
ant runtest -Dtest=JoinPredicateTest
ant runtest -Dtest=FilterTest
ant runtest -Dtest=JoinTest

ant runsystest -Dtest=FilterTest
ant runsystest -Dtest=JoinTest

#lab2_2
echo "========lab2_2==========\n"
ant runtest -Dtest=IntegerAggregatorTest
ant runtest -Dtest=StringAggregatorTest
ant runtest -Dtest=AggregateTest

#lab2_3
echo "========lab2_3==========\n"
ant runtest -Dtest=HeapPageWriteTest
ant runtest -Dtest=HeapFileWriteTest
ant runtest -Dtest=BufferPoolWriteTest

#lab2_4
echo "========lab2_4==========\n"
ant runtest -Dtest=InsertTest
ant runsystest -Dtest=InsertTest
ant runsystest -Dtest=DeleteTest

#lab2_5
echo "========lab2_5==========\n"
ant runsystest -Dtest=EvictionTest


#===================================================
#lab3_1
echo "========lab3_1==========\n"
ant runtest -Dtest=IntHistogramTest

#lab3_2
echo "========lab3_2==========\n"
ant runtest -Dtest=TableStatsTest

#lab3_3&4
echo "========lab3_3&4==========\n"
ant runtest -Dtest=JoinOptimizerTest
ant runsystest -Dtest=QueryTest

#lab3_3&4
echo "========lab3_3&4==========\n"
ant runtest -Dtest=JoinOptimizerTest
ant runsystest -Dtest=QueryTest


#===================================================
#lab4_1 and lab4_2
echo "========lab4_1 and lab4_2==========\n"
ant runtest -Dtest=LockingTest

#lab4_3 and lab4_4
echo "========lab4_3 and lab4_4==========\n"
ant runtest -Dtest=TransactionTest
ant runsystest -Dtest=AbortEvictionTest

#lab4_1
echo "========lab4_1==========\n"
ant runsystest -Dtest=TransactionTest
