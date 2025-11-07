# Usage: AllReplicate <input> <output> <gridRows> <gridCols> <totalWidth> <totalHeight>
hadoop jar spatialjoin.jar AllReplicate /input/data.txt /output_all 2 2 100 100

# Check output:
hadoop fs -cat /output_all/part-r-00000

# Usage: CRep <input> <output> <gridRows> <gridCols> <totalWidth> <totalHeight>
hadoop jar spatialjoin.jar CRep /input/data.txt /output_crep 2 2 100 100