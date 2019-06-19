# Perfect Bloom Filter Count
Fast counting of 'bounded and dense' integer sets in Impala. 

'Bounded' meaning there is a well defined min and max range of values and 'dense' meaning the number of values in the set is high compared to the range.  When 'density' is greater than 1 integer per 64 possible values (e.g. 3 values in the range of 1-128) this method will use less memory than storing all the individual values in a structure like a hashset. For example, a set of integers with a range between 1-64 only requires 64 bits or 8 bytes total (plus a fixed size in the struct to store the range) and if storing the numbers 1, 2, and 3 will use 1/3 the memory compared to storing all 3 of the 64bit values. In a perfect case where the range is a power of 2 and all values have been counted it's 64x more efficient than storing each value.  Performant hashsets normally require 2-4x more space than the actual values stored, meaning this approach can be >128x more space efficient than hashsets.  In addition, storing values in a perfect bloom filter requires only a few instructions compared to hashing, leading to more CPU efficiency than hashsets as well.

Where this is a good fit
- Well defined range of integers and results are counting many values.  The amount of memory used is approx 2^(ceil(log2(max-min)))/8 bytes, so a cardinality of 1 billion will use ~134MB.
- Sequential integers like auto-incremented id's.


Bad fit
- **If your values are ever outside the provided range!** Depending on how much 'room' is outside the range provided you might get a correct count or you may get a collision which will result in a lower number than the actual count (estimation will always be equal to or less than exact count).  Current implementation does not throw an error as speed was prefered to safety, can be modified to warn or error if desired.
- Only a few unique values are ever selected and the range is very large.  The function will of course work, but waste memory compared to other methods.

## Use

1. Install the impala udf development package (e.g. <http://archive.cloudera.com/cdh5/> see Impala documentation for more detail)
2. git clone \<repo url here\>
3. cd \<repo dir here\>
4. cmake .
5. make

**Int**
```sql
CREATE AGGREGATE FUNCTION CountInt(int, bigint, bigint) RETURNS BIGINT
LOCATION '/path/to/udfs/libpbfcount.so'
INIT_FN='pbfInit'
UPDATE_FN='pbfIntUpdate'
MERGE_FN='pbfMerge'
SERIALIZE_FN='pbfSerialize'
FINALIZE_FN='pbfFinalize'
INTERMEDIATE string;
```

```sql
SELECT CountInt(column_name, 1, 1000) FROM table_name 
```

**BigInt**
```sql
CREATE AGGREGATE FUNCTION CountBigInt(bigint, bigint, bigint) RETURNS BIGINT
LOCATION '/path/to/udfs/libpbfcount.so'
INIT_FN='pbfInit'
UPDATE_FN='pbfBigIntUpdate'
MERGE_FN='pbfMerge'
SERIALIZE_FN='pbfSerialize'
FINALIZE_FN='pbfFinalize'
INTERMEDIATE string;
```

```sql
SELECT CountBigInt(column_name, 1, 1000) FROM table_name 
```

### Notes
- Requires processor capable of bmi2 and abm (check 'cat /proc/cpuinfo' flags)
- Requires Impala version >2.9 for mixing intermediate and final types, could be backported but an exercise for the user.
- Current UDA test framework is not compatible with constants in the function, so they are not provided.
- Cannot count above ~4.2 billion (2^32) due to Impala intermediate limits of 1GB (due to thrift)
