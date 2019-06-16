#include <assert.h>
#include <math.h>
#include <algorithm>
#include <sstream>
#include <iostream>
#include <impala_udf/udf.h>
#include <string>     // std::to_string
#include <x86intrin.h>  // lzcnt


#include "pbf-count.h"
#include "perf-bloom-filter.h"

#define LIKELY(expr) __builtin_expect(!!(expr), 1)
#define UNLIKELY(expr) __builtin_expect(!!(expr), 0)



using namespace std;
using namespace impala_udf;

// ---------------------------------------------------------------------------
// Counting using perfect bloom filters
// ---------------------------------------------------------------------------

// Initialize the StringVal intermediate to a zero'd BloomFilter
void pbfInit(FunctionContext* ctx, StringVal* inter) {

  BigIntVal* minconst;
  BigIntVal* maxconst;
  // // HACKING AROUND IMPALA-6179
  // // read args
  minconst = reinterpret_cast<BigIntVal*>(ctx->GetConstantArg(1));
  maxconst = reinterpret_cast<BigIntVal*>(ctx->GetConstantArg(2));


  if(!minconst || !maxconst) {
    return;
    // IMPALA-6179: consts will be missing in merge init, exit and use size merging data instead
    // intermediate value will be null, check for this in merge
    // // HACKING AROUND IMPALA-6179
    // comment out the above 'return;' if testing with manual values below
  }

  // IMPALA-6179: TESTING work around: manually set the default min/max and run tests in cluster
  // minconst = new BigIntVal(0);
  // maxconst = new BigIntVal(12000000);
  // todo: test with min = max, should return 0;

  if(minconst > maxconst) {
    ctx->SetError("Bad input: Minimum cannot be less than maximum.");
    return;
  }

  uint64_t leadingzeros = __lzcnt64(maxconst->val - minconst->val);
  //minimum of 1 byte, this also handles where max=min

  if(leadingzeros > 64-3) {
    leadingzeros = 64-3;
  }

  // 64-3=61, 64 bits minus 3 to divide by 8 bits in a byte
  uint64_t bytecount = 1ull << (61-leadingzeros);
  // 1's mark relevent bit locations, subtract 1 from power of 2 to set all previous bits to 1
  uint64_t bitmask = (1ull << (64-leadingzeros))-1;

  
  // Range cap to avoid OOM (24 = 2MiB, 36 = 8GiB)
  if (leadingzeros < (64-32)) {
    ctx->SetError("Impala strings cannot exceed 1GB, cannot count greater than max-min = 2^32 (~4.2 billion).");
    return;
  }

  //Allocate Memory
  int StructureSize = sizeof(BloomFilter)+bytecount;
  inter->ptr = ctx->Allocate(StructureSize);
  //!todo check if allocation over mem_limit fails correctly, check intermediates as well, more likely there

  if (inter->ptr == NULL) {
    *inter = StringVal::null();
    ctx->SetError("BF: Init memory allocation failed.");
    return;
    //!todo ensure that null pointers do not cause errors in other stages
  }
  inter->is_null = false;
  inter->len = StructureSize;

  BloomFilter* bf = reinterpret_cast<BloomFilter*>(inter->ptr);
  bf->bytesize = bytecount;
  bf->offset = minconst->val;
  bf->bitmask = bitmask;

  bf->Reset();
  
}

void pbfUpdate(FunctionContext* ctx, const IntVal& value, const BigIntVal& minconst, const BigIntVal& maxconst, StringVal* inter) {
//void pbfUpdate(FunctionContext* ctx, const IntVal& value, StringVal* inter) {
  // as with other sql counts, null is skipped
  if(value.is_null) return;

  if(UNLIKELY(!inter->ptr)) {
    ctx->SetError("PBF Update: intermediate contains null pointer.");
    return;
  }

  BloomFilter* bf = reinterpret_cast<BloomFilter*>(inter->ptr);
  bf->Set(value.val);
  //todo, can't subtract bigint over 2^32 from int
}

StringVal pbfSerialize(FunctionContext* ctx, const StringVal& inter) {

  if (inter.is_null) {
    ctx->SetError("PBF Serialize: intermediate contains null pointer.");
    return StringVal::null();
  }
  
  StringVal serialized = StringVal::CopyFrom(ctx, inter.ptr, inter.len);

  //StringVal serialized = StringVal::CopyFrom(ctx, bf->bits, bf->bytesize);

  ctx->Free(inter.ptr);

  return serialized;
  //todo check null for failed allocation?
  //todo perf: check if copy can be removed
  //todo perf: serialize may not be needed, use string cleanup
}

void pbfMerge(FunctionContext* ctx, const StringVal& merge, StringVal* inter) {

  if(!inter->ptr) {
    if(!merge.ptr) return;  // check if merge allocation failed, if so take no action
    //merge init is empty but merge isnt, copy merge
    uint8_t* copy = ctx->Allocate(merge.len);
    memcpy(copy, merge.ptr, merge.len);
    inter->ptr = copy;
    //!todo!handle allocation failure for inter
  }
 
  //size of merge must equal size of inter
  BloomFilter* interbf = reinterpret_cast<BloomFilter*>(inter->ptr);
  BloomFilter* mergebf = reinterpret_cast<BloomFilter*>(merge.ptr); 

  interbf->Merge(mergebf->bytes);
}

BigIntVal pbfFinalize(FunctionContext* ctx, const StringVal& inter) {  
  if(!inter.ptr) {
    ctx->SetError("PBF Finalize: intermediate contains null pointer.");
    return 0;
  }
  
  BloomFilter* bf = reinterpret_cast<BloomFilter*>(inter.ptr);
  BigIntVal result = BigIntVal(bf->Count());
  //free entire structure
  ctx->Free(inter.ptr);

  return result;
}

