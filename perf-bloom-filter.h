#include <bitset>
#include <iostream>
#include <x86intrin.h> // popcnt

#ifndef BLOOM_FILTER_H
#define BLOOM_FILTER_H

#define LIKELY(expr) __builtin_expect(!!(expr), 1)
#define UNLIKELY(expr) __builtin_expect(!!(expr), 0)

// setting to 64 bit / alignment did not change performance, sticking with bytes for now, revisit later
const uint8_t CHUNK_SIZE = 8;//64
const uint8_t CHUNK_BIT_SIZE = 3;//6
const uint8_t WORD_SIZE = 8;
// does not correct for datatypes

using namespace impala_udf;
using namespace std;

struct BloomFilter {
  uint64_t bytesize;
  int64_t offset;
  uint64_t bitmask; //can use _blsmsk_u64 
  uint8_t bytes[];
  //todo: might be cleaner as class with constructor that manages memory

  void Set(int64_t position) {
    // note: will not warn if values are outside of range to avoid performance impact
    uint64_t position_offset = position-offset & bitmask; 
    uint8_t* chunk = &bytes[ position_offset >> CHUNK_BIT_SIZE ];
    *chunk |= 1 << (position_offset & ((1 << CHUNK_BIT_SIZE)-1));
    //todo: performance analysis
  }

  void Reset() {
    memset(bytes, 0, bytesize);
  }

  uint64_t Count() {
    uint64_t ones = 0;
    uint64_t* words = reinterpret_cast<uint64_t*>(bytes);

    int wordsize = bytesize/WORD_SIZE;

    for(int i=0; i < wordsize; i++) {
      ones += _mm_popcnt_u64(words[i]);
      // if (ones > 0) {
      //   bitset<64> bs = words[i];
      //   cout << "current word:" << bs << endl;
      //   return ones;
      // };
    }

    return ones;
  }

  void Merge(uint8_t* mergebytes) {
    uint64_t* mergewords = reinterpret_cast<uint64_t*>(mergebytes);
    uint64_t* mywords = reinterpret_cast<uint64_t*>(bytes);

    //cout << _mm_popcnt_u64(mergewords[0]);

    int wordsize = bytesize/WORD_SIZE;
    for(int i=0; i < wordsize; i++) {
      mywords[i] |= mergewords[i];
    }

  }

};


#endif