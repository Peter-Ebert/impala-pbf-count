#ifndef SAMPLES_UDA_H
#define SAMPLES_UDA_H


#include <impala_udf/udf.h>

using namespace impala_udf;

void pbfInit(FunctionContext* ctx, StringVal* inter);
void pbfIntUpdate(FunctionContext* ctx, const IntVal& value, const BigIntVal& minconst, const BigIntVal& maxconst, StringVal* inter);
void pbfBigIntUpdate(FunctionContext* ctx, const IntVal& value, const BigIntVal& minconst, const BigIntVal& maxconst, StringVal* inter);
StringVal pbfSerialize(FunctionContext* ctx, const StringVal& inter);
void pbfMerge(FunctionContext* ctx, const StringVal& merge, StringVal* inter);
BigIntVal pbfFinalize(FunctionContext* ctx, const StringVal& inter);


#endif
