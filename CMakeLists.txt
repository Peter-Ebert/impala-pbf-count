
cmake_minimum_required(VERSION 2.6)

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "build")
# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "build")

find_program(CLANG_EXECUTABLE clang++)

# -mbmi2 -mpopcnt
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -ggdb -msse4.2 -mlzcnt -O3")

# Function to generate rule to cross compile a source file to an IR module.
# This should be called with the .cc src file and it will generate a
# src-file-ir target that can be built.
# e.g. COMPILE_TO_IR(test.cc) generates the "test-ir" make target.
# Disable compiler optimizations because generated IR is optimized at runtime
set(IR_COMPILE_FLAGS "-emit-llvm" "-c" "-msse4.2" "-mlzcnt") 
# "-mbmi2"
function(COMPILE_TO_IR SRC_FILE)
  get_filename_component(BASE_NAME ${SRC_FILE} NAME_WE)
  set(OUTPUT_FILE "build/${BASE_NAME}.ll")
  add_custom_command(
    OUTPUT ${OUTPUT_FILE}
    COMMAND ${CLANG_EXECUTABLE} ${IR_COMPILE_FLAGS} ${SRC_FILE} -o ${OUTPUT_FILE}
    DEPENDS ${SRC_FILE})
  add_custom_target(${BASE_NAME}-ir ALL DEPENDS ${OUTPUT_FILE})
endfunction(COMPILE_TO_IR)

# Build the UDA/UDFs into a shared library.
add_library(pbfcount SHARED pbf-count.cc)

# Custom targest to cross compile UDA/UDF to ir
if (CLANG_EXECUTABLE)
  COMPILE_TO_IR(pbf-count.cc)
endif(CLANG_EXECUTABLE)

# This is an example of how to use the test harness to help develop UDF and UDAs.
target_link_libraries(pbfcount ImpalaUdf)

