if(${EXIT_TIME_DESTRUCTORS_WARNING})
  set(CMAKE_CXX_FLAGS_DEBUG
      "${CMAKE_CXX_FLAGS_DEBUG} -Wno-exit-time-destructors")
endif()

add_library_unity(
  duckdb_common_types_row
  OBJECT
  block_iterator.cpp
  partitioned_tuple_data.cpp
  row_data_collection.cpp
  row_data_collection_scanner.cpp
  row_layout.cpp
  tuple_data_allocator.cpp
  tuple_data_collection.cpp
  tuple_data_iterator.cpp
  tuple_data_layout.cpp
  tuple_data_scatter_gather.cpp
  tuple_data_segment.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_common_types_row>
    PARENT_SCOPE)
