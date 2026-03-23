# Options for building Pstress with database support
option(MYSQL "Build Pstress with MySQL support" OFF)
option(DUCKDB "Build Pstress with DuckDB support" OFF)
option(CLICKHOUSE "Build Pstress with ClickHouse support" OFF)

# Set fork-specific details
if(MYSQL)
  set(LIB_NAMES mysqlclient mysqlclient_r)
  set(PSTRESS_EXT "ms")
  set(FORK "MySQL")
  set(HEADER_NAME "mysql.h")
  set(LIB_SUFFIX "mysql")
  add_definitions(-DUSE_MYSQL)
  add_definitions(-DMAXPACKET)
  add_definitions(-DFORK="MySQL")
elseif(DUCKDB)
  set(LIB_NAMES libduckdb duckdb)
  set(PSTRESS_EXT "dd")
  set(FORK "DuckDB")
  set(HEADER_NAME "duckdb.h")
  set(LIB_SUFFIX "")
  add_definitions(-DUSE_DUCKDB)
  add_definitions(-DFORK="DuckDB")
elseif(CLICKHOUSE)
  set(LIB_NAMES clickhouse-cpp-lib)
  set(PSTRESS_EXT "ch")
  set(FORK "ClickHouse")
  set(HEADER_NAME "clickhouse/client.h")
  set(LIB_SUFFIX "")
  add_definitions(-DUSE_CLICKHOUSE)
  add_definitions(-DFORK="ClickHouse")
else()
  message(FATAL_ERROR "Please enable either MYSQL, DUCKDB, or CLICKHOUSE")
endif()

# Check for user-provided paths
if(MYSQL AND MYSQL_INCLUDE_DIR AND MYSQL_LIBRARY)
  set(DB_INCLUDE_DIR ${MYSQL_INCLUDE_DIR} CACHE PATH "Path to MySQL include directory")
  set(DB_LIBRARY ${MYSQL_LIBRARY} CACHE FILEPATH "Path to MySQL library")
  set(DB_FOUND TRUE)
elseif(DUCKDB AND DUCKDB_INCLUDE_DIR AND DUCKDB_LIBRARY)
  set(DB_INCLUDE_DIR ${DUCKDB_INCLUDE_DIR} CACHE PATH "Path to DuckDB include directory")
  set(DB_LIBRARY ${DUCKDB_LIBRARY} CACHE FILEPATH "Path to DuckDB library")
  set(DB_FOUND TRUE)
elseif(CLICKHOUSE AND CLICKHOUSE_INCLUDE_DIR AND CLICKHOUSE_LIBRARY)
  set(DB_INCLUDE_DIR ${CLICKHOUSE_INCLUDE_DIR} CACHE PATH "Path to ClickHouse include directory")
  set(DB_LIBRARY ${CLICKHOUSE_LIBRARY} CACHE FILEPATH "Path to ClickHouse library")
  set(DB_FOUND TRUE)
else()
  # Validate BASEDIR if provided
  if(BASEDIR)
    if(NOT EXISTS ${BASEDIR})
      message(FATAL_ERROR "Directory ${BASEDIR} doesn't exist. Check the path!")
    endif()
    message(STATUS "BASEDIR is set, looking for ${FORK} in ${BASEDIR}")
  endif()

  # Use static libraries if requested
  if(STATIC_LIBRARY)
    set(CMAKE_FIND_LIBRARY_SUFFIXES ".a")
  endif()

  # Define search paths
  set(INCLUDE_PATHS
    ${BASEDIR}/include
    ${BASEDIR}/include/mysql
    /usr/local/include/mysql
    /usr/include/mysql
    /usr/local/mysql/include
  )
  set(LIBRARY_PATHS
    ${BASEDIR}/lib
    ${BASEDIR}/lib64
    /usr/lib
    /usr/local/lib
    /usr/lib/x86_64-linux-gnu
    /usr/lib/i386-linux-gnu
    /usr/lib64
    /usr/local/mysql/lib
  )

  # Find include directory and library if not already set
  find_path(DB_INCLUDE_DIR ${HEADER_NAME} PATHS ${INCLUDE_PATHS} NO_CMAKE_SYSTEM_PATH)
  if(NOT DB_LIBRARY)
    find_library(DB_LIBRARY NAMES ${LIB_NAMES} PATHS ${LIBRARY_PATHS} PATH_SUFFIXES ${LIB_SUFFIX} NO_CMAKE_SYSTEM_PATH)
  endif()
endif()

# Check if found
if(DB_INCLUDE_DIR AND DB_LIBRARY)
  set(DB_FOUND TRUE)
  set(DB_LIBRARIES ${DB_LIBRARY})
  message(STATUS "DB_LIBRARY is set to: ${DB_LIBRARY}")  # Debug output
  message(STATUS "Found ${FORK} library: ${DB_LIBRARY}")
  message(STATUS "Found ${FORK} include directory: ${DB_INCLUDE_DIR}")
else()
  if(NOT DB_INCLUDE_DIR)
    message(STATUS "Could not find ${FORK} header: ${HEADER_NAME} in ${INCLUDE_PATHS}")
  endif()
  if(NOT DB_LIBRARY)
    message(STATUS "Could not find ${FORK} library named ${LIB_NAMES} in ${LIBRARY_PATHS}")
  endif()
  message(FATAL_ERROR "Could NOT find ${FORK} library")
endif()

# Mark variables as advanced
mark_as_advanced(DB_LIBRARY DB_INCLUDE_DIR)

# Set runtime path for DuckDB using detected library location
if(DUCKDB AND DB_LIBRARY)
  get_filename_component(DUCKDB_LIB_DIR ${DB_LIBRARY} DIRECTORY)
  if(NOT "${CMAKE_EXE_LINKER_FLAGS}" MATCHES "-Wl,-rpath,${DUCKDB_LIB_DIR}")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-rpath,${DUCKDB_LIB_DIR}" CACHE STRING "Linker flags" FORCE)
    message(STATUS "Added DuckDB rpath: ${DUCKDB_LIB_DIR}")
  endif()
endif()

# Set runtime path for ClickHouse using detected library location
if(CLICKHOUSE AND DB_LIBRARY)
  get_filename_component(CLICKHOUSE_LIB_DIR ${DB_LIBRARY} DIRECTORY)
  if(NOT "${CMAKE_EXE_LINKER_FLAGS}" MATCHES "-Wl,-rpath,${CLICKHOUSE_LIB_DIR}")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-rpath,${CLICKHOUSE_LIB_DIR}" CACHE STRING "Linker flags" FORCE)
    message(STATUS "Added ClickHouse rpath: ${CLICKHOUSE_LIB_DIR}")
  endif()
endif()
