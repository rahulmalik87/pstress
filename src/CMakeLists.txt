SET (BINARY_NAME "pstress")
ADD_SUBDIRECTORY(third_party)
INCLUDE_DIRECTORIES(third_party/inih++/include third_party/rapidjson/include)
if(CLICKHOUSE)
  INCLUDE_DIRECTORIES(SYSTEM ${DB_INCLUDE_DIR})
else()
  INCLUDE_DIRECTORIES( ${DB_INCLUDE_DIR} )
endif()
ADD_EXECUTABLE(${BINARY_NAME}-${PSTRESS_EXT} pstress.cpp help.cpp node.cpp thread.cpp random_test.cpp json.cpp table.cpp run_method.cpp)
if(CLICKHOUSE)
  TARGET_LINK_LIBRARIES( ${BINARY_NAME}-${PSTRESS_EXT} ${DB_LIBRARY} ${OTHER_LIBS} inih++
    /usr/local/lib/libcityhash.a /usr/local/lib/liblz4.a /usr/local/lib/libzstdstatic.a)
else()
  TARGET_LINK_LIBRARIES( ${BINARY_NAME}-${PSTRESS_EXT} ${DB_LIBRARY} ${OTHER_LIBS} inih++)
endif()

# Copy all .sql files
FILE(GLOB SQL_FILES "*.sql")
FILE(COPY ${SQL_FILES} DESTINATION .)
FILE(GLOB TEXT_FILES "*.txt")
FILE(COPY ${TEXT_FILES} DESTINATION .)

# Install all .sql files to the bin directory
INSTALL(FILES ${SQL_FILES} DESTINATION bin)
INSTALL(TARGETS ${BINARY_NAME}-${PSTRESS_EXT} DESTINATION bin)

SET( CMAKE_EXPORT_COMPILE_COMMANDS ON )
