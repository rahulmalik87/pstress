SET (BINARY_NAME "pstress")
ADD_SUBDIRECTORY(third_party)
INCLUDE_DIRECTORIES(third_party/inih++/include third_party/rapidjson/include)
if(CLICKHOUSE)
  INCLUDE_DIRECTORIES(SYSTEM ${DB_INCLUDE_DIR})
else()
  INCLUDE_DIRECTORIES( ${DB_INCLUDE_DIR} )
endif()
ADD_EXECUTABLE(${BINARY_NAME}-${PSTRESS_EXT} pstress.cpp help.cpp node.cpp thread.cpp random_test.cpp json.cpp table.cpp run_method.cpp ch_verify.cpp)
if(CLICKHOUSE)
  TARGET_LINK_LIBRARIES( ${BINARY_NAME}-${PSTRESS_EXT} ${DB_LIBRARY} ${OTHER_LIBS} inih++
    /usr/local/lib/libcityhash.a /usr/local/lib/liblz4.a /usr/local/lib/libzstdstatic.a)
  # Statically link libstdc++ and libgcc so the binary runs on any Linux
  # without needing matching runtime library versions installed.
  TARGET_LINK_OPTIONS( ${BINARY_NAME}-${PSTRESS_EXT} PRIVATE -static-libstdc++ -static-libgcc)
else()
  TARGET_LINK_LIBRARIES( ${BINARY_NAME}-${PSTRESS_EXT} ${DB_LIBRARY} ${OTHER_LIBS} inih++)
endif()

# Copy all .sql and .txt files next to the binary, which is where pstress looks
# them up at runtime (grammar file, dictionary, settings pool).
FILE(GLOB SQL_FILES "*.sql")
FILE(COPY ${SQL_FILES} DESTINATION .)
FILE(GLOB TEXT_FILES "*.txt")
FILE(COPY ${TEXT_FILES} DESTINATION .)

# FILE(COPY) above runs at configure time only, so editing a grammar or
# settings file left a stale copy in the build dir until the next cmake run and
# the binary kept reading the old one. A custom target is always out of date,
# so this re-copies on every make. Adding a brand new file still needs a cmake
# re-run, because the globs above are evaluated at configure time.
ADD_CUSTOM_TARGET(copy_data_files ALL
  COMMAND ${CMAKE_COMMAND} -E copy_if_different ${SQL_FILES} ${TEXT_FILES}
          ${CMAKE_CURRENT_BINARY_DIR}
  COMMENT "Refreshing .sql and .txt files in the build directory")

# Install all .sql and .txt files to the bin directory. The .txt files are the
# dictionary and the ClickHouse table settings pool, both looked up next to the
# binary at runtime.
INSTALL(FILES ${SQL_FILES} ${TEXT_FILES} DESTINATION bin)
INSTALL(TARGETS ${BINARY_NAME}-${PSTRESS_EXT} DESTINATION bin)

SET( CMAKE_EXPORT_COMPILE_COMMANDS ON )
