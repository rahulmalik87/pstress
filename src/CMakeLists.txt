SET (BINARY_NAME "pstress")
ADD_SUBDIRECTORY(third_party)
INCLUDE_DIRECTORIES(third_party/inih++/include third_party/rapidjson/include)
INCLUDE_DIRECTORIES( ${DB_INCLUDE_DIR} )
ADD_EXECUTABLE(${BINARY_NAME}-${PSTRESS_EXT} pstress.cpp help.cpp node.cpp thread.cpp random_test.cpp json.cpp table.cpp run_method.cpp)
TARGET_LINK_LIBRARIES( ${BINARY_NAME}-${PSTRESS_EXT} ${DB_LIBRARY} ${OTHER_LIBS} inih++)

# Copy all .sql files
FILE(GLOB SQL_FILES "*.sql")
FILE(COPY ${SQL_FILES} DESTINATION .)
FILE(GLOB TEXT_FILES "*.txt")
FILE(COPY ${TEXT_FILES} DESTINATION .)

# Install all .sql files to the bin directory
INSTALL(FILES ${SQL_FILES} DESTINATION bin)
INSTALL(TARGETS ${BINARY_NAME}-${PSTRESS_EXT} DESTINATION bin)

SET( CMAKE_EXPORT_COMPILE_COMMANDS ON )
