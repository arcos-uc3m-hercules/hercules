
set_source_files_properties(../src/utils.c ../src/comms.c ../src/crc.c ../src/imss.c ../src/shared_memory.c PROPERTIES LANGUAGE CXX )
set_source_files_properties(../src/memalloc.c ../src/policies.c ../src/workers.c ../src/arg_parser.c ../src/map_server_eps.cpp PROPERTIES LANGUAGE CXX )
set_source_files_properties(./server.c ./release_imss.c PROPERTIES LANGUAGE CXX )
#set_source_files_properties(./monitor.c ./release_imss.c PROPERTIES LANGUAGE CXX )

ADD_EXECUTABLE( hercules_server
		./server.c
		../src/utils.c
		../src/map_ep.cpp
		../src/comms.c
		../src/queue.c
		../src/workers.c
		../src/memalloc.c
		../src/memalloc.c
		../src/records.cpp
		../src/arg_parser.c
		../src/slog.c
		../src/map_server_eps.cpp
		../src/cfg_parse.c
		../src/crc.c
		../src/imss.c
		../src/shared_memory.c
		../src/policies.c
		../src/hercules.cpp
		../src/hierarchical_records.cpp
)

ADD_LIBRARY( imss_static STATIC
		../src/crc.c
		../src/imss.c
		../src/comms.c
		../src/workers.c
		../src/queue.c
		../src/utils.c
		../src/hercules.cpp
		../src/policies.c
		../src/records.cpp
		../src/map_ep.cpp
		../src/slog.c
		../src/map_server_eps.cpp
		../src/shared_memory.c
)

ADD_LIBRARY( imss_shared SHARED
		../src/crc.c
		../src/imss.c
		../src/comms.c
		../src/queue.c
		../src/workers.c
		../src/utils.c
		../src/hercules.cpp
		../src/policies.c
		../src/records.cpp
		../src/map_ep.cpp
		../src/slog.c
		../src/map_server_eps.cpp
		../src/shared_memory.c
		../src/hierarchical_records.cpp
)


SET(IMSS_INC "-I${CMAKE_SOURCE_DIR}/include")
SET(IMSS_LIB "-L${CMAKE_SOURCE_DIR}/build/app")

target_link_directories(hercules_server PUBLIC ${UCX_LIBRARIES})
#target_link_directories(monitor PUBLIC ${UCX_LIBRARIES})


TARGET_LINK_LIBRARIES(imss_static ${CMAKE_THREAD_LIBS_INIT} ${MPI_LIBRARIES} ${GLIB_LIBRARIES} ucx::ucp ucx::ucs ${JEMALLOC_LIBRARIES} )
TARGET_LINK_LIBRARIES(imss_shared ${CMAKE_THREAD_LIBS_INIT}  ${MPI_LIBRARIES} ${GLIB_LIBRARIES} dl ucx::ucp ucx::ucs ${JEMALLOC_LIBRARIES} )
TARGET_LINK_LIBRARIES(hercules_server ${CMAKE_THREAD_LIBS_INIT}  ${MPI_LIBRARIES} ${GLIB_LIBRARIES} ${PCRE_LIBRARY} ucx::ucp ucx::ucs ${JEMALLOC_LIBRARIES} )
#TARGET_LINK_LIBRARIES(monitor ${CMAKE_THREAD_LIBS_INIT}  ${MPI_LIBRARIES} ${GLIB_LIBRARIES} ${PCRE_LIBRARY} ucx::ucp ucx::ucs ${JEMALLOC_LIBRARIES} )

install(TARGETS hercules_server DESTINATION bin)
#install(TARGETS monitor DESTINATION bin)
install(TARGETS imss_static DESTINATION lib)
install(TARGETS imss_shared DESTINATION lib)

install(FILES ../scripts/hercules DESTINATION bin
        PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ
                    GROUP_EXECUTE GROUP_READ)

install(FILES ../scripts/check-servers.sh DESTINATION bin
        PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ
                    GROUP_EXECUTE GROUP_READ)

install(FILES ../conf/hercules.conf.sample DESTINATION conf
        PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ
                    GROUP_EXECUTE GROUP_READ)
