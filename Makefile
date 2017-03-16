# paths
OBJECT_DIR = obj
BINARY_DIR = bin
HEADERS_PATH = headers

#compile/link options
#SYSLIBS = -L/usr/local/lib -L/usr/lib64  -lboost_system -lboost_program_options -lboost_thread -lz -lrt -lboost_thread-mt
SYSLIBS = -L/usr/local/lib -L/usr/lib  -lboost_system -lboost_program_options -lboost_thread -lz -lrt -lm -lpthread
CXX?= g++
#CXXFLAGS?= -O3 -DNDEBUG -Wall -Wno-unused-function -I./$(HEADERS_PATH)
CXXFLAGS?= -O3 -DDEBUG -Wall -Wno-unused-function -I./$(HEADERS_PATH)
CXXFLAGS+= -Wfatal-errors -fpermissive

# make selections
CONVERT_SRC = convert.o process_edgelist.o process_adjlist.o radix_sort.o process_in_edge.o k_way_merge.o
CONVERT_OBJS= $(addprefix $(OBJECT_DIR)/, $(CONVERT_SRC))
CONVERT_TARGET=$(BINARY_DIR)/convert

TEST_SRC = test.o
TEST_OBJS= $(addprefix $(OBJECT_DIR)/, $(TEST_SRC))
TEST_TARGET=$(BINARY_DIR)/test

FOG_HEADERS = types.hpp config.hpp print_debug.hpp disk_thread.hpp index_vert_array.hpp fog_engine.hpp options_utils.h config_parse.h bitmap.hpp     cpu_thread.hpp fog_adapter.h
FOG_REL_HEADERS = $(addprefix $(HEADERS_PATH)/, $(FOG_HEADERS))

APPS_SRC = $(shell find application/ -name '*.cpp')
#APPS_OBJ = $(addprefix $(OBJECT_DIR)/, $(notdir $(APPS_SRC:.cpp=.o)))
APPS_OBJ =  $(notdir $(APPS_SRC:.cpp=.o))
#APPS_TARGET = $(addprefix $(BINARY_DIR)/, $(basename $(notdir $(APPS_SRC)))) 
APPS_TARGET = $(basename $(notdir $(APPS_SRC))) 

all: $(CONVERT_TARGET) $(APPS_TARGET)
	@echo $(APPS_OBJ)

#dependencies
$(OBJECT_DIR):
	mkdir ./obj

$(BINARY_DIR):
	mkdir ./bin

#following lines defined for convert
$(OBJECT_DIR)/convert.o:convert/convert.cpp 
	$(CXX) $(CXXFLAGS) -c -o $@ $<

$(OBJECT_DIR)/radix_sort.o:convert/radix_sort.cpp
	$(CXX) $(CXXFLAGS) -c -o $@ $<

$(OBJECT_DIR)/process_edgelist.o:convert/process_edgelist.cpp 
	$(CXX) $(CXXFLAGS) -c -o $@ $<

$(OBJECT_DIR)/process_adjlist.o:convert/process_adjlist.cpp 
	$(CXX) $(CXXFLAGS) -c -o $@ $<

$(OBJECT_DIR)/process_in_edge.o:convert/process_in_edge.cpp
	$(CXX) $(CXXFLAGS) -c -o $@ $<

$(OBJECT_DIR)/k_way_merge.o:convert/k_way_merge.cpp
	$(CXX) $(CXXFLAGS) -c -o $@ $<

$(BINARY_DIR)/convert: $(CONVERT_OBJS)
	$(CXX) -o $@ $(CONVERT_OBJS) $(SYSLIBS)

$(CONVERT_OBJS): |$(OBJECT_DIR)
$(CONVERT_TARGET): |$(BINARY_DIR)

#following lines defined for testing
$(OBJECT_DIR)/test.o:convert/test.cpp 
	$(CXX) $(CXXFLAGS) -c -o $@ $<

$(BINARY_DIR)/test: $(TEST_OBJS)
	$(CXX) -o $@ $(TEST_OBJS) $(SYSLIBS)

$(TEST_OBJS): |$(OBJECT_DIR)
$(TEST_TARGET): |$(BINARY_DIR)

#following lines defined for fog


$(OBJECT_DIR)/fog_engine.o:fogsrc/fog_engine.cpp $(FOG_REL_HEADERS)
	$(CXX) $(CXXFLAGS) -c -o $@ fogsrc/fog_engine.cpp
$(OBJECT_DIR)/bitmap.o:fogsrc/bitmap.cpp $(FOG_REL_HEADERS)
	$(CXX) $(CXXFLAGS) -c -o $@ fogsrc/bitmap.cpp
$(OBJECT_DIR)/disk_thread.o:fogsrc/disk_thread.cpp $(FOG_REL_HEADERS)
	$(CXX) $(CXXFLAGS) -c -o $@ fogsrc/disk_thread.cpp
$(OBJECT_DIR)/index_vert_array.o:fogsrc/index_vert_array.cpp $(FOG_REL_HEADERS)
	$(CXX) $(CXXFLAGS) -c -o $@ fogsrc/index_vert_array.cpp
$(OBJECT_DIR)/cpu_thread.o:fogsrc/cpu_thread.cpp $(FOG_REL_HEADERS)
	$(CXX) $(CXXFLAGS) -c -o $@ fogsrc/cpu_thread.cpp
$(OBJECT_DIR)/fog_adapter.o:fogsrc/fog_adapter.cpp $(FOG_REL_HEADERS)
	$(CXX) $(CXXFLAGS) -c -o $@ fogsrc/fog_adapter.cpp 
$(OBJECT_DIR)/fog_task.o:fogsrc/fog_task.cpp $(FOG_REL_HEADERS)
	$(CXX) $(CXXFLAGS) -c -o $@ fogsrc/fog_task.cpp 
$(OBJECT_DIR)/filter.o:fogsrc/filter.cpp $(FOG_REL_HEADERS)
	$(CXX) $(CXXFLAGS) -c -o $@ fogsrc/filter.cpp 



#added by Huiming LV
#time:2015/3/20
ENGINE_SRC = fog_engine.o bitmap.o disk_thread.o index_vert_array.o cpu_thread.o fog_adapter.o fog_task.o filter.o
ENGINE_OBJS= $(addprefix $(OBJECT_DIR)/, $(ENGINE_SRC))

$(APPS_OBJ):%.o:application/%.cpp $(HEADERS_PATH)/fog_program.h 
	$(CXX) $(CXXFLAGS) -std=c++11 -c -o $(OBJECT_DIR)/$@ $< 

$(APPS_TARGET):%:%.o $(ENGINE_OBJS)
	$(CXX) -o $(BINARY_DIR)/$@ $(OBJECT_DIR)/$< $(ENGINE_OBJS) $(SYSLIBS)


.PHONY:

convert: $(CONVERT_TARGET)

test: $(TEST_TARGET)


SCC: scc


# utilities
cscope:
	find ./ -name "*.cpp" > cscope.files
	find ./ -name "*.c" >> cscope.files
	find ./ -name "*.h" >> cscope.files
	find ./ -name "*.hpp" >> cscope.files
	cscope -bqk

clean: 
	rm -f $(CONVERT_TARGET) $(CONVERT_OBJS)
	rm -f $(TEST_TARGET) $(TEST_OBJS)
	rm -f $(FOG_TARGET) $(FOG_OBJS)
	rm -f $(BINARY_DIR)/*
	rm -f $(OBJECT_DIR)/*
	rm -f cscope.*
	rm -rf *sched_strip*

