/**************************************************************************************************
 * Authors: 
 *   Zhiyuan Shao, Jian He, Huiming Lv
 *
 * Declaration:
 *   Prototype CPU threads.
 *************************************************************************************************/

#ifndef __CPU_THREAD_HPP__
#define __CPU_THREAD_HPP__

#include "config.hpp"
#include "print_debug.hpp"
#include "bitmap.hpp"
#include "disk_thread.hpp"
#include <cassert>
#include <sys/stat.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sstream> 
#include <fcntl.h>
#include "fog_program.h"

extern std::vector<struct bag_config>task_bag_config_vec;
extern struct mmap_config mmap_file(std::string file_name);
extern void unmap_file(const struct mmap_config & m_config);

template<typename D>
extern u32_t fog_binary_search(D * array, int left, int right, D key);

enum fog_engine_state{
    INIT = 0, 
    GLOBAL_SCATTER, TARGET_SCATTER, 
    GLOBAL_GATHER, TARGET_GATHER,
    CREATE_SUBTASK_DATASET
};

//denotes the different status of cpu threads after they finished the given tasks.
// Note: these status are for scatter phase ONLY!
enum cpu_thread_status{
	UPDATE_BUF_FULL = 100,	//Cannot scatter more updates, since my update buffer is full
	NO_MORE_SCHED,			//I have no more sched tasks, but have updates in the auxiliary update buffer
	FINISHED_SCATTER		//I have no more sched tasks, and no updates in auxiliary update buffer.
	                        //	But still have updates in my strip update buffer. 
};

enum scatter_signal
{
    NORMAL_SCATTER = 0,
    CONTEXT_SCATTER,
    STEAL_SCATTER,
    SPECIAL_STEAL_SCATTER
};

enum gather_signal
{
    NORMAL_GATHER = 0,
    CONTEXT_GATHER,
    STEAL_GATHER
};

//global variables
//U:updates
//VA:attr
//A:
template <typename VA, typename U, typename T>
class cpu_thread;



//parameter to use to perform different actions
struct init_param{
	char* attr_buf_head;
	u32_t start_vert_id;
	u32_t num_of_vertices;
};

struct scatter_param{
	void* attr_array_head;
    u32_t PHASE;
};

struct gather_param{
    void * attr_array_head;
    int strip_id;
    u32_t threshold;
};

struct create_dataset_param{
    bool is_ordered;
};

//class barrier - for multi-thread synchronization
class barrier {
    volatile unsigned long count[2];
    volatile unsigned long sense;
    unsigned long expected;
    public:
    barrier(unsigned long expected_in)
        :sense(0), expected(expected_in)
    {
        count[0] = 0;
        count[1] = 0;
    }

    void wait()
    {
        unsigned long sense_used = sense;
        unsigned long arrived =
            __sync_fetch_and_add(&count[sense_used], 1);
        if(arrived == (expected - 1)) {
            sense = 1 - sense_used; // Reverse sense
            count[sense_used] = 0;
        }
        while(count[sense_used] != 0);
        __sync_synchronize(); // Also clobber memory
    }
//    friend class cpu_thread<A,VA>;
};


template <typename VA, typename U, typename T>
struct cpu_work{
	u32_t engine_state;
	void* state_param;

	cpu_work( u32_t state, void* state_param_in);
	void operator() ( u32_t processor_id, barrier *sync, index_vert_array<T> *vert_index, segment_config<VA>* seg_config, int *status, T t_edge, in_edge t_in_edge, update<U> t_update ,Fog_program<VA,U,T> * alg_ptr);
    void show_update_map( int processor_id, segment_config<VA>* seg_config, u32_t* map_head );
};

template <typename VA, typename U, typename T>
class cpu_thread {
public:
    const unsigned long processor_id; 
	index_vert_array<T>* vert_index;
	segment_config<VA>* seg_config;
    T t_edge;
    in_edge t_in_edge;
    update<U> t_update; 
    Fog_program<VA,U,T> * m_alg_ptr;
	int status;

	//following members will be shared among all cpu threads
    static barrier *sync;
    static volatile bool terminate;
    static struct cpu_work<VA,U, T> * volatile work_to_do;

    cpu_thread(u32_t processor_id_in, index_vert_array<T> * vert_index_in, segment_config<VA>* seg_config_in, Fog_program<VA,U,T> * alg_ptr );
    void operator() ();
	sched_task* get_sched_task();
	void browse_sched_list();
};

template <typename VA, typename U, typename T>
barrier * cpu_thread<VA, U, T>::sync;

template <typename VA, typename U, typename T>
volatile bool cpu_thread<VA, U, T>::terminate;

template <typename VA, typename U, typename T>
cpu_work<VA,U, T> * volatile cpu_thread<VA, U, T>::work_to_do;

#endif
