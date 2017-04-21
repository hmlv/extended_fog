/**************************************************************************************************
 * Authors:
 *   Huiming Lv
 *
 * Routines:
 *   Implements PageRank
 *************************************************************************************************/


#include "fog_adapter.h"
#include "fog_program.h"
#include "fog_task.h"
#include "../fogsrc/fog_task.cpp"
#include <atomic>

#define DAMPINGFACTOR 0.85

//this structure will define the "attribute" of one vertex, the only member will be the rank
// value of the vertex
struct pagerank_vert_attr{
    //std::atomic<float> rank;
    //u32_t out_degree;
    float rank;
    std::atomic<float> for_neigh_rank;
};
typedef pagerank_vert_attr VERT_ATTR;
typedef char ALG_UPDATE;
//template <typename VERT_ATTR, typename ALG_UPDATE, typename T>
template <typename T>
class pagerank_program : public Fog_program<pagerank_vert_attr, char, T>
{
	public:
        u32_t iteration_time;

        pagerank_program(int p_forward_backward_phase, bool p_init_sched, bool p_set_forward_backward, u32_t iterations):Fog_program<pagerank_vert_attr, char, T>(p_forward_backward_phase, p_init_sched, p_set_forward_backward)
        {
            this->need_all_neigh = true;
            this->iteration_time = iterations;
        }

        //initialize each vertex of the graph
        void init( u32_t vid, pagerank_vert_attr* this_vert, index_vert_array<T> * vert_index )
        {
            this_vert->rank = 1.0;
            this_vert->for_neigh_rank.store(1.0, std::memory_order_relaxed);
            //this_vert->out_degree = vert_index->num_edges(vid, OUT_EDGE);
            if(vert_index->num_edges(vid, IN_EDGE) == 0){
                fog_engine<VERT_ATTR, ALG_UPDATE, T>::vote_to_halt(vid, this->CONTEXT_PHASE);
            }
        }

		// update one vertex. Explain the parameters:
		// vid: the vertex id of destination vertex;
		// this_vert: the attribute of this vertex;
		// vert_attr_array: vertex attribute array;
        // vert_index: object which manage the edges of the vertex
		//void update_vertex( u32_t vid, pagerank_vert_attr * this_vert, pagerank_vert_attr * vert_attr_array, index_vert_array<T> * vert_index )
		void update_vertex( u32_t vid, pagerank_vert_attr * this_vert, index_vert_array<T> * vert_index )
		{
            float sum           = 0;
            const pagerank_vert_attr* neigh_attr = NULL;
            u32_t in_edges_num  = vert_index->num_edges(vid, IN_EDGE);
            //scan the in-neighbours
            for(u32_t i = 0; i < in_edges_num; ++i){
                neigh_attr = vert_index->template get_in_neigh_attr<pagerank_vert_attr>(vid, i);
                sum = sum + neigh_attr->for_neigh_rank.load(std::memory_order_relaxed);
            }
            this_vert->rank = sum*DAMPINGFACTOR + 1 - DAMPINGFACTOR;
            this_vert->for_neigh_rank.store(this_vert->rank / vert_index->num_edges(vid, OUT_EDGE), std::memory_order_relaxed);
        }

        void before_iteration()
        {
            PRINT_DEBUG("PageRank engine is running for the %d-th iteration, there are %d tasks to schedule!\n",
                    this->loop_counter, this->num_tasks_to_sched);
        }
        int after_iteration()
        {
            PRINT_DEBUG("PageRank engine has finished the %d-th iteration!, there are %d tasks to schedule!\n",
                    this->loop_counter, this->num_tasks_to_sched);
            if(this->loop_counter == this->iteration_time){
                return ITERATION_STOP;
            }
            if (0 == this->num_tasks_to_sched)
                return ITERATION_STOP;
            return ITERATION_CONTINUE;
        }
        int finalize(pagerank_vert_attr * va)
        {
            for (unsigned int id = 0; id < 100; id++)
            {
                PRINT_DEBUG_LOG("PageRank:result[%d], rank = %f\n", id, (va+id)->rank);
            }

            PRINT_DEBUG("PageRank engine stops!\n");
            return ENGINE_STOP;
        }
};


template <typename T>
void start_engine()
{
    u32_t niters = vm["pagerank::niters"].as<unsigned long>();
    Fog_program<VERT_ATTR, ALG_UPDATE, T> *pr_ptr = new pagerank_program<T>(FORWARD_TRAVERSAL, true, false, niters);

    fog_engine<VERT_ATTR, ALG_UPDATE, T> * eng;
    eng = new fog_engine<VERT_ATTR, ALG_UPDATE, T>(VOTE_TO_HALT_ENGINE);

    std::string desc_name = vm["graph"].as<std::string>();
    struct task_config * ptr_task_config = new struct task_config(false, 0, desc_name);
    Fog_task<VERT_ATTR, ALG_UPDATE, T> *task
        = new Fog_task<VERT_ATTR, ALG_UPDATE, T>();
    task->set_task_config(ptr_task_config);
    task->set_task_id(0);
    task->set_alg_ptr(pr_ptr);
    eng->run_task(task);

    delete pr_ptr;
    delete ptr_task_config;
    delete task;
    delete eng;
}

int main(int argc, const char**argv)
{
    Fog_adapter *adapter = new Fog_adapter();
    unsigned int type1_or_type2 = adapter->init(argc, argv);

    if(1 == type1_or_type2)
    {
        start_engine<type1_edge>();
    }
    else
    {
        start_engine<type2_edge>();
    }

    return 0;
}
