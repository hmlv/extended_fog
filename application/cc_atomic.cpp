/**************************************************************************************************
 * Authors:
 *   Huiming Lv
 *
 * Routines:
 *   Implements cc
 *************************************************************************************************/


#include "fog_adapter.h"
#include "fog_program.h"
#include "fog_task.h"
#include "../fogsrc/fog_task.cpp"
#include <atomic>


//this structure will define the "attribute" of one vertex, the only member will be the label
// value of the vertex
struct cc_vert_attr{
    std::atomic<uint32_t> label;
};
typedef cc_vert_attr VERT_ATTR;
typedef char ALG_UPDATE;
//template <typename VERT_ATTR, typename ALG_UPDATE, typename T>
template <typename T>
class cc_program : public Fog_program<VERT_ATTR, ALG_UPDATE, T>
{
	public:
        cc_program(int p_forward_backward_phase, bool p_init_sched, bool p_set_forward_backward):Fog_program<cc_vert_attr, char, T>(p_forward_backward_phase, p_init_sched, p_set_forward_backward)
        {
            this->need_all_neigh = true;
        }

        //initialize each vertex of the graph
        void init( u32_t vid, cc_vert_attr* this_vert, index_vert_array<T> * vert_index )
        {
            this_vert->label = vid;
            if(vert_index->num_edges(vid, IN_EDGE) + vert_index->num_edges(vid, OUT_EDGE) > 0){
                fog_engine<VERT_ATTR, ALG_UPDATE, T>::add_schedule_no_optimize(vid, this->CONTEXT_PHASE);
            }
        }

		// update one vertex. Explain the parameters:
		// vid: the vertex id of destination vertex;
		// this_vert: the attribute of this vertex;
		// vert_attr_array: vertex attribute array;
        // vert_index: object which manage the edges of the vertex
		//void update_vertex( u32_t vid, cc_vert_attr * this_vert, cc_vert_attr * vert_attr_array, index_vert_array<T> * vert_index )
		void update_vertex( u32_t vid, cc_vert_attr * this_vert, index_vert_array<T> * vert_index )
		{
            u32_t in_edges_num  = 0;
            u32_t out_edges_num = 0;
            u32_t min_label     = 0;
            u32_t temp          = 0;
            const cc_vert_attr* neigh_attr = NULL;
            in_edges_num = vert_index->num_edges(vid, IN_EDGE);
            out_edges_num = vert_index->num_edges(vid, OUT_EDGE);
            if(in_edges_num + out_edges_num== 0){
                fog_engine<VERT_ATTR, ALG_UPDATE, T>::vote_to_halt(vid, this->CONTEXT_PHASE);
                return;   //trivial vertex
            }
            min_label = this_vert->label.load(std::memory_order_relaxed);
            //scan the in-neighbours
            for(u32_t i = 0; i < in_edges_num; ++i){
                neigh_attr = vert_index->template get_in_neigh_attr<cc_vert_attr>(vid, i);
                temp = neigh_attr->label.load(std::memory_order_relaxed);
                if(temp < min_label){
                    min_label = temp;
                }
            }
            //scan the out-neighbours
            for(u32_t i = 0; i < out_edges_num; ++i){
                neigh_attr = vert_index->template get_out_neigh_attr<cc_vert_attr>(vid, i);
                temp = neigh_attr->label.load(std::memory_order_relaxed);
                if(temp < min_label){
                    min_label = temp;
                }
            }
            temp = this_vert->label.load(std::memory_order_relaxed);
            if(min_label==temp)
            {
                //converged
            }
            else{
                assert(min_label < temp);
                this_vert->label.store(min_label, std::memory_order_relaxed);
                u32_t neigh_id = 0;
                for(u32_t i = 0; i < in_edges_num; ++i){
                    neigh_id = vert_index->get_in_neighbour(vid, i);
                    fog_engine<VERT_ATTR, ALG_UPDATE, T>::add_schedule_no_optimize(neigh_id, this->CONTEXT_PHASE);
                }
                for(u32_t i = 0; i < out_edges_num; ++i){
                    neigh_id = vert_index->get_out_neighbour(vid, i);
                    fog_engine<VERT_ATTR, ALG_UPDATE, T>::add_schedule_no_optimize(neigh_id, this->CONTEXT_PHASE);
                }
            }
        }

        void before_iteration()
        {
            PRINT_DEBUG("cc engine is running for the %d-th iteration, there are %d tasks to schedule!\n",
                    this->loop_counter, this->num_tasks_to_sched);
        }
        int after_iteration()
        {
            PRINT_DEBUG("cc engine has finished the %d-th iteration!, there are %d tasks to schedule!\n",
                    this->loop_counter, this->num_tasks_to_sched);
            if (0 == this->num_tasks_to_sched)
                return ITERATION_STOP;
            return ITERATION_CONTINUE;
        }
        int finalize(cc_vert_attr * va)
        {
            for (unsigned int id = 0; id < 100; id++)
            {
                PRINT_DEBUG_LOG("cc:result[%d], label = %u\n", id, (va+id)->label.load(std::memory_order_relaxed));
            }

            PRINT_DEBUG("cc engine stops!\n");
            return ENGINE_STOP;
        }
};


template <typename T>
void start_engine()
{
    Fog_program<VERT_ATTR, ALG_UPDATE, T> *cc_ptr = new cc_program<T>(FORWARD_TRAVERSAL, true, false);

    fog_engine<VERT_ATTR, ALG_UPDATE, T> * eng;
    eng = new fog_engine<VERT_ATTR, ALG_UPDATE, T>(TARGET_ENGINE);

    std::string desc_name = vm["graph"].as<std::string>();
    struct task_config * ptr_task_config = new struct task_config(false, 0, desc_name);
    Fog_task<VERT_ATTR, ALG_UPDATE, T> *task
        = new Fog_task<VERT_ATTR, ALG_UPDATE, T>();
    task->set_task_config(ptr_task_config);
    task->set_task_id(0);
    task->set_alg_ptr(cc_ptr);
    eng->run_task(task);

    delete cc_ptr;
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
