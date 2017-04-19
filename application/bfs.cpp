/**************************************************************************************************
 * Authors:
 *   Huiming Lv
 *
 * Routines:
 *   Implements bfs
 *************************************************************************************************/


#include "fog_adapter.h"
#include "fog_program.h"
#include "fog_task.h"
#include "../fogsrc/fog_task.cpp"
#include <atomic>


//this structure will define the "attribute" of one vertex, the only member will be the level
// value of the vertex
struct bfs_vert_attr{
    int level;
};
typedef bfs_vert_attr VERT_ATTR;
typedef char ALG_UPDATE;
//template <typename VERT_ATTR, typename ALG_UPDATE, typename T>
template <typename T>
class bfs_program : public Fog_program<VERT_ATTR, ALG_UPDATE, T>
{
	public:
        u32_t bfs_root = 0;
        u32_t curr_level = -1;
        bfs_program(int p_forward_backward_phase, bool p_init_sched, bool p_set_forward_backward, u32_t root):Fog_program<bfs_vert_attr, char, T>(p_forward_backward_phase, p_init_sched, p_set_forward_backward)
        {
            this->need_all_neigh = true;
            this->bfs_root       = root;
        }

        //initialize each vertex of the graph
        void init( u32_t vid, bfs_vert_attr* this_vert, index_vert_array<T> * vert_index )
        {
            if(vid==bfs_root){
                this_vert->level = 0;
                fog_engine<VERT_ATTR, ALG_UPDATE, T>::add_schedule_no_optimize(vid, this->CONTEXT_PHASE);
            }
            else{
                this_vert->level = -1;
            }
        }

		// update one vertex. Explain the parameters:
		// vid: the vertex id of destination vertex;
		// this_vert: the attribute of this vertex;
		// vert_attr_array: vertex attribute array;
        // vert_index: object which manage the edges of the vertex
		//void update_vertex( u32_t vid, bfs_vert_attr * this_vert, bfs_vert_attr * vert_attr_array, index_vert_array<T> * vert_index )
		void update_vertex( u32_t vid, bfs_vert_attr * this_vert, index_vert_array<T> * vert_index )
		{
            if(-1!=this_vert->level && vid!=this->bfs_root){
                return;
            }
            u32_t out_edges_num = 0;
            this_vert->level = this->curr_level;
            //avtive out-neighbours
            out_edges_num = vert_index->num_edges(vid, OUT_EDGE);
            u32_t neigh_id = 0;
            for(u32_t i = 0; i < out_edges_num; ++i){
                neigh_id = vert_index->get_out_neighbour(vid, i);
                fog_engine<VERT_ATTR, ALG_UPDATE, T>::add_schedule_no_optimize(neigh_id, this->CONTEXT_PHASE);
            }
        }

        void before_iteration()
        {
            this->curr_level += 1;
            PRINT_DEBUG("bfs engine is running for the %d-th iteration, there are %d tasks to schedule!\n",
                    this->loop_counter, this->num_tasks_to_sched);
        }
        int after_iteration()
        {
            PRINT_DEBUG("bfs engine has finished the %d-th iteration!, there are %d tasks to schedule!\n",
                    this->loop_counter, this->num_tasks_to_sched);
            if (0 == this->num_tasks_to_sched)
                return ITERATION_STOP;
            return ITERATION_CONTINUE;
        }
        int finalize(bfs_vert_attr * va)
        {
            for (unsigned int id = 0; id < 100; id++)
            {
                PRINT_DEBUG_LOG("bfs:result[%d], level = %d\n", id, (va+id)->level);
            }

            PRINT_DEBUG("bfs engine stops!\n");
            return ENGINE_STOP;
        }
};


template <typename T>
void start_engine()
{
    u32_t bfs_root = vm["bfs::bfs-root"].as<unsigned long>();
    Fog_program<VERT_ATTR, ALG_UPDATE, T> *bfs_ptr = new bfs_program<T>(FORWARD_TRAVERSAL, true, false, bfs_root);

    fog_engine<VERT_ATTR, ALG_UPDATE, T> * eng;
    eng = new fog_engine<VERT_ATTR, ALG_UPDATE, T>(TARGET_ENGINE);

    std::string desc_name = vm["graph"].as<std::string>();
    struct task_config * ptr_task_config = new struct task_config(false, 0, desc_name);
    Fog_task<VERT_ATTR, ALG_UPDATE, T> *task
        = new Fog_task<VERT_ATTR, ALG_UPDATE, T>();
    task->set_task_config(ptr_task_config);
    task->set_task_id(0);
    task->set_alg_ptr(bfs_ptr);
    eng->run_task(task);

    delete bfs_ptr;
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
