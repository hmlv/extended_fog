/**************************************************************************************************
 * Authors:
 *   Huiming Lv
 *
 * Routines:
 *   Implements Graph Coloring algorithm
 *   Reference: A Comparison of Parallel Graph Coloring Algorithms (Section 2.6)
 *************************************************************************************************/


#include "fog_adapter.h"
#include "fog_program.h"
#include "fog_task.h"
#include "../fogsrc/fog_task.cpp"
#include <atomic>

//this structure will define the "attribute" of one vertex, the only member will be the label
// value of the vertex
struct graph_coloring_vert_attr{
    unsigned int degree;
    unsigned int color;
};
typedef graph_coloring_vert_attr VERT_ATTR;

//template <typename VERT_ATTR, typename ALG_UPDATE, typename T>
template <typename T>
class graph_coloring_program : public Fog_program<VERT_ATTR, char, T>
{
        int step_switch;//"0" means to caculate the degree of vertex, and "1" means select independent set and color the independent set
	public:

        graph_coloring_program(int p_forward_backward_phase, bool p_init_sched, bool p_set_forward_backward):Fog_program<VERT_ATTR, char, T>(p_forward_backward_phase, p_init_sched, p_set_forward_backward)
        {
            this->need_all_neigh = true;
            this->step_switch    = 1;
        }

        //initialize each vertex of the graph
        void init( u32_t vid, VERT_ATTR * this_vert, index_vert_array<T> * vert_index )
        {
            this_vert->degree = vert_index->num_edges(vid, IN_EDGE) + vert_index->num_edges(vid, OUT_EDGE);
            if(0==this_vert->degree){
                this_vert->color = 1;
                fog_engine<VERT_ATTR, char, T>::vote_to_halt(vid, this->CONTEXT_PHASE);
            }
            else{
                this_vert->color = 0;
            }
        }

		// update one vertex. Explain the parameters:
		// vid: the vertex id of destination vertex;
		// this_vert: the attribute of this vertex;
		// vert_attr_array: vertex attribute array;
        // vert_index: object which manage the edges of the vertex
		//void update_vertex( u32_t vid, community_detection_vert_attr * this_vert, community_detection_vert_attr * vert_attr_array, index_vert_array<T> * vert_index )
		void update_vertex( u32_t vid, VERT_ATTR * this_vert, index_vert_array<T> * vert_index )
		{
            /*
            if(0 != this_vert->color){ //if implement vote_to_halt, remove this operation
                return;
            }
            */
            if(0==step_switch){  //modify the degree of vertex
                u32_t degree = 0;
                u32_t in_edges_num  = vert_index->num_edges(vid, IN_EDGE);
                u32_t out_edges_num = vert_index->num_edges(vid, OUT_EDGE);
                const VERT_ATTR * neigh_attr_ptr = NULL;
                for(u32_t i = 0; i < in_edges_num; ++i){
                    neigh_attr_ptr = vert_index->template get_in_neigh_attr<VERT_ATTR>(vid, i);
                    if(0 == neigh_attr_ptr->color){
                        ++degree;
                    }
                }
                for(u32_t i = 0; i < out_edges_num; ++i){
                    neigh_attr_ptr = vert_index->template get_out_neigh_attr<VERT_ATTR>(vid, i);
                    if(0 == neigh_attr_ptr->color){
                        ++degree;
                    }
                }
                this_vert->degree = degree;
            }
            else{  //find the independent set, and color vertices in the independent set
                u32_t in_edges_num  = vert_index->num_edges(vid, IN_EDGE);
                u32_t out_edges_num = vert_index->num_edges(vid, OUT_EDGE);
                const VERT_ATTR * neigh_attr_ptr = NULL;
                for(u32_t i = 0; i < in_edges_num; ++i){
                    neigh_attr_ptr = vert_index->template get_in_neigh_attr<VERT_ATTR>(vid, i);
                    if(0 != neigh_attr_ptr->color){
                        continue;
                    }
                    if(neigh_attr_ptr->degree > this_vert->degree){
                        return;
                    }
                    else if(neigh_attr_ptr->degree == this_vert->degree && vert_index->get_in_neighbour(vid, i) > vid){
                        return;
                    }
                }
                for(u32_t i = 0; i < out_edges_num; ++i){
                    neigh_attr_ptr = vert_index->template get_out_neigh_attr<VERT_ATTR>(vid, i);
                    if(0 != neigh_attr_ptr->color){
                        continue;
                    }
                    if(neigh_attr_ptr->degree > this_vert->degree){
                        return;
                    }
                    if(neigh_attr_ptr->degree == this_vert->degree && vert_index->get_out_neighbour(vid, i) > vid){
                        return;
                    }
                }
                //now, we have found the independent set, and then just select the color for the vertex
                std::vector<unsigned int> color_vec;
                unsigned int candidate_color = 1;
                for(u32_t i = 0; i < in_edges_num; ++i){
                    neigh_attr_ptr = vert_index->template get_in_neigh_attr<VERT_ATTR>(vid, i);
                    if(0 == neigh_attr_ptr->color){
                        continue;
                    }
                    color_vec.push_back(neigh_attr_ptr->color);
                }
                for(u32_t i = 0; i < out_edges_num; ++i){
                    neigh_attr_ptr = vert_index->template get_out_neigh_attr<VERT_ATTR>(vid, i);
                    if(0 == neigh_attr_ptr->color){
                        continue;
                    }
                    color_vec.push_back(neigh_attr_ptr->color);
                }
                std::sort(color_vec.begin(), color_vec.end());
                for(std::vector<unsigned int>::iterator iter = color_vec.begin(); iter!=color_vec.end(); ++iter){
                    if(*iter > candidate_color){
                        break;
                    }else{
                        ++candidate_color;
                    }
                }
                this_vert->color = candidate_color;
                fog_engine<VERT_ATTR, char, T>::vote_to_halt(vid, this->CONTEXT_PHASE);
            }
        }

        void before_iteration()
        {
            this->step_switch = 1 - this->step_switch;
            PRINT_DEBUG("GraphColoring engine is running for the %d-th iteration, step:%d, there are %d tasks to schedule!\n",
                    this->loop_counter, this->step_switch, this->num_tasks_to_sched);
        }
        int after_iteration()
        {
            PRINT_DEBUG("GraphColoring engine has finished the %d-th iteration!, there are %d tasks to schedule!\n",
                    this->loop_counter, this->num_tasks_to_sched);
            if (0 == this->num_tasks_to_sched)
                return ITERATION_STOP;
            return ITERATION_CONTINUE;
        }
        int finalize(VERT_ATTR * va)
        {
            unsigned int color = 0;
            for (unsigned int id = 0; id <= gen_config.max_vert_id; ++id){
                if(0==va[id].color){
                    PRINT_DEBUG("the answer is not correct, vertex %u has no color\n", id);
                    PRINT_ERROR("wrong answer! exit the computation\n");
                }
                if(va[id].color > color){
                    color = va[id].color;
                }
            }
            PRINT_DEBUG("this graph needs %u colors to color the graph\n", color);

            PRINT_DEBUG("GraphColoring engine stops!\n");
            return ENGINE_STOP;
        }
};


template <typename T>
void start_engine()
{
    Fog_program<VERT_ATTR, char, T> *gc_ptr = new graph_coloring_program<T>(FORWARD_TRAVERSAL, true, false);

    fog_engine<VERT_ATTR, char, T> * eng;
    eng = new fog_engine<VERT_ATTR, char, T>(VOTE_TO_HALT_ENGINE);
    //eng = new fog_engine<VERT_ATTR, char, T>(GLOBAL_ENGINE);

    struct task_config * ptr_task_config = new struct task_config;
    ptr_task_config->min_vert_id = gen_config.min_vert_id;
    ptr_task_config->max_vert_id = gen_config.max_vert_id;
    ptr_task_config->num_edges = gen_config.num_edges;
    ptr_task_config->is_remap = false;
    ptr_task_config->graph_path = gen_config.graph_path;
    ptr_task_config->vert_file_name = gen_config.vert_file_name;
    ptr_task_config->edge_file_name = gen_config.edge_file_name;
    ptr_task_config->attr_file_name = gen_config.attr_file_name;
    ptr_task_config->in_vert_file_name = gen_config.in_vert_file_name;
    ptr_task_config->in_edge_file_name = gen_config.in_edge_file_name;
    ptr_task_config->with_in_edge = gen_config.with_in_edge;

    Fog_task<VERT_ATTR, char, T> *task
        = new Fog_task<VERT_ATTR, char, T>();
    task->set_task_config(ptr_task_config);
    task->set_task_id(0);
    task->set_alg_ptr(gc_ptr);
    eng->run_task(task);

    delete gc_ptr;
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
