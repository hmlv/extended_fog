/**************************************************************************************************
 * Authors:
 *   Huiming Lv
 *
 * Routines:
 *   Implements Graph Coloring algorithm on HYBRID_ENGINE (combined use scatter-gather and update_vertex)
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
struct graph_coloring_update{

};

typedef graph_coloring_vert_attr VERT_ATTR;
typedef graph_coloring_update UPDATE_DATA;

//template <typename VERT_ATTR, typename ALG_UPDATE, typename T>
template <typename T>
class graph_coloring_program : public Fog_program<VERT_ATTR, UPDATE_DATA, T>
{
        int SG_CONTEXT_PHASE; //change the degree of vertex
        int UV_CONTEXT_PHASE; //select independent set and color the vertices in the independent set
	public:

        graph_coloring_program(int p_forward_backward_phase, bool p_init_sched, bool p_set_forward_backward, int scatter_gather_phase, int update_vertex_phase, int first_operation):Fog_program<VERT_ATTR, UPDATE_DATA, T>(p_forward_backward_phase, p_init_sched, p_set_forward_backward)
        {
            this->need_all_neigh = true;
            this->SG_CONTEXT_PHASE = scatter_gather_phase;
            this->UV_CONTEXT_PHASE = update_vertex_phase;
            this->operation = first_operation;
        }

        //initialize each vertex of the graph
        void init( u32_t vid, VERT_ATTR * this_vert, index_vert_array<T> * vert_index )
        {
            this_vert->degree = vert_index->num_edges(vid, IN_EDGE) + vert_index->num_edges(vid, OUT_EDGE);
            if(0==this_vert->degree){
                this_vert->color = 1;
                fog_engine<VERT_ATTR, UPDATE_DATA, T>::vote_to_halt(vid, this->UV_CONTEXT_PHASE);
            }
            else{
                this_vert->color = 0;
            }
        }

        //scatter updates at vid-th vertex
        void scatter_one_edge(VERT_ATTR * this_vert, T &this_edge, u32_t backward_update_dest, update<UPDATE_DATA> &this_update)
        {
            if(this->forward_backward_phase == FORWARD_TRAVERSAL){
                this_update.dest_vert = this_edge.get_dest_value();
            }
            else{
                this_update.dest_vert = backward_update_dest;
            }
        }

        //gather one update "u" from outside
        void gather_one_update(u32_t vid, VERT_ATTR* this_vert, update<UPDATE_DATA>* this_update)
        {
            assert(this_vert->degree > 0);
            --this_vert->degree;
        }

		// update one vertex. Explain the parameters:
		// vid: the vertex id of destination vertex;
		// this_vert: the attribute of this vertex;
		// vert_attr_array: vertex attribute array;
        // vert_index: object which manage the edges of the vertex
		//void update_vertex( u32_t vid, community_detection_vert_attr * this_vert, community_detection_vert_attr * vert_attr_array, index_vert_array<T> * vert_index )
		void update_vertex( u32_t vid, VERT_ATTR * this_vert, index_vert_array<T> * vert_index )
        {
            //find the independent set, and color vertices in the independent set
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
            fog_engine<VERT_ATTR, UPDATE_DATA, T>::vote_to_halt(vid, this->UV_CONTEXT_PHASE);
            fog_engine<VERT_ATTR, UPDATE_DATA, T>::add_schedule_no_optimize(vid, this->SG_CONTEXT_PHASE);
        }

        void before_iteration()
        {
            if(this->operation == SCATTER_GATHER){
                if(this->forward_backward_phase == FORWARD_TRAVERSAL){
                    PRINT_DEBUG("GraphColoring engine is running for the %d-th iteration, operation:SCATTER_GATHER: FORWARD, there are %d tasks to schedule!\n",
                            this->loop_counter, this->num_tasks_to_sched);
                }
                else{
                    PRINT_DEBUG("GraphColoring engine is running for the %d-th iteration, operation:SCATTER_GATHER: BACKWARD, there are %d tasks to schedule!\n",
                            this->loop_counter, this->num_tasks_to_sched);
                }
            }
            else{
                PRINT_DEBUG("GraphColoring engine is running for the %d-th iteration, operation:UPDATE_VERTEX, there are %d tasks to schedule!\n",
                        this->loop_counter, this->num_tasks_to_sched);
            }
        }
        int after_iteration()
        {
            PRINT_DEBUG("GraphColoring engine has finished the %d-th iteration!, there are %d non-converged vertices!\n",
                    this->loop_counter, this->num_tasks_to_sched);
            if (0 == this->num_tasks_to_sched){
                return ITERATION_STOP;
            }
            if(this->operation == SCATTER_GATHER){
                if(this->forward_backward_phase == FORWARD_TRAVERSAL){
                    this->forward_backward_phase = BACKWARD_TRAVERSAL;
                }
                else{
                    this->operation = UPDATE_VERTEX;
                }
            }
            else{
                this->operation = SCATTER_GATHER;
                this->forward_backward_phase = FORWARD_TRAVERSAL;
            }
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
    Fog_program<VERT_ATTR, UPDATE_DATA, T> *gc_ptr = new graph_coloring_program<T>(FORWARD_TRAVERSAL, true, true, 0, 1, UPDATE_VERTEX);

    fog_engine<VERT_ATTR, UPDATE_DATA, T> * eng;
    eng = new fog_engine<VERT_ATTR, UPDATE_DATA, T>(HYBRID_ENGINE, 0, 1);

    std::string desc_name = vm["graph"].as<std::string>();
    struct task_config * ptr_task_config = new struct task_config(false, 0, desc_name);

    Fog_task<VERT_ATTR, UPDATE_DATA, T> *task
        = new Fog_task<VERT_ATTR, UPDATE_DATA, T>();
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
