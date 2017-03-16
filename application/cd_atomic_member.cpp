/**************************************************************************************************
 * Authors:
 *   Huiming Lv
 *
 * Routines:
 *   Implements Community Detection algorithm
 *************************************************************************************************/


#include "fog_adapter.h"
#include "fog_program.h"
#include "fog_task.h"
#include "../fogsrc/fog_task.cpp"
#include <atomic>

//this structure will define the "attribute" of one vertex, the only member will be the label
// value of the vertex
struct community_detection_vert_attr{
	//u32_t label;
    std::atomic<uint32_t> label;
};

//template <typename VERT_ATTR, typename ALG_UPDATE, typename T>
template <typename T>
class community_detection_program : public Fog_program<community_detection_vert_attr, char, T>
{
	public:

        community_detection_program(int p_forward_backward_phase, bool p_init_sched, bool p_set_forward_backward):Fog_program<community_detection_vert_attr, char, T>(p_forward_backward_phase, p_init_sched, p_set_forward_backward)
        {
            this->need_all_neigh = true;
        }

        //initialize each vertex of the graph
        void init( u32_t vid, community_detection_vert_attr* this_vert, index_vert_array<T> * vert_index )
        {
            this_vert->label = vid;
            fog_engine<community_detection_vert_attr, char, T>::add_schedule_no_optimize(vid, this->CONTEXT_PHASE);
        }

		// update one vertex. Explain the parameters:
		// vid: the vertex id of destination vertex;
		// this_vert: the attribute of this vertex;
		// vert_attr_array: vertex attribute array;
        // vert_index: object which manage the edges of the vertex
		//void update_vertex( u32_t vid, community_detection_vert_attr * this_vert, community_detection_vert_attr * vert_attr_array, index_vert_array<T> * vert_index )
		void update_vertex( u32_t vid, community_detection_vert_attr * this_vert, index_vert_array<T> * vert_index )
		{
            u32_t in_edges_num  = 0L;
            u32_t out_edges_num = 0L;
            //u32_t neigh_id  = 0;
            std::map<u32_t, u32_t> counts_map;
            std::map<u32_t, u32_t>::iterator counts_map_iter = counts_map.end();
            u32_t max_count = 0;
            u32_t max_label = 0;
            u32_t curr_count = 0;
            u32_t curr_label = 0;
            in_edges_num = vert_index->num_edges(vid, IN_EDGE);
            out_edges_num = vert_index->num_edges(vid, OUT_EDGE);
            const community_detection_vert_attr* neigh_attr = NULL;
            if(in_edges_num + out_edges_num == 0){
                return;   //trivial vertex
            }
            //scan the in-neighbours
            for(u32_t i = 0; i < in_edges_num; ++i){
                /*
                neigh_id = vert_index->get_in_neighbour(vid, i);
                curr_label = vert_attr_array[neigh_id].label;
                */
                neigh_attr = vert_index->template get_in_neigh_attr<community_detection_vert_attr>(vid, i);
                //curr_label = neigh_attr->label;
                curr_label = neigh_attr->label.load(std::memory_order_relaxed);
                counts_map_iter = counts_map.find(curr_label);
                if(counts_map.end() == counts_map_iter){
                    counts_map.insert(std::pair<u32_t, u32_t>(curr_label, 1));
                    curr_count = 1;
                }
                else{
                    counts_map_iter->second++;
                    curr_count = counts_map_iter->second;
                }
                if(curr_count > max_count || (curr_count==max_count && curr_label > max_label)){
                    max_count = curr_count;
                    max_label = curr_label;
                }
            }
            //scan the out-neighbours;
            for(u32_t i = 0; i < out_edges_num; ++i){
                /*
                neigh_id = vert_index->get_out_neighbour(vid, i);
                curr_label = vert_attr_array[neigh_id].label;
                */
                neigh_attr = vert_index->template get_out_neigh_attr<community_detection_vert_attr>(vid, i);
                //curr_label = neigh_attr->label;
                curr_label = neigh_attr->label.load(std::memory_order_relaxed);
                counts_map_iter = counts_map.find(curr_label);
                if(counts_map.end() == counts_map_iter){
                    counts_map.insert(std::pair<u32_t, u32_t>(curr_label, 1));
                    curr_count = 1;
                }
                else{
                    counts_map_iter->second++;
                    curr_count = counts_map_iter->second;
                }
                if(curr_count > max_count || (curr_count==max_count && curr_label > max_label)){
                    max_count = curr_count;
                    max_label = curr_label;
                }
            }
            if(max_label != this_vert->label){
                //if(max_count > 1){

                    //this_vert->label = max_label;
                    this_vert->label.store(max_label, std::memory_order_relaxed);
                    fog_engine<community_detection_vert_attr, char, T>::add_schedule_no_optimize(vid, this->CONTEXT_PHASE);
                    /*
                }
                else if(1==max_count && max_label > this_vert->label){

                    if(this->loop_counter == 200 || this->loop_counter == 199){
                        PRINT_DEBUG_TEST_LOG("max_count = 1, vid = %u, old_label = %u, new_label = %u\n", vid, this_vert->label, max_label);
                    }

                    this_vert->label = max_label;
                    fog_engine<community_detection_vert_attr, community_detection_vert_attr, T>::add_schedule_no_optimize(vid, this->CONTEXT_PHASE);
                }
                */
            }
        }

        void before_iteration()
        {
            PRINT_DEBUG("CommunityDetection engine is running for the %d-th iteration, there are %d tasks to schedule!\n",
                    this->loop_counter, this->num_tasks_to_sched);
        }
        int after_iteration()
        {
            if(this->loop_counter == 200){
                return ITERATION_STOP;
            }
            PRINT_DEBUG("CommunityDetection engine has finished the %d-th iteration!, there are %d tasks to schedule!\n",
                    this->loop_counter, this->num_tasks_to_sched);
            if (0 == this->num_tasks_to_sched)
                return ITERATION_STOP;
            return ITERATION_CONTINUE;
        }
        int finalize(community_detection_vert_attr * va)
        {
            /*
            for (unsigned int id = 0; id < 100; id++)
                PRINT_DEBUG_LOG("CommunityDetection:result[%d], label = %u\n", id, (va+id)->label);
                */
            std::map<u32_t, u32_t> counts_map;
            std::map<u32_t, u32_t>::iterator iter = counts_map.end();
            for (unsigned int id = 0; id <= gen_config.max_vert_id; ++id){
                iter = counts_map.find(va[id].label);
                if(counts_map.end()==iter){
                    counts_map.insert(std::pair<u32_t, u32_t>(va[id].label, 1));
                }
                else{
                    iter->second++;
                }
            }
            PRINT_DEBUG("this graph has %lu communities\n", counts_map.size());

            PRINT_DEBUG("CommunityDetection engine stops!\n");
            return ENGINE_STOP;
        }
};


template <typename T>
void start_engine()
{
    //Fog_program<community_detection_vert_attr, community_detection_vert_attr, T> *cd_ptr = new community_detection_program<T>(FORWARD_TRAVERSAL, true, false);
    Fog_program<community_detection_vert_attr, char, T> *cd_ptr = new community_detection_program<T>(FORWARD_TRAVERSAL, true, false);

    //fog_engine<community_detection_vert_attr, community_detection_vert_attr, T> * eng;
    fog_engine<community_detection_vert_attr, char, T> * eng;
    //eng = new fog_engine<community_detection_vert_attr, community_detection_vert_attr, T>(TARGET_ENGINE);
    eng = new fog_engine<community_detection_vert_attr, char, T>(TARGET_ENGINE);

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
    //Fog_task<community_detection_vert_attr, community_detection_vert_attr, T> *task
       // = new Fog_task<community_detection_vert_attr, community_detection_vert_attr, T>();
    Fog_task<community_detection_vert_attr, char, T> *task
        = new Fog_task<community_detection_vert_attr, char, T>();
    task->set_task_config(ptr_task_config);
    task->set_task_id(0);
    task->set_alg_ptr(cd_ptr);
    eng->run_task(task);

    delete cd_ptr;
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
