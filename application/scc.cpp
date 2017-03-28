/**************************************************************************************************
 * Authors:
 *   Huiming Lv
 *
 * Routines:
 *   Implements strongly connected component algorithm
 *
 *************************************************************************************************/


#include "fog_adapter.h"
#include "fog_program.h"
#include "fog_task.h"
#include "../fogsrc/fog_task.cpp"
#include "filter.h"
#include "../fogsrc/filter.cpp"
#include "convert.h"
#include "config_parse.h"
#include <vector>
#include <stdio.h>
#include <memory.h>

time_t start_time;
time_t end_time;

std::string result_filename;
u32_t * components_label_ptr;

std::vector<struct bag_config> vec_bag_config;


/***************************************************************************************************
 *BFS_based
 ****************************************************************************************************/
//fw_bw_label = 0, start
//fw_bw_label = 1, FW
//fw_bw_label = 2, is_found = TRUE,  SCC
//fw_bw_label = 2, is_found = FALSE, BW
//fw_bw_label = 3, TRIM
//fw_bw_label = 8, remain
struct scc_vert_attr{
	//u32_t fw_bw_label;
    char fw_bw_label;
    bool is_found;
}__attribute__((__packed__));

struct scc_update
{
    //u32_t fw_bw_label;
    char fw_bw_label;
};

template <typename T>
class scc_fb_program : public Fog_program<scc_vert_attr, scc_update, T>
{
	public:
        int out_loop;
        u32_t m_pivot;

        scc_fb_program(int p_forward_backward_phase, bool p_init_sched, bool p_set_forward_backward, u32_t pivot):Fog_program<scc_vert_attr, scc_update, T>(p_forward_backward_phase, p_init_sched, p_set_forward_backward)
        {
            out_loop = 0;
            m_pivot = pivot;
        }

        void init( u32_t vid, scc_vert_attr* va, index_vert_array<T> * vert_index)
        {
            if (out_loop == 0 && this->forward_backward_phase == FORWARD_TRAVERSAL)
            {
                if (vert_index->num_edges(vid, OUT_EDGE) == 0 || vert_index->num_edges(vid, IN_EDGE) == 0)
                {
                    va->is_found = true;
                    va->fw_bw_label = 3;
                }
                else
                {
                    va->is_found = false;
                    if(vid == m_pivot)
                    {
                        va->fw_bw_label = 0;
                        fog_engine<scc_vert_attr, scc_update, T>::add_schedule(vid, this->CONTEXT_PHASE);
                    }
                    else
                    {
                        //va->fw_bw_label = UINT_MAX;
                        va->fw_bw_label = 8;
                    }
                }

            }
            else
            {
                assert(this->forward_backward_phase == BACKWARD_TRAVERSAL );
                {
                    //if(vid == gen_config.min_vert_id)
                    if(vid == m_pivot)
                    {
                        va->is_found = true;
                        va->fw_bw_label = 2;
                        fog_engine<scc_vert_attr, scc_update, T>::add_schedule(vid, this->CONTEXT_PHASE);
                    }
                    else if(va->fw_bw_label==0)
                    {
                        va->fw_bw_label = 1;
                    }
                }
            }
        }

        //scatter updates at vid-th vertex
		void scatter_one_edge(
                scc_vert_attr * this_vert,
                T &this_edge,
                u32_t backward_update_dest,
                update<scc_update> &this_update)
        {
            if (this->forward_backward_phase == FORWARD_TRAVERSAL)
            {
                this_update.dest_vert = this_edge.get_dest_value();
            }
            else
            {
                assert (this->forward_backward_phase == BACKWARD_TRAVERSAL);
                this_update.dest_vert = backward_update_dest;
            }
            this_update.vert_attr.fw_bw_label = this_vert->fw_bw_label;
            //return ret;
        }
        //gather one update "u" from outside
        void gather_one_update( u32_t vid, scc_vert_attr* this_vert,
                struct update<scc_update>* this_update)
        {
            /*
             * just gather everything
             */
            if (this->forward_backward_phase == FORWARD_TRAVERSAL)
            {
                assert(this_update->vert_attr.fw_bw_label==0);
                //if (this_vert->fw_bw_label==UINT_MAX)
                if (this_vert->fw_bw_label==8)
                {
                    this_vert->fw_bw_label = this_update->vert_attr.fw_bw_label;
                    fog_engine<scc_vert_attr, scc_update, T>::add_schedule(vid, this->CONTEXT_PHASE);
                }
            }
            else
            {
                assert (this->forward_backward_phase == BACKWARD_TRAVERSAL);
                assert(this_update->vert_attr.fw_bw_label==2);
                //if (this_vert->fw_bw_label==UINT_MAX)
                if (this_vert->fw_bw_label==8)
                {
                    this_vert->fw_bw_label = this_update->vert_attr.fw_bw_label;
                    fog_engine<scc_vert_attr, scc_update, T>::add_schedule(vid, this->CONTEXT_PHASE);
                }
                else if (this_vert->fw_bw_label==1 && this_vert->is_found == false)
                {
                    this_vert->fw_bw_label = this_update->vert_attr.fw_bw_label;
                    this_vert->is_found = true;
                    fog_engine<scc_vert_attr, scc_update, T>::add_schedule(vid, this->CONTEXT_PHASE);
                }
            }
        }
        void before_iteration()
        {
            if (this->forward_backward_phase == FORWARD_TRAVERSAL)
                PRINT_DEBUG("SCC engine is running FORWARD_TRAVERSAL for the %d-th iteration, there are %d tasks to schedule!\n",
                        this->loop_counter, this->num_tasks_to_sched);
            else
            {
                assert(this->forward_backward_phase == BACKWARD_TRAVERSAL);
                PRINT_DEBUG("SCC engine is running BACKWARD_TRAVERSAL for the %d-th iteration, there are %d tasks to schedule!\n",
                        this->loop_counter, this->num_tasks_to_sched);
            }
        }
        int after_iteration()
        {
            PRINT_DEBUG("SCC engine has finished the %d-th iteration, there are %d tasks to schedule at next iteration!\n",
                    this->loop_counter, this->num_tasks_to_sched);
            if (this->num_tasks_to_sched == 0)
                return ITERATION_STOP;
            else
                return ITERATION_CONTINUE;
        }
        int finalize(scc_vert_attr * va)
        {
            out_loop ++;
            if (out_loop==2)
            {
                //std::cout<<"finalize "<<va<<std::endl;
                return ITERATION_STOP;
            }
            if (this->forward_backward_phase == FORWARD_TRAVERSAL)
            {
                this->forward_backward_phase = BACKWARD_TRAVERSAL;
                this->loop_counter = 0;
                this->CONTEXT_PHASE = 0;
                return ENGINE_CONTINUE;
            }
            else
            {
                assert(this->forward_backward_phase == BACKWARD_TRAVERSAL);
                this->forward_backward_phase = FORWARD_TRAVERSAL;
                this->loop_counter = 0;
                this->CONTEXT_PHASE = 0;
                return ENGINE_CONTINUE;
            }
        }

        int num_of_remain_partitions(){
            return 3;
        }
        int judge_for_filter(scc_vert_attr * this_vert){
            if(1==this_vert->fw_bw_label){
                return 0;
            }
            else if(2==this_vert->fw_bw_label){
                if(false==this_vert->is_found){
                    return 1;
                }
            }
            else if(8==this_vert->fw_bw_label){
                return 2;
            }
            return -1;
        }
};

/********************************************************************************************
 *minimal label propagation
 ********************************************************************************************/

struct scc_color_vert_attr
{
    u32_t prev_root;
    u32_t component_root;
    bool is_found;
}__attribute__((__packed__));

struct scc_color_update
{
    u32_t component_root;
};

template <typename T>
class scc_color_program : public Fog_program<scc_color_vert_attr, scc_color_update, T>
{
	public:
        int out_loop;
        bool has_trim;
        struct mmap_config attr_mmap_config;
        struct scc_color_vert_attr * p_vert_attr;

        scc_color_program(int p_forward_backward_phase, bool p_init_sched, bool p_set_forward_backward, bool p_has_trim):Fog_program<scc_color_vert_attr, scc_color_update, T>(p_forward_backward_phase, p_init_sched, p_set_forward_backward)
        {
            out_loop = 0;
            has_trim = p_has_trim;
            if(has_trim)
            {
                attr_mmap_config = mmap_file(gen_config.attr_file_name);
                p_vert_attr = (struct scc_color_vert_attr*)attr_mmap_config.mmap_head;
                //std::cout<<"vert mmap ok!\n";
            }
        }

        void init( u32_t vid, scc_color_vert_attr* va, index_vert_array<T> * vert_index)
        {
            if (out_loop == 0 && this->forward_backward_phase == FORWARD_TRAVERSAL)
            {
                if(has_trim && (p_vert_attr+vid)->is_found==true)
                {
                    assert((p_vert_attr+vid)->component_root==vid);
                    va->prev_root = va->component_root = vid;
                    va->is_found = true;
                }
                else if (vert_index->num_edges(vid, OUT_EDGE) == 0 || vert_index->num_edges(vid, IN_EDGE) == 0)
                {
                    va->is_found = true;
                    va->prev_root = va->component_root = vid;
                }
                else
                {
                    va->is_found = false;
                    va->component_root = vid;
                    va->prev_root = (u32_t)-1;
                    fog_engine<scc_color_vert_attr, scc_color_update, T>::add_schedule(vid, this->CONTEXT_PHASE);
                }
            }
            else
            {
                if (this->forward_backward_phase == FORWARD_TRAVERSAL )
                {
                    if (va->component_root != va->prev_root)
                    {
                        va->prev_root = va->component_root;
                        va->component_root = vid;
                        fog_engine<scc_color_vert_attr, scc_color_update, T>::add_schedule(vid, this->CONTEXT_PHASE);
                    }
                }
                else
                {
                    assert(this->forward_backward_phase == BACKWARD_TRAVERSAL);
                    if (va->component_root != va->prev_root)
                    {
                        va->prev_root = va->component_root;
                        va->component_root = vid;
                        if (va->component_root == va->prev_root)
                        {
                            va->is_found = true;
                            fog_engine<scc_color_vert_attr, scc_color_update, T>::add_schedule(vid, this->CONTEXT_PHASE);
                        }
                    }
                    else
                    {
                        assert(va->component_root == va->prev_root);
                        if (va->is_found == false)
                        {
                            va->is_found = true;
                            fog_engine<scc_color_vert_attr, scc_color_update, T>::add_schedule(vid, this->CONTEXT_PHASE);
                        }
                    }
                }
            }
		}

		//scatter updates at vid-th vertex
		void scatter_one_edge(
                scc_color_vert_attr * this_vert,
                T &this_edge,
                u32_t backward_update_dest,
                update<scc_color_update> &this_update)
        {
            if (this->forward_backward_phase == FORWARD_TRAVERSAL)
            {
                this_update.dest_vert = this_edge.get_dest_value();
            }
            else
            {
                assert (this->forward_backward_phase == BACKWARD_TRAVERSAL);
                this_update.dest_vert = backward_update_dest;
            }
            this_update.vert_attr.component_root = this_vert->component_root;
		}
		//gather one update "u" from outside
		void gather_one_update( u32_t vid, scc_color_vert_attr* this_vert,
                struct update<scc_color_update>* this_update)
        {
            // just gather everything
            if (this->forward_backward_phase == FORWARD_TRAVERSAL)
            {
                if (this_update->vert_attr.component_root < this_vert->component_root && this_vert->is_found == false)
                {
                    this_vert->component_root = this_update->vert_attr.component_root;
                    fog_engine<scc_color_vert_attr, scc_color_update, T>::add_schedule(vid, this->CONTEXT_PHASE);
                }
            }
            else
            {
                assert (this->forward_backward_phase == BACKWARD_TRAVERSAL);
                if (this_update->vert_attr.component_root == this_vert->prev_root && this_vert->is_found == false)
                {
                    this_vert->component_root = this_update->vert_attr.component_root;
                    this_vert->is_found = true;
                    fog_engine<scc_color_vert_attr, scc_color_update, T>::add_schedule(vid, this->CONTEXT_PHASE);
                }
            }
		}
        void before_iteration()
        {
            if (this->forward_backward_phase == FORWARD_TRAVERSAL)
                PRINT_DEBUG("SCC engine is running FORWARD_TRAVERSAL for the %d-th iteration, there are %d tasks to schedule!\n",
                        this->loop_counter, this->num_tasks_to_sched);
            else
            {
                assert(this->forward_backward_phase == BACKWARD_TRAVERSAL);
                PRINT_DEBUG("SCC engine is running BACKWARD_TRAVERSAL for the %d-th iteration, there are %d tasks to schedule!\n",
                        this->loop_counter, this->num_tasks_to_sched);
            }
        }
        int after_iteration()
        {
            PRINT_DEBUG("SCC engine has finished the %d-th iteration, there are %d tasks to schedule at next iteration!\n",
                    this->loop_counter, this->num_tasks_to_sched);
            if (this->num_tasks_to_sched == 0)
                return ITERATION_STOP;
            else
                return ITERATION_CONTINUE;
        }
        int finalize(scc_color_vert_attr * va)
        {
            out_loop ++;
            if (this->forward_backward_phase == FORWARD_TRAVERSAL)
            {
                this->forward_backward_phase = BACKWARD_TRAVERSAL;
                this->loop_counter = 0;
                this->CONTEXT_PHASE = 0;
                return ENGINE_CONTINUE;
            }
            else
            {
                assert(this->forward_backward_phase == BACKWARD_TRAVERSAL);
                this->forward_backward_phase = FORWARD_TRAVERSAL;
                this->loop_counter = 0;
                this->CONTEXT_PHASE = 0;
                return ENGINE_CONTINUE;
            }
        }
        ~scc_color_program()
        {
            if(has_trim)
            {
                unmap_file(attr_mmap_config);
            }
        }
};


template <typename T>
class trim_program : public Fog_program<scc_color_vert_attr, scc_color_update, T>
{
    public:
        int out_loop;

        trim_program(int p_forward_backward_phase, bool p_init_sched, bool p_set_forward_backward):Fog_program<scc_color_vert_attr, scc_color_update, T>(p_forward_backward_phase, p_init_sched, p_set_forward_backward)
        {
            out_loop = 0;
        }

        void init( u32_t vid, scc_color_vert_attr * va, index_vert_array<T> * vert_index)
        {
            if(out_loop == 0)
            {
                assert(this->forward_backward_phase == FORWARD_TRAVERSAL);
                //va->fw_bw_label = vert_index->num_edges(vid, IN_EDGE);
                va->component_root = vert_index->num_edges(vid, IN_EDGE);
                //if(va->fw_bw_label == 0)
                if(va->component_root == 0)
                {
                    //va->fw_bw_label = 3;
                    va->prev_root = va->component_root = vid;
                    va->is_found = true;
                    fog_engine<scc_color_vert_attr, scc_color_update, T>::add_schedule(vid, this->CONTEXT_PHASE);
                }
                else
                {
                    va->prev_root = UINT_MAX;
                    va->is_found = false;
                }
            }
            else if(out_loop==1)
            {
                assert(this->forward_backward_phase == BACKWARD_TRAVERSAL);
                if(va->is_found==false)
                {
                    //va->fw_bw_label = vert_index->num_edges(vid, OUT_EDGE);
                    va->component_root = vert_index->num_edges(vid, OUT_EDGE);
                    //if(va->fw_bw_label==0)
                    if(va->component_root==0)
                    {
                        //va->fw_bw_label = 3;
                        va->prev_root = va->component_root = vid;
                        va->is_found = true;
                        fog_engine<scc_color_vert_attr, scc_color_update, T>::add_schedule(vid, this->CONTEXT_PHASE);
                    }
                }
            }
        }

        //scatter updates at vid-th vertex
        void scatter_one_edge(
                scc_color_vert_attr * this_vert,
                T &this_edge,
                u32_t backward_update_dest,
                update<scc_color_update> &this_update)
        {
            if (this->forward_backward_phase == FORWARD_TRAVERSAL)
            {
                this_update.dest_vert = this_edge.get_dest_value();
            }
            else
            {
                assert (this->forward_backward_phase == BACKWARD_TRAVERSAL);
                this_update.dest_vert = backward_update_dest;
            }
            this_update.vert_attr.component_root = this_vert->component_root;
		}

        //gather one update "u" from outside
        void gather_one_update( u32_t vid, scc_color_vert_attr* this_vert,
                struct update<scc_color_update>* this_update)
        {
            /*
             * just gather everything
             */
            if(this_vert->is_found == false)
            {
                this_vert->component_root--;
                if(0==this_vert->component_root)
                {
                    this_vert->prev_root = this_vert->component_root = vid;
                    this_vert->is_found = true;
                    fog_engine<scc_color_vert_attr, scc_color_update, T>::add_schedule(vid, this->CONTEXT_PHASE);
                }
            }
            else
            {
                assert(vid == this_vert->component_root);
            }
        }
        void before_iteration()
        {
            if (this->forward_backward_phase == FORWARD_TRAVERSAL)
                PRINT_DEBUG("TRIM engine is running FORWARD_TRAVERSAL for the %d-th iteration, there are %d tasks to schedule!\n",
                        this->loop_counter, this->num_tasks_to_sched);
            else
            {
                assert(this->forward_backward_phase == BACKWARD_TRAVERSAL);
                PRINT_DEBUG("TRIM engine is running BACKWARD_TRAVERSAL for the %d-th iteration, there are %d tasks to schedule!\n",
                        this->loop_counter, this->num_tasks_to_sched);
            }
        }
        int after_iteration()
        {
            PRINT_DEBUG("TRIM engine has finished the %d-th iteration, there are %d tasks to schedule at next iteration!\n",
                    this->loop_counter, this->num_tasks_to_sched);
            if (this->num_tasks_to_sched == 0)
                return ITERATION_STOP;
            else
                return ITERATION_CONTINUE;
        }
        int finalize(scc_color_vert_attr * va)
        {
            out_loop ++;
            if (out_loop==2)
            {
                return ENGINE_STOP;
            }
            if (this->forward_backward_phase == FORWARD_TRAVERSAL)
            {
                this->forward_backward_phase = BACKWARD_TRAVERSAL;
                this->loop_counter = 0;
                this->CONTEXT_PHASE = 0;
                return ENGINE_CONTINUE;
            }
            else
            {
                assert(this->forward_backward_phase == BACKWARD_TRAVERSAL);
                this->forward_backward_phase = FORWARD_TRAVERSAL;
                this->loop_counter = 0;
                this->CONTEXT_PHASE = 0;
                return ENGINE_CONTINUE;
            }
        }
        int num_of_remain_partitions(){
            return 1;
        }
        int judge_for_filter(scc_color_vert_attr * this_vert){
            if(false==this_vert->is_found){
                return 0;
            }
            return -1;
        }
};

template <typename T>
u32_t select_pivot(const struct task_config * p_task_config)
{
    gen_config.min_vert_id = p_task_config->min_vert_id;
    gen_config.max_vert_id = p_task_config->max_vert_id;
    gen_config.num_edges = p_task_config->num_edges;
    gen_config.vert_file_name = p_task_config->vert_file_name;
    gen_config.edge_file_name = p_task_config->edge_file_name;
    gen_config.attr_file_name = p_task_config->attr_file_name;
    gen_config.in_vert_file_name = p_task_config->in_vert_file_name;
    gen_config.in_edge_file_name = p_task_config->in_edge_file_name;
    index_vert_array<T> * vert_index = new index_vert_array<T>;
    u64_t temp_deg = 0;
    u64_t max_deg = 0;
    u32_t pivot = gen_config.min_vert_id;
    for (u32_t i = gen_config.min_vert_id; i <= gen_config.max_vert_id; i++)
    {
        temp_deg = vert_index->num_edges(i, IN_EDGE) * vert_index->num_edges(i, OUT_EDGE);
        if(temp_deg > max_deg)
        {
            pivot = i;
            max_deg = temp_deg;
        }

    }
    delete vert_index;
    return pivot;
}

template <typename T>
void start_engine()
{
    start_time = time(NULL);

    int check = access(gen_config.in_edge_file_name.c_str(), F_OK);
    if(-1 ==check )
    {
        PRINT_ERROR("in_edge file doesn't exit or '-i' is false!\n");
    }

    TASK_ID = 0;
    std::queue< Fog_task<scc_vert_attr, scc_update, T> * > fb_queue_task;
    std::queue< Fog_task<scc_color_vert_attr, scc_color_update, T> * > color_queue_task;

    //task 0
    std::string desc_name = vm["graph"].as<std::string>();
    struct task_config * ptr_task_config = new struct task_config(false, TASK_ID, desc_name);

    result_filename = gen_config.vert_file_name.substr(0, gen_config.vert_file_name.find_last_of(".")) + ".result";

    if(in_mem(ptr_task_config, sizeof(struct scc_color_vert_attr)))
    {
        Fog_task<scc_color_vert_attr, scc_color_update, T> *task = new Fog_task<scc_color_vert_attr, scc_color_update, T>();
        task->set_task_config(ptr_task_config);
        task->set_task_id(TASK_ID);
        assert(color_queue_task.empty());
        color_queue_task.push(task);
    }
    else
    {
        Fog_task<scc_vert_attr, scc_update, T> *task = new Fog_task<scc_vert_attr, scc_update, T>();
        task->set_task_config(ptr_task_config);
        task->set_task_id(TASK_ID);
        assert(fb_queue_task.empty());
        fb_queue_task.push(task);
    }


    fog_engine<scc_vert_attr, scc_update, T> * eng_fb;
    eng_fb = new fog_engine<scc_vert_attr, scc_update, T>(TARGET_ENGINE);


    while(!fb_queue_task.empty())
    {
        struct Fog_task<scc_vert_attr, scc_update, T> * main_task = fb_queue_task.front();
        fb_queue_task.pop();
        PRINT_DEBUG("*********************************** task %d****************************************\n", main_task->get_task_id());

        PRINT_DEBUG_TEST_LOG("TASK %d starts at time = %.f seconds\n", main_task->get_task_id(), difftime(time(NULL), start_time));

        u32_t pivot = select_pivot<T>(main_task->m_task_config);
        Fog_program<scc_vert_attr, scc_update, T> *scc_ptr = new scc_fb_program<T>(FORWARD_TRAVERSAL, true, false, pivot);

        main_task->set_alg_ptr(scc_ptr);
        eng_fb->run_task(main_task);

        PRINT_DEBUG_TEST_LOG("TASK %d graph mutation at time = %.f seconds\n", main_task->get_task_id(), difftime(time(NULL), start_time));
        /*
        Filter<scc_vert_attr> * filter = new Filter<scc_vert_attr>();
        filter->do_scc_filter(eng_fb->get_attr_array_header(), main_task->get_task_id());
        delete filter;
        */
        Filter<scc_vert_attr, scc_update, T> * filter = new Filter<scc_vert_attr, scc_update, T>();
        filter->set_alg(scc_ptr);
        filter->do_filter(eng_fb->get_attr_array_header(), main_task->get_task_id());
        delete filter;

        eng_fb->create_subtask_dataset();
        create_result_fb(eng_fb->get_attr_array_header(), main_task->m_task_config);
        std::cout<<"first task result over\n";

        for(u32_t i = 0; i < task_bag_config_vec.size(); i++)
        {
            struct bag_config b_config = task_bag_config_vec[i];
            if(b_config.data_size==0)
            {
                continue;
            }
            std::string temp = task_bag_config_vec[i].data_name;
            std::string desc_data_name = temp.substr(0, temp.find_last_of(".")) + ".desc";
            init_graph_desc(desc_data_name);

            struct task_config * ptr_sub_task_config = new struct task_config(true, main_task->m_task_config, b_config.bag_id, desc_data_name);

            vec_bag_config.push_back(b_config);

            if(in_mem(ptr_sub_task_config, sizeof(struct scc_color_vert_attr)))
            {
                Fog_task<scc_color_vert_attr, scc_color_update, T> *sub_task = new Fog_task<scc_color_vert_attr, scc_color_update, T>();
                sub_task->set_task_config(ptr_sub_task_config);
                sub_task->set_task_id(b_config.bag_id);
                color_queue_task.push(sub_task);
            }
            else
            {
                Fog_task<scc_vert_attr, scc_update, T> *sub_task = new Fog_task<scc_vert_attr, scc_update, T>();
                sub_task->set_task_config(ptr_sub_task_config);
                sub_task->set_task_id(b_config.bag_id);
                fb_queue_task.push(sub_task);
            }
        }
        task_bag_config_vec.clear();

        delete scc_ptr;
        delete main_task->m_task_config;
        delete main_task;
    }
    std::cout<<color_queue_task.size()<<std::endl;



    delete eng_fb;
    fog_engine<scc_color_vert_attr, scc_color_update, T> * eng_color;
    eng_color = new fog_engine<scc_color_vert_attr, scc_color_update, T>(TARGET_ENGINE);

    //Filter<scc_color_vert_attr> * color_filter = new Filter<scc_color_vert_attr>();
    Filter<scc_color_vert_attr, scc_color_update, T> * color_filter = new Filter<scc_color_vert_attr, scc_color_update, T>();
    while(!color_queue_task.empty())
    {
        struct Fog_task<scc_color_vert_attr, scc_color_update, T> * sub_task = color_queue_task.front();
        color_queue_task.pop();
        PRINT_DEBUG("*********************************** task %d****************************************\n", sub_task->get_task_id());
        PRINT_DEBUG_TEST_LOG("TASK %d starts at time = %.f seconds\n", sub_task->get_task_id(), difftime(time(NULL), start_time));

        Fog_program<scc_color_vert_attr, scc_color_update, T> * scc_trim_ptr = new trim_program<T>(FORWARD_TRAVERSAL, true, false);
        sub_task->set_alg_ptr(scc_trim_ptr);
        eng_color->run_task(sub_task);
        //color_filter->do_trim_filter(eng_color->get_attr_array_header(), sub_task->get_task_id());
        color_filter->set_alg(scc_trim_ptr);
        color_filter->do_filter(eng_color->get_attr_array_header(), sub_task->get_task_id());
        assert(1==task_bag_config_vec.size());

        Fog_program<scc_color_vert_attr, scc_color_update, T> *scc_ptr;
        //***************how to deal with task_bag_config_vec[0].size == 0;*************//
        if( 2*(u32_t)task_bag_config_vec[0].data_size <= gen_config.max_vert_id+1)
        {
            eng_color->create_subtask_dataset();
            create_result_coloring(eng_color->get_attr_array_header(), sub_task->m_task_config);

            std::string temp = task_bag_config_vec[0].data_name;
            std::string desc_data_name = temp.substr(0, temp.find_last_of(".")) + ".desc";
            init_graph_desc(desc_data_name);

            struct task_config * ptr_sub_color_task_config = new struct task_config(true, sub_task->get_task_config(), task_bag_config_vec[0].bag_id, desc_data_name);

            sub_task->set_task_config(ptr_sub_color_task_config);
            sub_task->set_task_id(task_bag_config_vec[0].bag_id);

            scc_ptr = new scc_color_program<T>(FORWARD_TRAVERSAL, true, false, false);
        }
        else
        {
            scc_ptr = new scc_color_program<T>(FORWARD_TRAVERSAL, true, false, true);
            sub_task->m_task_config->attr_file_name = sub_task->m_task_config->attr_file_name + "C";
            std::cout<<sub_task->m_task_config->attr_file_name<<std::endl;
        }

        PRINT_DEBUG("*********************************** task %d****************************************\n", sub_task->get_task_id());
        PRINT_DEBUG_TEST_LOG("TASK %d starts at time = %.f seconds\n", sub_task->get_task_id(), difftime(time(NULL), start_time));
        sub_task->set_alg_ptr(scc_ptr);
        eng_color->run_task(sub_task);
        create_result_coloring(eng_color->get_attr_array_header(), sub_task->m_task_config);

        delete scc_ptr;
        delete sub_task->m_task_config;
        delete sub_task;
        task_bag_config_vec.clear();
    }

    //delete task;
    delete color_filter;
    delete eng_color;
    PRINT_DEBUG("scc runtime = %.f seconds\n", difftime(time(NULL), start_time));
    PRINT_DEBUG_TEST_LOG("program ending at time = %.f seconds\n", difftime(time(NULL), start_time));
}

void create_result_fb(struct scc_vert_attr * task_vert_attr, struct task_config * p_task_config)
{
    int fd = 0;
    size_t len = 0;
    bool is_min_id = true;
    u32_t min_id = 0;

    fd = open(result_filename.c_str(), O_RDWR|O_CREAT, S_IRUSR|S_IWUSR);
    if(-1==fd)
    {
        std::cout<<"open "<<result_filename<<" error!"<<std::endl;
        exit(-1);
    }

    if(p_task_config->vec_prefix_task_id.size()==1)
    {
        std::cout<<"first task result\n";
        assert(p_task_config->vec_prefix_task_id[0] == 0);
        len = sizeof(u32_t) * (p_task_config->max_vert_id+1);
        if( ftruncate(fd, len) < 0 )
        {
            std::cout<<"result file ftruncate error!"<<std::endl;
            exit(-1);
        }
        components_label_ptr = (u32_t*)mmap(NULL, len, PROT_WRITE, MAP_SHARED, fd, 0);
        for(u32_t id = 0; id <= p_task_config->max_vert_id; id++)
        {
            if(task_vert_attr[id].fw_bw_label==2 && task_vert_attr[id].is_found)
            {
                if(is_min_id)
                {
                    min_id = id;
                    is_min_id = false;
                }
                components_label_ptr[id] = min_id;
            }
            else if(task_vert_attr[id].fw_bw_label==3)
            {
                assert(task_vert_attr[id].is_found==true);
                components_label_ptr[id] = id;
            }
        }
    }
    else
    {
        struct stat st;
        fstat(fd, &st);
        len = st.st_size;
        components_label_ptr = (u32_t*)mmap(NULL, len, PROT_WRITE, MAP_SHARED, fd, 0);

        u32_t prefix_len = p_task_config->vec_prefix_task_id.size();

        struct mmap_config * remap_map_config = new struct mmap_config[prefix_len-1];
        u32_t ** remap_array = new u32_t*[prefix_len-1];
        for(u32_t i = 0; i < prefix_len-1; i++)
        {
            std::stringstream tmp_str_stream;
            tmp_str_stream<<(p_task_config->vec_prefix_task_id[i+1]);
            std::string filename = p_task_config->remap_file_name;
            filename = filename.substr(0, filename.find_last_of("/")+1);
            filename = filename + "temp"+ tmp_str_stream.str() + ".remap";
            remap_map_config[i] = mmap_file(filename);
            remap_array[i] = (u32_t*)remap_map_config[i].mmap_head;
        }

        u32_t origin_id = 0;
        int prefix = 0;
        for(u32_t id = 0; id < p_task_config->max_vert_id; id++)
        {
            origin_id = id;
            prefix = prefix_len-1;
            if(task_vert_attr[id].fw_bw_label==2 && task_vert_attr[id].is_found)
            {
                while(prefix > 0)
                {
                    prefix--;
                    origin_id = remap_array[prefix][origin_id];
                }
                if(is_min_id)
                {
                    min_id = origin_id;
                    is_min_id = false;
                }
                components_label_ptr[origin_id] = min_id;
            }
            else if(task_vert_attr[id].fw_bw_label==3)
            {
                assert(task_vert_attr[id].is_found==true);
                while(prefix > 0)
                {
                    prefix--;
                    origin_id = remap_array[prefix][origin_id];
                }
                components_label_ptr[id] = origin_id;
            }

        }

        for(u32_t i = 0; i < prefix_len-1; i++)
        {
            unmap_file(remap_map_config[i]);
        }
        delete []remap_map_config;
        delete []remap_array;
    }

    close(fd);
    munmap((void*)components_label_ptr, len);

}

void create_result_coloring(struct scc_color_vert_attr * task_vert_attr, struct task_config * p_task_config)
{
    int fd = 0;
    fd = open(result_filename.c_str(), O_RDWR|O_CREAT, S_IRUSR|S_IWUSR);
    size_t len = 0;
    if(-1==fd)
    {
        std::cout<<"open "<<result_filename<<" error!"<<std::endl;
        exit(-1);
    }
    if(p_task_config->vec_prefix_task_id.size()==1)
    {
        assert(p_task_config->vec_prefix_task_id[0] == 0);
        len = sizeof(u32_t) * (p_task_config->max_vert_id+1);
        if( ftruncate(fd, len) < 0 )
        {
            std::cout<<"result file ftruncate error!"<<std::endl;
            exit(-1);
        }
        components_label_ptr = (u32_t*)mmap(NULL, len, PROT_WRITE, MAP_SHARED, fd, 0);
        //create components_label_ptr
        for(u32_t id = 0; id <= p_task_config->max_vert_id; id++)
        {
            if(task_vert_attr[id].is_found==false)
            {
                continue;
            }
            components_label_ptr[id] = task_vert_attr[id].component_root;
        }
    }
    else
    {
        struct stat st;
        fstat(fd, &st);
        len = st.st_size;
        components_label_ptr = (u32_t*)mmap(NULL, len, PROT_WRITE, MAP_SHARED, fd, 0);

        u32_t prefix_len = p_task_config->vec_prefix_task_id.size();


        struct mmap_config * remap_map_config = new struct mmap_config[prefix_len-1];
        u32_t ** remap_array = new u32_t*[prefix_len-1];
        for(u32_t i = 0; i < prefix_len-1; i++)
        {
            std::stringstream tmp_str_stream;
            tmp_str_stream<<(p_task_config->vec_prefix_task_id[i+1]);
            std::string filename = p_task_config->remap_file_name;
            std::cout<<filename<<std::endl;
            filename = filename.substr(0, filename.find_last_of("/")+1);
            filename = filename + "temp"+ tmp_str_stream.str() + ".remap";
            remap_map_config[i] = mmap_file(filename);
            remap_array[i] = (u32_t *)remap_map_config[i].mmap_head;
        }

        u32_t origin_id = 0;
        u32_t origin_component_root = 0;
        int prefix = 0;
        for(u32_t id = 0; id <= p_task_config->max_vert_id; id++)
        {
            if(task_vert_attr[id].is_found==false)
            {
                continue;
            }
            origin_id = id;
            origin_component_root = task_vert_attr[id].component_root;
            prefix = prefix_len - 1;
            while(prefix > 0)
            {
                prefix--;
                origin_id = remap_array[prefix][origin_id];
                origin_component_root = remap_array[prefix][origin_component_root];
            }
            components_label_ptr[origin_id] = origin_component_root;
        }

        for(u32_t i = 0; i < prefix_len-1; i++)
        {
            unmap_file(remap_map_config[i]);
        }
        delete []remap_map_config;
        delete []remap_array;
    }
    munmap((void*)components_label_ptr, len);
    close(fd);
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

    PRINT_DEBUG("result file is %s\n", result_filename.c_str());

    return 0;

}
