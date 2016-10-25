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
                std::cout<<gen_config.attr_file_name<<std::endl;
                //attr_mmap_config = mmap_file(gen_config.attr_file_name.substr(0,gen_config.attr_file_name.length()-1));
                attr_mmap_config = mmap_file(gen_config.attr_file_name);
                p_vert_attr = (struct scc_color_vert_attr*)attr_mmap_config.mmap_head;
                std::cout<<"vert mmap ok!\n";
                std::cout<<(p_vert_attr+1)->component_root<<std::endl;
            }
        }

        void init( u32_t vid, scc_color_vert_attr* va, index_vert_array<T> * vert_index)
        {
            if (out_loop == 0 && this->forward_backward_phase == FORWARD_TRAVERSAL)
            {
                if(has_trim && (p_vert_attr+vid)->is_found==true)
                {
                    //assert((p_vert_attr+vid)->component_root==vid);
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
            //return ret;
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
            //ret->vert_attr.
            //return ret;
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

};

template <typename T>
//u32_t select_pivot(const index_vert_array<T> * vert_index)
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
    //debug
    //u64_t max_in_deg = 0;
    //u64_t max_out_deg = 0;
    //end
    for (u32_t i = gen_config.min_vert_id; i <= gen_config.max_vert_id; i++)
    {
        //debug
        /*
        T edge;
        u32_t edges = vert_index->num_edges(i, OUT_EDGE);
        for(u32_t j = 0; j < edges; j++)
        {
            vert_index->get_out_edge(i, j, edge);
            std::cout<<i<<"->"<<edge.get_dest_value()<<std::endl;
        }
        */
        //if(vert_index->num_edges(i, IN_EDGE) > max_in_deg)
        //{
            //max_in_deg = vert_index->num_edges(i, IN_EDGE);
        //}
        //if(vert_index->num_edges(i, OUT_EDGE) > max_out_deg)
        //{
            //max_out_deg = vert_index->num_edges(i, OUT_EDGE);
        //}
        //end
        temp_deg = vert_index->num_edges(i, IN_EDGE) * vert_index->num_edges(i, OUT_EDGE);
        if(temp_deg > max_deg)
        {
            pivot = i;
            max_deg = temp_deg;
        }

    }
    //debug
    //std::cout<<max_in_deg<<std::endl;
    //std::cout<<max_out_deg<<std::endl;
    //end
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
    ptr_task_config->vec_prefix_task_id.push_back(TASK_ID);

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


    //fb_queue_task_id.push(TASK_ID);
    //while(!fb_queue_task_id.empty())
    while(!fb_queue_task.empty())
    {
        //temp_task_id = fb_queue_task_id.front();
        //std::cout<<"task : "<<temp_task_id<<std::endl;
        //fb_queue_task_id.pop();
        struct Fog_task<scc_vert_attr, scc_update, T> * main_task = fb_queue_task.front();
        fb_queue_task.pop();
        PRINT_DEBUG("*********************************** task %d****************************************\n", main_task->get_task_id());
        
        PRINT_DEBUG_TEST_LOG("TASK %d starts at time = %.f seconds\n", main_task->get_task_id(), difftime(time(NULL), start_time));

        u32_t pivot = select_pivot<T>(main_task->m_task_config);
        //exit(0);
        //u32_t pivot = select_pivot<T>(eng_fb->get_vert_index());
        Fog_program<scc_vert_attr, scc_update, T> *scc_ptr = new scc_fb_program<T>(FORWARD_TRAVERSAL, true, false, pivot);

        //index_vert_array<type1_edge> * vert_index = new index_vert_array<type1_edge>;
        //if(typeid(*scc_ptr) == typeid(scc_fb_program<T>))
        //{
        //}
        main_task->set_alg_ptr(scc_ptr);
        eng_fb->run_task(main_task);
        
        PRINT_DEBUG_TEST_LOG("TASK %d graph mutation at time = %.f seconds\n", main_task->get_task_id(), difftime(time(NULL), start_time));
        //filter->do_scc_filter(eng_fb->get_attr_array_header(), eng_fb->get_vert_index(), main_task->get_task_id());
        Filter<scc_vert_attr> * filter = new Filter<scc_vert_attr>();
        filter->do_scc_filter(eng_fb->get_attr_array_header(), main_task->get_task_id());
        delete filter;

        eng_fb->create_subtask_dataset();
        create_result_fb(eng_fb->get_attr_array_header(), main_task->m_task_config);
        std::cout<<"first task result over\n";

        //while(!queue_bag_config.empty())
        for(u32_t i = 0; i < task_bag_config_vec.size(); i++)  
        {
            struct bag_config b_config = task_bag_config_vec[i];
            if(b_config.data_size==0)
            {
                continue;
            }
            struct task_config * ptr_sub_task_config = new struct task_config;
            //std::cout<<b_config.bag_id<<std::endl;
            //std::cout<<b_config.data_size<<std::endl;
            //std::cout<<b_config.data_name<<std::endl;
            std::string temp = task_bag_config_vec[i].data_name;
            std::string desc_data_name = temp.substr(0, temp.find_last_of(".")) + ".desc";
            init_graph_desc(desc_data_name);
            
            ptr_sub_task_config->min_vert_id = pt.get<u32_t>("description.min_vertex_id");
            ptr_sub_task_config->max_vert_id = pt.get<u32_t>("description.max_vertex_id");
            ptr_sub_task_config->num_edges = pt.get<u32_t>("description.num_of_edges");
            ptr_sub_task_config->is_remap = true;
            ptr_sub_task_config->remap_file_name = desc_data_name.substr(0, desc_data_name.find_last_of(".")) + ".remap";
            ptr_sub_task_config->vert_file_name  = desc_data_name.substr(0, desc_data_name.find_last_of(".")) + ".index";
            ptr_sub_task_config->edge_file_name  = desc_data_name.substr(0, desc_data_name.find_last_of(".")) + ".edge";
            ptr_sub_task_config->attr_file_name  = desc_data_name.substr(0, desc_data_name.find_last_of(".")) + ".attr";
            ptr_sub_task_config->with_in_edge = gen_config.with_in_edge;
            //in order to create result
            ptr_sub_task_config->vec_prefix_task_id = main_task->m_task_config->vec_prefix_task_id;
            ptr_sub_task_config->vec_prefix_task_id.push_back(b_config.bag_id);

            vec_bag_config.push_back(b_config);

            //ptr_sub_task_config->with_in_edge = ;
            if(gen_config.with_in_edge)
            {
                ptr_sub_task_config->in_vert_file_name = desc_data_name.substr(0, desc_data_name.find_last_of(".")) + ".in-index";
                ptr_sub_task_config->in_edge_file_name = desc_data_name.substr(0, desc_data_name.find_last_of(".")) + ".in-edge";
            }


            //remain to do : use the bag.data_size to decide which algorithm is the best(suitable) 
            //Fog_task<scc_vert_attr, scc_update, T> *sub_task = new Fog_task<scc_vert_attr, scc_update, T>();

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

            //remain to do : use the bag.data_size to decide which algorithm is the best(suitable) 
            //fb_queue_task.push(sub_task);

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

    Filter<scc_color_vert_attr> * color_filter = new Filter<scc_color_vert_attr>();
    while(!color_queue_task.empty())
    {
        struct Fog_task<scc_color_vert_attr, scc_color_update, T> * sub_task = color_queue_task.front();
        color_queue_task.pop();
        PRINT_DEBUG("*********************************** task %d****************************************\n", sub_task->get_task_id());
        PRINT_DEBUG_TEST_LOG("TASK %d starts at time = %.f seconds\n", sub_task->get_task_id(), difftime(time(NULL), start_time));

        Fog_program<scc_color_vert_attr, scc_color_update, T> * scc_trim_ptr = new trim_program<T>(FORWARD_TRAVERSAL, true, false);
        sub_task->set_alg_ptr(scc_trim_ptr);
        eng_color->run_task(sub_task);
        color_filter->do_trim_filter(eng_color->get_attr_array_header(), sub_task->get_task_id());
        assert(1==task_bag_config_vec.size());

        Fog_program<scc_color_vert_attr, scc_color_update, T> *scc_ptr;
        //***************how to deal with task_bag_config_vec[0].size == 0;*************//
        if( 2*(u32_t)task_bag_config_vec[0].data_size <= gen_config.max_vert_id+1)
        {
            eng_color->create_subtask_dataset();
            create_result_coloring(eng_color->get_attr_array_header(), sub_task->m_task_config);

            struct task_config * ptr_sub_color_task_config = new struct task_config;
            std::string temp = task_bag_config_vec[0].data_name;
            std::string desc_data_name = temp.substr(0, temp.find_last_of(".")) + ".desc";
            init_graph_desc(desc_data_name);

            ptr_sub_color_task_config->min_vert_id = pt.get<u32_t>("description.min_vertex_id");
            ptr_sub_color_task_config->max_vert_id = pt.get<u32_t>("description.max_vertex_id");
            ptr_sub_color_task_config->num_edges = pt.get<u32_t>("description.num_of_edges");
            ptr_sub_color_task_config->is_remap = true;
            ptr_sub_color_task_config->remap_file_name = desc_data_name.substr(0, desc_data_name.find_last_of(".")) + ".remap";
            ptr_sub_color_task_config->vert_file_name  = desc_data_name.substr(0, desc_data_name.find_last_of(".")) + ".index";
            ptr_sub_color_task_config->edge_file_name  = desc_data_name.substr(0, desc_data_name.find_last_of(".")) + ".edge";
            ptr_sub_color_task_config->attr_file_name  = desc_data_name.substr(0, desc_data_name.find_last_of(".")) + ".attr";
            ptr_sub_color_task_config->with_in_edge = gen_config.with_in_edge;
            if(gen_config.with_in_edge)
            {
                ptr_sub_color_task_config->in_vert_file_name = desc_data_name.substr(0, desc_data_name.find_last_of(".")) + ".in-index";
                ptr_sub_color_task_config->in_edge_file_name = desc_data_name.substr(0, desc_data_name.find_last_of(".")) + ".in-edge";
            }
            ptr_sub_color_task_config->vec_prefix_task_id = sub_task->get_task_config()->vec_prefix_task_id;
            ptr_sub_color_task_config->vec_prefix_task_id.push_back(task_bag_config_vec[0].bag_id);

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



        //gen_config.memory_size = sizeof(scc_color_vert_attr) * ( sub_task->m_task_config->max_vert_id + 1 ) * 3; 

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

/*
void create_result()
{
    time_t start_time;
    time_t end_time;

    start_time = time(NULL);

    struct mmap_config base_attr_map_config;
    struct mmap_config attr_map_config[3];
    struct mmap_config remap_map_config[3];

    //struct mmap_config attr1_map_config;
    //struct mmap_config attr2_map_config;
    //struct mmap_config attr3_map_config;
    
    struct scc_vert_attr * base_attr_array_head = NULL;
    struct scc_color_vert_attr * attr_array_head[3];
    u32_t * remap_array_head[3]; 
    
    //struct scc_color_vert_attr * attr1_array_head = NULL;
    //struct scc_color_vert_attr * attr2_array_head = NULL;
    //struct scc_color_vert_attr * attr3_array_head = NULL;

    std::string desc_name = vm["graph"].as<std::string>();
    std::string base_attr_file_name = desc_name.substr(0, desc_name.find_last_of(".")) + ".attr";
    std::cout<<base_attr_file_name<<std::endl;
    base_attr_map_config = mmap_file(base_attr_file_name);
    base_attr_array_head = (struct scc_vert_attr *)base_attr_map_config.mmap_head; 


    for(u32_t i = 0; i < vec_bag_config.size(); i++)
    {
        std::string remap_array_name = vec_bag_config[i].data_name; 

        //int id = vec_bag_config[i].bag_id;
        //std::cout<<id<<std::endl;
        std::string attr_array_name = remap_array_name.substr(0, remap_array_name.find_last_of(".")+1) + "attr";

        std::cout<<remap_array_name<<std::endl;
        std::cout<<attr_array_name<<std::endl;

        remap_map_config[i] = mmap_file(remap_array_name);
        remap_array_head[i] = (u32_t *)remap_map_config[i].mmap_head;

        attr_map_config[i] = mmap_file(attr_array_name);
        attr_array_head[i] = (struct scc_color_vert_attr *)attr_map_config[i].mmap_head;

    }

    //create result
    init_graph_desc(vm["graph"].as<std::string>());
    u32_t max_vert_id = pt.get<u32_t>("description.max_vertex_id");
    //std::cout<<max_vert_id<<std::endl;
    u32_t origin_id = 0;
    u32_t FW_suffix = 0;
    u32_t BW_suffix = 0;
    u32_t RM_suffix = 0; 
    bool is_min_id = true;
    u32_t min_id = 0;
    u32_t scc_count = 0;
    for(u32_t id = 0; id <= max_vert_id; id++)
    {
        //PRINT_DEBUG_LOG("vertex %d, fw_bw_label=%d, is_found=%d\n", id, (base_attr_array_head+id)->fw_bw_label,(base_attr_array_head+id)->is_found);
        switch((base_attr_array_head + id)->fw_bw_label)
        {
            case 1:
                {
                    assert((base_attr_array_head+id)->is_found==false);
                    origin_id = *(remap_array_head[0]+FW_suffix);
                    assert((attr_array_head[0]+FW_suffix)->prev_root == (attr_array_head[0]+FW_suffix)->component_root);
                    assert((attr_array_head[0]+FW_suffix)->is_found==true);
                    PRINT_DEBUG_TEST_LOG("vertex %d,\tcomponent = %d\n", origin_id, *(remap_array_head[0]+(attr_array_head[0]+FW_suffix)->component_root));
                    if(origin_id==(*(remap_array_head[0]+(attr_array_head[0]+FW_suffix)->component_root)))
                    {
                        scc_count++;
                    }
                    FW_suffix++;

                    break;
                }
            
            case 2:
                {
                    if((base_attr_array_head+id)->is_found)
                    {
                        if(is_min_id)
                        {
                            min_id = id;
                            scc_count++;
                            is_min_id = false;
                        }
                        PRINT_DEBUG_TEST_LOG("vertex %d,\tcomponent = %d\n", id, min_id);
                    }
                    else
                    {
                        assert((base_attr_array_head+id)->is_found==false);
                        origin_id = *(remap_array_head[1]+BW_suffix);
                        assert((attr_array_head[1]+BW_suffix)->prev_root == (attr_array_head[1]+BW_suffix)->component_root);
                        assert((attr_array_head[1]+BW_suffix)->is_found==true);
                        PRINT_DEBUG_TEST_LOG("vertex %d,\tcomponent = %d\n", origin_id, *(remap_array_head[1]+(attr_array_head[1]+BW_suffix)->component_root));
                        if(origin_id==(*(remap_array_head[1]+(attr_array_head[1]+BW_suffix)->component_root)))
                        {
                            scc_count++;
                        }
                        BW_suffix++;
                    }
                    break;
                }

            case 3:
                {
                    assert((base_attr_array_head+id)->is_found==true);
                    scc_count++;
                    PRINT_DEBUG_TEST_LOG("vertex %d,\tcomponent = %d\n", id, id);
                    break;
                }
            case UINT_MAX:
                {
                    assert((base_attr_array_head+id)->is_found==false);
                    origin_id = *(remap_array_head[2]+RM_suffix);
                    assert((attr_array_head[2]+RM_suffix)->prev_root == (attr_array_head[2]+RM_suffix)->component_root);
                    assert((attr_array_head[2]+RM_suffix)->is_found==true);
                    PRINT_DEBUG_TEST_LOG("vertex %d,\tcomponent = %d\n", origin_id, *(remap_array_head[2]+(attr_array_head[2]+RM_suffix)->component_root));
                    if(origin_id==(*(remap_array_head[2]+(attr_array_head[2]+RM_suffix)->component_root)))
                    {
                        scc_count++;
                    }
                    RM_suffix++;
                    break;
                }

            default:
                PRINT_ERROR("WRONG!!!\n");

        }


    }
    PRINT_DEBUG_TEST_LOG("total scc num: %d\n", scc_count);

    //unmap
    unmap_file(base_attr_map_config);
    for(u32_t i = 0; i < vec_bag_config.size(); i++)
    {
        //int id = vec_bag_config[i].bag_id;
        unmap_file(remap_map_config[i]);
        unmap_file(attr_map_config[i]);
    }

    end_time = time(NULL);

    PRINT_DEBUG("The time for create result is %.f seconds\n", difftime(end_time, start_time));
}
*/

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
            //filename = filename.substr(0, filename.find_last_of(".")-1);
            filename = filename.substr(0, filename.find_last_of("/")+1);
            filename = filename + "temp"+ tmp_str_stream.str() + ".remap";
            //debug
            //std::cout<<filename<<std::endl;
            //end
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
            //filename = filename.substr(0, filename.find_last_of(".")-1);
            filename = filename.substr(0, filename.find_last_of("/")+1);
            filename = filename + "temp"+ tmp_str_stream.str() + ".remap";
            //debug
            //std::cout<<filename<<std::endl;
            //end
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
    //time_t start_time;
    //time_t end_time;
    Fog_adapter *adapter = new Fog_adapter();
    unsigned int type1_or_type2 = adapter->init(argc, argv);

    //start_time = time(NULL);

    if(1 == type1_or_type2)
    {
        start_engine<type1_edge>();
    }   
    else
    {
        start_engine<type2_edge>();
    }   

    //create_result();
    //end_time = time(NULL);
    //PRINT_DEBUG_TEST_LOG("time = %.f seconds\n", difftime(time(NULL), start_time));

    //PRINT_DEBUG("The SCC program's run time = %.f seconds\n", difftime(end_time, start_time));
    PRINT_DEBUG("result file is %s\n", result_filename.c_str());

    return 0;

}
