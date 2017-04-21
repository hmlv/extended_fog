/**************************************************************************************************
 * Authors:
 *   Zhiyuan Shao, Jian He, Huiming Lv
 *
 * Routines:
 *   CPU (computing) threads.
 *
 * Notes:
 *   1.if vertex's attribute is in the attr_buf, we will use attr_buf instead of mmap
 *     modified by Huiming Lv   2015/1/23
 *************************************************************************************************/

//#include "fog_adapter.h"
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
#include <fstream>
#include <sstream>
#include <fcntl.h>
#include <index_vert_array.hpp>

#include "convert.h"
#include "cpu_thread.hpp"

#define REMAP_EDGE_BUFFER_LEN 2048*2048
#define REMAP_VERT_BUFFER_LEN 2048*2048

template <typename VA, typename U, typename T>
cpu_work<VA, U, T>::cpu_work( u32_t state, void* state_param_in)
    :engine_state(state), state_param(state_param_in)
{
}

template <typename VA, typename U, typename T>
void cpu_work<VA, U, T>::operator() ( u32_t processor_id, barrier *sync, index_vert_array<T> *vert_index,
    segment_config<VA>* seg_config, int *status, T t_edge, in_edge t_in_edge, update<U> t_update, Fog_program<VA,U,T> *alg_ptr)
{
    u32_t local_term_vert_off, local_start_vert_off;
    sync->wait();

    switch( engine_state ){
        case INIT:
        {
            if (alg_ptr->init_sched == false)
            {
                init_param* p_init_param = (init_param*) state_param;

                if( processor_id*seg_config->partition_cap > p_init_param->num_of_vertices ) break;

                //compute loca_start_vert_id and local_term_vert_id
                local_start_vert_off = processor_id*(seg_config->partition_cap);

                if ( ((processor_id+1)*seg_config->partition_cap-1) > p_init_param->num_of_vertices )
                    local_term_vert_off = p_init_param->num_of_vertices - 1;
                else
                    local_term_vert_off = local_start_vert_off + seg_config->partition_cap - 1;

                //Note: for alg_ptr->init, the vertex id and VA* address does not mean the same offset!
                for (u32_t i=local_start_vert_off; i<=local_term_vert_off; i++ )
                    alg_ptr->init( p_init_param->start_vert_id + i, (VA*)(p_init_param->attr_buf_head) + i, vert_index);
                break;
            }
            else
            {
                assert(alg_ptr->init_sched == true);
                init_param * p_init_param = (init_param *)state_param;
                //if( processor_id*seg_config->partition_cap > p_init_param->num_of_vertices ) break;
                //modify by Huiming Lv
                //I don't know why the previous author doing this
                //2015-10-24
                if( processor_id*seg_config->partition_cap > seg_config->segment_cap ) break;

                u32_t current_start_id = p_init_param->start_vert_id + processor_id;
                u32_t current_term_id = p_init_param->start_vert_id + p_init_param->num_of_vertices;

                for (u32_t i=current_start_id ; i<current_term_id; i+=gen_config.num_processors)
                {
                    assert((i-processor_id)%gen_config.num_processors == 0);
                    assert(i <= gen_config.max_vert_id);
                    u32_t index = 0;
                    if (seg_config->num_segments > 1)
                        index = i%seg_config->segment_cap;
                    else
                        index = i;

                    assert(index <= seg_config->segment_cap);
                    alg_ptr->init( i, (VA*)(p_init_param->attr_buf_head) + index, vert_index);
                }
                break;

            }
        }
        case TARGET_SCATTER:
        {
            *status = FINISHED_SCATTER;
            scatter_param * p_scatter_param = (scatter_param *)state_param;
            sched_bitmap_manager * my_sched_bitmap_manager;
            struct context_data * my_context_data;
            update_map_manager * my_update_map_manager;
            u32_t my_strip_cap, per_cpu_strip_cap;
            u32_t * my_update_map_head;

            VA * attr_array_head;
            //update<VA> * my_update_buf_head;
            update<U> * my_update_buf_head;
            //update<U> * t_update;

            //bitmap * next_bitmap = NULL;
           // context_data * next_context_data = NULL;

            //T * t_edge;
            //in_edge * t_in_edge;
            u32_t num_edges;
            u32_t strip_num, cpu_offset, map_value, update_buf_offset;
            bitmap * current_bitmap = NULL ;
            u32_t signal_to_scatter;
            u32_t old_edge_id;
            u32_t max_vert = 0, min_vert = 0;
            bool vertex_in_attrbuf = false;

            my_sched_bitmap_manager = seg_config->per_cpu_info_list[processor_id]->target_sched_manager;
            my_update_map_manager = seg_config->per_cpu_info_list[processor_id]->update_manager;

            my_strip_cap = seg_config->per_cpu_info_list[processor_id]->strip_cap;
            per_cpu_strip_cap = my_strip_cap/gen_config.num_processors;
            my_update_map_head = my_update_map_manager->update_map_head;
            my_update_buf_head = (update<U> *)(seg_config->per_cpu_info_list[processor_id]->strip_buf_head);

            attr_array_head = (VA *)p_scatter_param->attr_array_head;


            my_context_data = p_scatter_param->PHASE > 0 ? my_sched_bitmap_manager->p_context_data1:
                my_sched_bitmap_manager->p_context_data0;

            signal_to_scatter = my_context_data->signal_to_scatter;
            if (signal_to_scatter == NORMAL_SCATTER || signal_to_scatter == CONTEXT_SCATTER)
            {
                current_bitmap = my_context_data->p_bitmap;
                max_vert = my_context_data->per_max_vert_id;
                min_vert = my_context_data->per_min_vert_id;
                if (signal_to_scatter == NORMAL_SCATTER && alg_ptr->set_forward_backward == true
                        && alg_ptr->forward_backward_phase == FORWARD_TRAVERSAL)
                {
                    //PRINT_DEBUG("set for backward traversal\n");
                    my_context_data->alg_per_max_vert_id = my_context_data->per_max_vert_id;
                    my_context_data->alg_per_min_vert_id = my_context_data->per_min_vert_id;
                    my_context_data->alg_per_bits_true_size = my_context_data->per_bits_true_size;
                }
            }
            if (signal_to_scatter == STEAL_SCATTER || signal_to_scatter == SPECIAL_STEAL_SCATTER)
            {
                current_bitmap = my_context_data->p_bitmap_steal;
                max_vert = my_context_data->steal_max_vert_id;
                min_vert = my_context_data->steal_min_vert_id;
                if (my_context_data->steal_special_signal == true)
                {
                    *status = FINISHED_SCATTER;
                    break;
                }
            }

            if (my_context_data->per_bits_true_size == 0 &&
                    signal_to_scatter != STEAL_SCATTER && signal_to_scatter != SPECIAL_STEAL_SCATTER)
            {
                *status = FINISHED_SCATTER;
                //PRINT_DEBUG("Processor %d Finished scatter, has %d bits to scatter!\n", processor_id,
                //       my_context_data->per_bits_true_size);
                break;
            }


            for (u32_t i = min_vert; i <= max_vert; i = i + gen_config.num_processors)
            {
                if (current_bitmap->get_value(i) == 0)
                    continue;

                if (alg_ptr->forward_backward_phase == FORWARD_TRAVERSAL)
                    num_edges = vert_index->num_edges(i, OUT_EDGE);
                else
                {
                    assert(alg_ptr->forward_backward_phase == BACKWARD_TRAVERSAL);
                    num_edges = vert_index->num_edges(i, IN_EDGE);
                }
                ///num_out_edges = vert_index->num_out_edges(i);
                //if (num_out_edges == 0 )
                if (num_edges == 0 )
                {
                    //if (seg_config->num_segments == 1 &&
                    //        ( engine_state == SCC_BACKWARD_SCATTER || engine_state == SCC_FORWARD_SCATTER))
                    //    alg_ptr->set_finish_to_vert(i, (VA*)&attr_array_head[i]);
                    if ((alg_ptr->set_forward_backward == true && alg_ptr->forward_backward_phase == BACKWARD_TRAVERSAL)
                           || alg_ptr->set_forward_backward == false)
                        current_bitmap->clear_value(i);

                    if (signal_to_scatter == STEAL_SCATTER || signal_to_scatter == SPECIAL_STEAL_SCATTER)
                        my_context_data->steal_bits_true_size++;
                    else
                        my_context_data->per_bits_true_size--;
                    continue;
                }

                if ((signal_to_scatter == CONTEXT_SCATTER) && (i == my_context_data->per_min_vert_id))
                    old_edge_id = my_context_data->per_num_edges;
                else if ((signal_to_scatter == SPECIAL_STEAL_SCATTER) && (my_context_data->steal_min_vert_id == i))
                    old_edge_id = my_context_data->steal_context_edge_id;
                else
                    old_edge_id = 0;

                //modify by lvhuiming
                //date:2015-1-23
                //if i is in the attr_buf, use the attr_buf's value
                //because attr_buf's value is newer than mmap's value
                //int seg_id = VID_TO_SEGMENT(i);
                strip_num = VID_TO_SEGMENT(i);
                vertex_in_attrbuf = false;
                if((int)strip_num == seg_config->buf0_holder)
                {
                    attr_array_head = (VA *)seg_config->attr_buf0;
                    vertex_in_attrbuf = true;
                }
                else if((int)strip_num == seg_config->buf1_holder)
                {
                    attr_array_head = (VA *)seg_config->attr_buf1;
                    vertex_in_attrbuf = true;
                }
                else
                {
                    attr_array_head = (VA *)p_scatter_param->attr_array_head;
                }
                //modify end

                //bool will_be_updated = false;
                //if (engine_state == CC_SCATTER)
                //    old_edge_id = 0;
                for (u32_t z = old_edge_id; z < num_edges; z++)
                {
                    if (alg_ptr->forward_backward_phase == FORWARD_TRAVERSAL)
                    {
                        //t_edge = vert_index->get_out_edge(i, z);
                        vert_index->get_out_edge(i, z, t_edge);
                        if (t_edge.get_dest_value() == i)
                        {
                            //delete t_edge;
                            continue;
                        }
                        //assert(t_edge);//Make sure this edge existd!

                        //modify by lvhuiming
                        //date:2015-1-23
                        if (vertex_in_attrbuf)
                        {
                            u32_t id_in_buf = i % seg_config->segment_cap;
                            alg_ptr->scatter_one_edge((VA *)&attr_array_head[id_in_buf], t_edge, i, t_update);
                            //t_update = alg_ptr->scatter_one_edge((VA *)&attr_array_head[id_in_buf], t_edge, i);
                        }
                        else
                        {
                            alg_ptr->scatter_one_edge((VA *)&attr_array_head[i], t_edge, i, t_update);
                            //t_update = alg_ptr->scatter_one_edge((VA *)&attr_array_head[i], t_edge, i);
                        }
                        //modify end

                        //assert(t_update);
                        //delete t_edge;
                    }
                    else
                    {
                        assert(alg_ptr->forward_backward_phase == BACKWARD_TRAVERSAL);
                        vert_index->get_in_edge(i, z, t_in_edge);
                        //t_in_edge = vert_index->get_in_edge(i, z);
                        if (t_in_edge.get_src_value() == i)
                        {
                            //delete t_in_edge;
                            continue;
                        }
                        //assert(t_in_edge);//Make sure this edge existd!

                        //modify by lvhuiming
                        //date:2015-1-23
                        if (vertex_in_attrbuf)
                        {
                            u32_t id_in_buf = i % seg_config->segment_cap;
                            alg_ptr->scatter_one_edge((VA *)&attr_array_head[id_in_buf], t_edge, t_in_edge.get_src_value(), t_update);
                            //t_update = alg_ptr->scatter_one_edge((VA *)&attr_array_head[id_in_buf], NULL, t_in_edge.get_src_value());
                        }
                        else
                        {
                            alg_ptr->scatter_one_edge((VA *)&attr_array_head[i], t_edge, t_in_edge.get_src_value(), t_update);
                            //t_update = alg_ptr->scatter_one_edge((VA *)&attr_array_head[i], NULL, t_in_edge.get_src_value());
                        }
                        //modify end
                        //delete t_in_edge;
                    }

                    strip_num = VID_TO_SEGMENT(t_update.dest_vert);
                    /*TBD:gather the update if the dest_vert is in the attr_buf
                    vertex_in_attrbuf = false;
                    if((int)strip_num == seg_config->buf0_holder)
                    {
                        attr_array_head = (VA *)seg_config->attr_buf0;
                        vertex_in_attrbuf = true;
                    }
                    else if((int)strip_num == seg_config->buf1_holder)
                    {
                        attr_array_head = (VA *)seg_config->attr_buf1;
                        vertex_in_attrbuf = true;
                    }
                    else
                    {
                        attr_array_head = (VA *)p_scatter_param->attr_array_head;
                    }
                    */
                    cpu_offset = VID_TO_PARTITION(t_update.dest_vert );
                    assert(strip_num < seg_config->num_segments);
                    assert(cpu_offset < gen_config.num_processors);

                    map_value = *(my_update_map_head + strip_num * gen_config.num_processors + cpu_offset);

                    if (map_value < per_cpu_strip_cap)
                    {

                        update_buf_offset = strip_num * my_strip_cap +
                            map_value * gen_config.num_processors + cpu_offset;

                        *(my_update_buf_head + update_buf_offset) = t_update;
                        map_value++;
                        *(my_update_map_head + strip_num * gen_config.num_processors + cpu_offset) = map_value;
                    }
                    else
                    {
                        if (signal_to_scatter == STEAL_SCATTER || signal_to_scatter == SPECIAL_STEAL_SCATTER)
                        {
                            my_context_data->steal_min_vert_id = i;
                            my_context_data->steal_context_edge_id = z;
                            //PRINT_DEBUG("In steal-scatter, update_buffer is fulled, need to store the context data!\n");
                        }
                        else
                        {
                            //PRINT_DEBUG("Update_buffer is fulled, need to store the context data!\n");
                            my_context_data->per_min_vert_id = i;
                            my_context_data->per_num_edges = z;
                            my_context_data->partition_gather_signal = processor_id;//just be different from origin status
                            my_context_data->partition_gather_strip_id = (int)strip_num;//record the strip_id to gather
                        }
                        *status = UPDATE_BUF_FULL;
                        //delete t_update;
                        break;
                    }
                    //delete t_update;
                }
                if (*status == UPDATE_BUF_FULL)
                    break;
                else
                {
                    assert(*status == FINISHED_SCATTER);
                    if ((alg_ptr->set_forward_backward == true && alg_ptr->forward_backward_phase == BACKWARD_TRAVERSAL)
                            || alg_ptr->set_forward_backward == false)
                        current_bitmap->clear_value(i);
                    if (signal_to_scatter == 1 && my_context_data->per_bits_true_size == 0)
                        PRINT_ERROR("i = %d\n", i);
                    if (signal_to_scatter == STEAL_SCATTER || signal_to_scatter == SPECIAL_STEAL_SCATTER)
                    {
                        my_context_data->steal_bits_true_size++;
                    }
                    else
                    {
                        my_context_data->per_bits_true_size--;
                    }

                }

            }

            if (*status == UPDATE_BUF_FULL)
            {
                if (signal_to_scatter == STEAL_SCATTER || signal_to_scatter == SPECIAL_STEAL_SCATTER)
                {
                    //PRINT_DEBUG("Steal-cpu %d has scatter %d bits\n", processor_id, my_context_data->steal_bits_true_size);
                }
                else
                {}
                    //PRINT_DEBUG("Processor %d have not finished scatter,  UPDATE_BUF_FULL, has %d bits to scatter!\n", processor_id,
                      //  my_context_data->per_bits_true_size);
            }
            else
            {
                if (signal_to_scatter == STEAL_SCATTER || signal_to_scatter == SPECIAL_STEAL_SCATTER)
                {
                    //PRINT_DEBUG("Steal-cpu %d has scatter %d bits\n", processor_id, my_context_data->steal_bits_true_size);
                }
                else
                {
                    //PRINT_DEBUG("Processor %d Finished scatter, has %d bits to scatter!\n", processor_id,
                      //     my_context_data->per_bits_true_size);
                    if (my_context_data->per_bits_true_size != 0)
                    {
                        PRINT_ERROR("Error, processor %d still has %d bits to scatter!\n", processor_id,
                            my_context_data->per_bits_true_size);
                        my_context_data->per_bits_true_size = 0;
                    }
                    my_context_data->per_max_vert_id = current_bitmap->get_start_vert();
                    my_context_data->per_min_vert_id = current_bitmap->get_term_vert();
                }
                *status = FINISHED_SCATTER;
            }
            break;
        }
        case GLOBAL_SCATTER:
        {
            //u64_t scatter_counts = 0;
            *status = FINISHED_SCATTER;
            scatter_param* p_scatter_param = (scatter_param*) state_param;
            sched_list_context_data* my_sched_list_manager;
            update_map_manager* my_update_map_manager;
            u32_t my_strip_cap, per_cpu_strip_cap;
            u32_t* my_update_map_head;

            VA* attr_array_head;
            update<U>* my_update_buf_head;

            //T * t_edge;
            //update<U> *t_update = NULL;
            u32_t num_out_edges;
            u32_t strip_num, cpu_offset, map_value, update_buf_offset;

            my_sched_list_manager = seg_config->per_cpu_info_list[processor_id]->global_sched_manager;
            my_update_map_manager = seg_config->per_cpu_info_list[processor_id]->update_manager;

            my_strip_cap = seg_config->per_cpu_info_list[processor_id]->strip_cap;
            per_cpu_strip_cap = my_strip_cap/gen_config.num_processors;
            my_update_map_head = my_update_map_manager->update_map_head;

            attr_array_head = (VA*) p_scatter_param->attr_array_head;
            my_update_buf_head =
                (update<U>*)(seg_config->per_cpu_info_list[processor_id]->strip_buf_head);

            u32_t signal_to_scatter = my_sched_list_manager->signal_to_scatter;
            u32_t min_vert = 0, max_vert = 0;
            u32_t old_edge_id;
            if (signal_to_scatter == NORMAL_SCATTER)
            {
                min_vert = my_sched_list_manager->normal_sched_min_vert;
                max_vert = my_sched_list_manager->normal_sched_max_vert;
                //PRINT_DEBUG("cpu %d normal scatter, min_vert = %d, max_vert = %d\n",
                //        processor_id, min_vert, max_vert);
            }
            else if (signal_to_scatter == CONTEXT_SCATTER)
            {
                min_vert = my_sched_list_manager->context_vert_id;
                max_vert = my_sched_list_manager->normal_sched_max_vert;
                //PRINT_DEBUG("cpu %d context scatter, min_vert = %d, max_vert = %d\n",
                 //       processor_id, min_vert, max_vert);
            }
            else if (signal_to_scatter == STEAL_SCATTER || signal_to_scatter == SPECIAL_STEAL_SCATTER)
            {
                min_vert = my_sched_list_manager->context_steal_min_vert;
                max_vert = my_sched_list_manager->context_steal_max_vert;
                //PRINT_DEBUG("cpu %d steal scatter, min_vert = %d, max_vert = %d\n",
                 //       processor_id, min_vert, max_vert);
            }

            if (my_sched_list_manager->num_vert_to_scatter == 0
                    && signal_to_scatter != STEAL_SCATTER && signal_to_scatter != SPECIAL_STEAL_SCATTER)
            {
                *status = FINISHED_SCATTER;
                break;
            }
            if (my_sched_list_manager->context_steal_min_vert == 0 &&
                    my_sched_list_manager->context_steal_max_vert == 0 &&
                    (signal_to_scatter == STEAL_SCATTER || signal_to_scatter == SPECIAL_STEAL_SCATTER))
            {
                *status = FINISHED_SCATTER;
                break;
            }

            //for loop for every vertex in every cpu
            //*********************************
            //if use range scatter:
            //for (u32_t i =min_vert; i <= max_vert; i++)
            //*********************************
            for (u32_t i = min_vert; i <= max_vert; i = i + gen_config.num_processors)
            {

                num_out_edges = vert_index->num_edges(i, OUT_EDGE);

                if (num_out_edges == 0)
                {
                    //different counter for different scatter-mode
                    if (signal_to_scatter == STEAL_SCATTER || signal_to_scatter == SPECIAL_STEAL_SCATTER)
                        my_sched_list_manager->context_steal_num_vert++;
                    else
                        my_sched_list_manager->num_vert_to_scatter--;

                    //jump to next loop
                    continue;
                }

                //set old_edge_id for context-scatter
                if ((signal_to_scatter == CONTEXT_SCATTER) && (i == my_sched_list_manager->context_vert_id))
                {
                    old_edge_id = my_sched_list_manager->context_edge_id;
                }
                else if ((signal_to_scatter == SPECIAL_STEAL_SCATTER) &&(i == my_sched_list_manager->context_steal_min_vert))
                    old_edge_id = my_sched_list_manager->context_steal_edge_id;
                else
                    old_edge_id = 0;

                //modify by lvhuiming
                //date:2015-1-23
                //if i is in the attr_buf, use the attr_buf's value
                //because attr_buf's value is newer than mmap's value
                int seg_id = VID_TO_SEGMENT(i);
                bool vertex_in_attrbuf = false;
                if(seg_id == seg_config->buf0_holder)
                {
                    attr_array_head = (VA *)seg_config->attr_buf0;
                    vertex_in_attrbuf = true;
                }
                else if(seg_id == seg_config->buf1_holder)
                {
                    attr_array_head = (VA *)seg_config->attr_buf1;
                    vertex_in_attrbuf = true;
                }
                else
                {
                    attr_array_head = (VA *)p_scatter_param->attr_array_head;
                }
                //modify end

                //generating updates for each edge of this vertex
                for (u32_t z = old_edge_id; z < num_out_edges; z++)
                {
                    //get edge from vert_index
                    //t_edge = vert_index->get_out_edge(i, z);
                    vert_index->get_out_edge(i, z, t_edge);
                    //Make sure this edge existd!
                    //assert(t_edge);

                    //modify by lvhuiming
                    //date:2015-1-23
                    if (vertex_in_attrbuf)
                    {
                        u32_t id_in_buf = i % seg_config->segment_cap;
                        alg_ptr->scatter_one_edge((VA *)&attr_array_head[id_in_buf], t_edge, num_out_edges, t_update);
                        //t_update = alg_ptr->scatter_one_edge((VA *)&attr_array_head[id_in_buf], t_edge, num_out_edges);
                    }
                    else
                    {
                        alg_ptr->scatter_one_edge((VA*)&attr_array_head[i], t_edge, num_out_edges, t_update);
                        //t_update = alg_ptr->scatter_one_edge((VA*)&attr_array_head[i], t_edge, num_out_edges);
                    }
                    //modify end

                    //assert(t_update);
                    //delete t_edge;

                    //Make sure this update existd!

                    //strip_num = VID_TO_SEGMENT(t_update->dest_vert);
                    //cpu_offset = VID_TO_PARTITION(t_update->dest_vert);
                    strip_num = VID_TO_SEGMENT(t_update.dest_vert);
                    cpu_offset = VID_TO_PARTITION(t_update.dest_vert);
                    //Check for existd!
                    assert(strip_num < seg_config->num_segments);
                    assert(cpu_offset < gen_config.num_processors);

                    //find out the corresponding value for update-buffer
                    map_value = *(my_update_map_head + strip_num * gen_config.num_processors + cpu_offset);

                    if (map_value < (per_cpu_strip_cap - 1))
                    {
                        //scatter_counts++;
                        update_buf_offset = strip_num * my_strip_cap + map_value * gen_config.num_processors + cpu_offset;
                        *(my_update_buf_head + update_buf_offset) = t_update;
                        //*(my_update_buf_head + update_buf_offset) = *t_update;
                        map_value++;
                        *(my_update_map_head + strip_num * gen_config.num_processors + cpu_offset) = map_value;
                    }
                    else
                    {
                        //PRINT_DEBUG("processor %d buf_full\n", processor_id);
                        //There is no space for this update, need to store the context data
                        if (signal_to_scatter == STEAL_SCATTER || signal_to_scatter == SPECIAL_STEAL_SCATTER)
                        {
                            my_sched_list_manager->context_steal_min_vert = i;
                            my_sched_list_manager->context_steal_max_vert = max_vert;
                            my_sched_list_manager->context_steal_edge_id = z;
                            //PRINT_DEBUG("In steal-scatter, update-buf is fulled, need to store the context data!\n");
                            //PRINT_DEBUG("min_vert = %d, max_vert = %d, edge = %d\n", i, max_vert, z);
                        }
                        else
                        {
                            //PRINT_DEBUG("other-scatter, update-buf is fulled, need to store the context data!\n");
                            my_sched_list_manager->context_vert_id = i;
                            my_sched_list_manager->context_edge_id = z;
                            my_sched_list_manager->partition_gather_strip_id = (int)strip_num;
                            //PRINT_DEBUG("vert = %d, edge = %d, strip_num = %d\n", i, z, strip_num);
                        }
                        *status = UPDATE_BUF_FULL;
                        //delete t_update;
                        break;
                    }
                    //delete t_update;
                }
                if (*status == UPDATE_BUF_FULL)
                    break;
                else
                {
                    //need to set the counter
                    if (signal_to_scatter == CONTEXT_SCATTER && my_sched_list_manager->num_vert_to_scatter == 0)
                        PRINT_ERROR("i = %d\n", i);
                    if (signal_to_scatter == STEAL_SCATTER || signal_to_scatter == SPECIAL_STEAL_SCATTER)
                        my_sched_list_manager->context_steal_num_vert++;
                    else
                        my_sched_list_manager->num_vert_to_scatter--;
                }
            }

            if (*status == UPDATE_BUF_FULL)
            {
                if (signal_to_scatter == STEAL_SCATTER || signal_to_scatter == SPECIAL_STEAL_SCATTER)
                    {}
                    //PRINT_DEBUG("Steal-cpu %d has scatter %d vertex\n", processor_id,
                      //      my_sched_list_manager->context_steal_num_vert);
                else{}
                    //PRINT_DEBUG("Processor %d has not finished scatter, has %d vertices to scatter~\n", processor_id,
                      //      my_sched_list_manager->num_vert_to_scatter);
            }
            else
            {
                if (signal_to_scatter == STEAL_SCATTER || signal_to_scatter == SPECIAL_STEAL_SCATTER)
                {}
                    //PRINT_DEBUG("Steal-cpu %d has scatter %d vertex\n", processor_id,
                      //      my_sched_list_manager->context_steal_num_vert);
                else
                {
                    if (my_sched_list_manager->num_vert_to_scatter != 0)
                    {
                        PRINT_ERROR("after scatter, num_vert_to_scatter != 0\n");
                    }
                    //PRINT_DEBUG("Processor %d has finished scatter, and there is %d vertex to scatter\n", processor_id,
                      //      my_sched_list_manager->num_vert_to_scatter);
                }
                *status = FINISHED_SCATTER;
            }
            //PRINT_DEBUG("processor %d, scatter_counts = %lld\n", processor_id, scatter_counts);
            break;
        }
        case GLOBAL_GATHER:
        case TARGET_GATHER:
        {
            gather_param * p_gather_param = (gather_param *)state_param;
            update_map_manager * my_update_map_manager;
            u32_t my_strip_cap;
            u32_t * my_update_map_head;
            VA * attr_array_head;
            update<U> * my_update_buf_head;

            update<U> * t_update_gather;
            u32_t map_value, update_buf_offset;
            u32_t dest_vert;
            int strip_id;
            u32_t threshold;
            u32_t vert_index;

            my_strip_cap = seg_config->per_cpu_info_list[processor_id]->strip_cap;
            attr_array_head = (VA *)p_gather_param->attr_array_head;
            strip_id = p_gather_param->strip_id;
            threshold = p_gather_param->threshold;

            //Traversal all the buffers of each cpu to find the corresponding UPDATES
            for (u32_t buf_id = 0; buf_id < gen_config.num_processors; buf_id++)
            {
                my_update_map_manager = seg_config->per_cpu_info_list[buf_id]->update_manager;
                my_update_map_head = my_update_map_manager->update_map_head;
                my_update_buf_head = (update<U> *)(seg_config->per_cpu_info_list[buf_id]->strip_buf_head);
                map_value = *(my_update_map_head + strip_id * gen_config.num_processors + processor_id);
                if (map_value == 0)
                    continue;

                for (u32_t update_id = 0; update_id < map_value; update_id++)
                {
                    update_buf_offset = strip_id * my_strip_cap + update_id * gen_config.num_processors + processor_id;

                    t_update_gather = (my_update_buf_head + update_buf_offset);
                    assert(t_update_gather);
                    dest_vert = t_update_gather->dest_vert;
                    if (threshold == 1)
                        vert_index = dest_vert%seg_config->segment_cap;
                    else
                        vert_index = dest_vert;

                        alg_ptr->gather_one_update(dest_vert, (VA *)&attr_array_head[vert_index], t_update_gather);
                }
                map_value = 0;
                *(my_update_map_head + strip_id * gen_config.num_processors + processor_id) = 0;
            }
            break;
        }
        case CREATE_SUBTASK_DATASET:
        {
            for(u32_t bag_id = processor_id; bag_id < task_bag_config_vec.size(); bag_id = bag_id + gen_config.num_processors)
            {
                if(task_bag_config_vec[bag_id].data_size==0)
                {
                    //std::cout<<"bag" <<task_bag_config_vec[bag_id].bag_id<<" is empty "<<std::endl;
                    continue;
                }
                //std::cout<<"bag" <<task_bag_config_vec[bag_id].bag_id<<" is processing in CPU "<<processor_id<<std::endl;

                bool with_type1 = false;
                if(sizeof(T)==sizeof(type1_edge))
                {
                    with_type1 = true;
                }
                bool with_in_edge = gen_config.with_in_edge;

                struct mmap_config remap_array_map_config;
                remap_array_map_config = mmap_file(task_bag_config_vec[bag_id].data_name);

                u32_t * remap_array_header = (u32_t * )remap_array_map_config.mmap_head;

                struct convert::edge * REMAP_edge_buffer = NULL;
                struct convert::type2_edge * type2_REMAP_edge_buffer = NULL;
                if(with_type1)
                {
                    REMAP_edge_buffer = new struct convert::edge[REMAP_EDGE_BUFFER_LEN];
                    memset((char*)REMAP_edge_buffer, 0, REMAP_EDGE_BUFFER_LEN*sizeof(convert::edge));
                }
                else
                {
                    type2_REMAP_edge_buffer = new struct convert::type2_edge[REMAP_EDGE_BUFFER_LEN];
                    memset((char*)type2_REMAP_edge_buffer, 0, REMAP_EDGE_BUFFER_LEN*sizeof(convert::type2_edge));
                }

                struct convert::vert_index * REMAP_vert_buffer = new struct convert::vert_index[REMAP_VERT_BUFFER_LEN];
                memset((char*)REMAP_vert_buffer, 0, REMAP_VERT_BUFFER_LEN*sizeof(convert::vert_index));

                struct convert::in_edge * REMAP_in_edge_buffer = NULL;
                struct convert::vert_index * REMAP_in_vert_buffer = NULL;
                if(with_in_edge)
                {
                    REMAP_in_edge_buffer = new struct convert::in_edge[REMAP_EDGE_BUFFER_LEN];
                    REMAP_in_vert_buffer = new struct convert::vert_index[REMAP_VERT_BUFFER_LEN];
                    memset((char*)REMAP_in_edge_buffer, 0, REMAP_EDGE_BUFFER_LEN*sizeof(convert::in_edge));
                    memset((char*)REMAP_in_vert_buffer, 0, REMAP_VERT_BUFFER_LEN*sizeof(convert::vert_index));
                }
                //std::cout<<"memset ok\n";

                int REMAP_edge_fd = 0;
                int REMAP_index_fd = 0;
                int REMAP_in_edge_fd = 0;
                int REMAP_in_index_fd = 0;
                std::ofstream REMAP_desc_ofstream;

                std::string temp_file_name      = task_bag_config_vec[bag_id].data_name;
                std::string REMAP_edge_file     = temp_file_name.substr(0, temp_file_name.find_last_of(".")) + ".edge"    ;
                std::string REMAP_index_file    = temp_file_name.substr(0, temp_file_name.find_last_of(".")) + ".index"   ;
                std::string REMAP_in_edge_file  = temp_file_name.substr(0, temp_file_name.find_last_of(".")) + ".in-edge" ;
                std::string REMAP_in_index_file = temp_file_name.substr(0, temp_file_name.find_last_of(".")) + ".in-index";
                std::string REMAP_desc_file     = temp_file_name.substr(0, temp_file_name.find_last_of(".")) + ".desc"    ;

                //std::cout<<REMAP_edge_file<<std::endl;
                //std::cout<<REMAP_index_file<<std::endl;
                //std::cout<<REMAP_in_edge_file<<std::endl;
                //std::cout<<REMAP_in_index_file<<std::endl;
                //std::cout<<REMAP_desc_file<<std::endl;


                REMAP_edge_fd = open(REMAP_edge_file.c_str(), O_CREAT|O_WRONLY, S_IRUSR);
                if(REMAP_edge_fd == -1)
                {
                    printf("Cannot create REMAP_edge_file:%s\nAborted..\n", REMAP_edge_file.c_str());
                    exit(-1);
                }
                REMAP_index_fd = open(REMAP_index_file.c_str(), O_CREAT|O_WRONLY, S_IRUSR);
                if(REMAP_index_fd == -1)
                {
                    printf("Cannot create REMAP_index_file:%s\nAborted..\n", REMAP_index_file.c_str());
                    exit(-1);
                }
                if(with_in_edge)
                {
                    REMAP_in_edge_fd = open(REMAP_in_edge_file.c_str(), O_CREAT|O_WRONLY, S_IRUSR);
                    if(REMAP_in_edge_fd == -1)
                    {
                        printf("Cannot create REMAP_in_edge_file:%s\nAborted..\n", REMAP_in_edge_file.c_str());
                        exit(-1);
                    }
                    REMAP_in_index_fd = open(REMAP_in_index_file.c_str(), O_CREAT|O_WRONLY, S_IRUSR);
                    if(REMAP_in_index_fd == -1)
                    {
                        printf("Cannot create REMAP_in_index_file:%s\nAborted..\n", REMAP_in_index_file.c_str());
                        exit(-1);
                    }
                }

                //std::cout<<"open file OK!\n";

                //u64_t REMAP_vert_num = 0;
                u32_t REMAP_vert_suffix = 0;
                u32_t REMAP_vert_buffer_offset = 0;
                u64_t REMAP_edge_num = 0;
                u32_t REMAP_edge_suffix = 0;
                u32_t REMAP_edge_buffer_offset = 0;
                u64_t recent_REMAP_edge_num = 0;
                u64_t REMAP_in_edge_num = 0;
                u32_t REMAP_in_edge_suffix = 0;
                u32_t REMAP_in_edge_buffer_offset = 0;
                u64_t recent_REMAP_in_edge_num = 0;

                T temp_out_edge;
                fog::in_edge temp_in_edge;

                u32_t temp_for_degree = 0;
                u32_t temp_for_dst_or_src = 0;

                u32_t old_vert_id = 0;
                u32_t new_vert_id = 0;

                int left = 0;
                int right = task_bag_config_vec[bag_id].data_size - 1;
                //int right = vert_bag_config->data_size;

                //***************second solution
                /*
                u32_t * location = new u32_t[gen_config.max_vert_id + 1];
                for(u32_t s = 0; s <= gen_config.max_vert_id; s++)
                {
                    location[s] = UINT_MAX;
                }
                for(int j = 0; j <= right; j++)
                {
                    location[remap_array_header[j]] = j;
                }
                */

                //for (int i = 0; i < vert_bag_config->data_size; i++ )
                //for (int i = 0; i < task_bag_config_vec[bag_id].data_size; i++ )
                for (int i = 0; i <= right ; i++ )
                {
                    //std::cout<<i<<std::endl;
                    //REMAP_vert_num++;
                    old_vert_id = remap_array_header[i];
                    temp_for_degree = vert_index->num_edges(old_vert_id, OUT_EDGE);
                    for(u32_t j = 0; j < temp_for_degree; j++)
                    {
                        //temp_out_edge = vert_index->get_out_edge(old_vert_id, j);
                        vert_index->get_out_edge(old_vert_id, j, temp_out_edge);
                        temp_for_dst_or_src = temp_out_edge.get_dest_value();
                        new_vert_id = fog_binary_search(remap_array_header, left, right, temp_for_dst_or_src);
                        //new_vert_id = location[temp_for_dst_or_src];
                        if(UINT_MAX != new_vert_id)
                        {
                            REMAP_edge_num++;
                            REMAP_edge_suffix = REMAP_edge_num - (REMAP_edge_buffer_offset*REMAP_EDGE_BUFFER_LEN);
                            if(with_type1)
                            {
                                REMAP_edge_buffer[REMAP_edge_suffix].dest_vert = new_vert_id;
                                REMAP_edge_buffer[REMAP_edge_suffix].edge_weight = temp_out_edge.get_edge_value();
                            }
                            else
                            {
                                type2_REMAP_edge_buffer[REMAP_edge_suffix].dest_vert = new_vert_id;
                            }

                            if(REMAP_edge_suffix == REMAP_EDGE_BUFFER_LEN-1)
                            {
                                if(with_type1)
                                {
                                    flush_buffer_to_file(REMAP_edge_fd, (char*)REMAP_edge_buffer,
                                            REMAP_EDGE_BUFFER_LEN*sizeof(convert::edge));
                                    memset((char*)REMAP_edge_buffer, 0, REMAP_EDGE_BUFFER_LEN*sizeof(convert::edge));
                                }
                                else
                                {
                                    flush_buffer_to_file(REMAP_edge_fd, (char*)type2_REMAP_edge_buffer,
                                            REMAP_EDGE_BUFFER_LEN*sizeof(convert::type2_edge));
                                    memset((char*)type2_REMAP_edge_buffer, 0, REMAP_EDGE_BUFFER_LEN*sizeof(convert::type2_edge));
                                }

                                REMAP_edge_buffer_offset++;
                            }
                        }
                    }

                    REMAP_vert_suffix = i - REMAP_vert_buffer_offset*REMAP_VERT_BUFFER_LEN;

                    if(REMAP_edge_num != recent_REMAP_edge_num)
                    {
                        REMAP_vert_buffer[REMAP_vert_suffix].offset = recent_REMAP_edge_num+1;
                        recent_REMAP_edge_num = REMAP_edge_num;
                    }
                    /* debug
                       if(this->task_id==6)
                       {
                       PRINT_DEBUG_TEST_LOG("task_id = %d, vid = %d, offset = %llu\n", this->task_id, i, REMAP_vert_buffer[REMAP_vert_suffix].offset);
                       }
                       */

                    if(with_in_edge)
                    {
                        temp_for_degree = vert_index->num_edges(old_vert_id, IN_EDGE);
                        for(u32_t j = 0; j < temp_for_degree; j++)
                        {
                            //temp_in_edge = vert_index->get_in_edge(old_vert_id, j);
                            vert_index->get_in_edge(old_vert_id, j, temp_in_edge);
                            //lvhuiming debug
                            /*
                               if(this->get_task_id() == 6 && i==42014)
                               {
                               std::cout<<"task 6, vert:"<<remap_array_header[42014]<<" indegree = "<<temp_for_degree<<std::endl;
                               temp_for_dst_or_src = temp_in_edge->get_src_value();
                               new_vert_id = fog_binary_search(remap_array_header, left, right, temp_for_dst_or_src);
                               std::cout<<"old desc: "<<temp_for_dst_or_src<<std::endl;
                               std::cout<<"new desc: "<<new_vert_id<<std::endl;
                               }
                               */
                            //debug end
                            temp_for_dst_or_src = temp_in_edge.get_src_value();
                            new_vert_id = fog_binary_search(remap_array_header, left, right, temp_for_dst_or_src);
                            //new_vert_id = location[temp_for_dst_or_src];
                            if(UINT_MAX != new_vert_id)
                            {
                                //lvhuiming debug
                                /*
                                   if(this->get_task_id() == 6)
                                   {
                                   fprintf(debug_in_edge_file, "%d\t%d\n", temp_for_dst_or_src, old_vert_id);
                                   }
                                   */
                                //debug end
                                REMAP_in_edge_num++;
                                REMAP_in_edge_suffix = REMAP_in_edge_num - REMAP_in_edge_buffer_offset*REMAP_EDGE_BUFFER_LEN;
                                REMAP_in_edge_buffer[REMAP_in_edge_suffix].in_vert = new_vert_id;
                                if(REMAP_in_edge_suffix == REMAP_EDGE_BUFFER_LEN-1)
                                {
                                    flush_buffer_to_file(REMAP_in_edge_fd, (char*)REMAP_in_edge_buffer,
                                            REMAP_EDGE_BUFFER_LEN*sizeof(convert::in_edge));
                                    memset((char*)REMAP_in_edge_buffer, 0, REMAP_EDGE_BUFFER_LEN*sizeof(convert::in_edge));
                                    REMAP_in_edge_buffer_offset++;
                                }
                            }
                        }
                        if(REMAP_in_edge_num != recent_REMAP_in_edge_num)
                        {
                            REMAP_in_vert_buffer[REMAP_vert_suffix].offset = recent_REMAP_in_edge_num+1;
                            recent_REMAP_in_edge_num = REMAP_in_edge_num;
                        }
                        /* debug
                           if(this->task_id==6)
                           {
                           PRINT_DEBUG_CV_LOG("task_id = %d, vid = %d, offset = %llu\n", this->task_id, i, REMAP_in_vert_buffer[REMAP_vert_suffix].offset);
                           }
                           */
                    }

                    if(REMAP_vert_suffix == REMAP_VERT_BUFFER_LEN-1)
                    {
                        flush_buffer_to_file(REMAP_index_fd, (char*)REMAP_vert_buffer,
                                REMAP_VERT_BUFFER_LEN*sizeof(convert::vert_index));
                        memset((char*)REMAP_vert_buffer, 0, REMAP_VERT_BUFFER_LEN*sizeof(convert::vert_index));

                        if(with_in_edge)
                        {
                            flush_buffer_to_file(REMAP_in_index_fd, (char*)REMAP_in_vert_buffer,
                                    REMAP_VERT_BUFFER_LEN*sizeof(convert::vert_index));
                            memset((char*)REMAP_in_vert_buffer, 0, REMAP_VERT_BUFFER_LEN*sizeof(convert::vert_index));
                        }

                        REMAP_vert_buffer_offset++;
                    }
                }
                //delete location;

                //std::cout<<"for over!\n";


                if(with_type1)
                {
                    //flush_buffer_to_file(REMAP_edge_fd, (char*)REMAP_edge_buffer,
                    //        (REMAP_EDGE_BUFFER_LEN)*sizeof(convert::edge));
                    flush_buffer_to_file(REMAP_edge_fd, (char*)REMAP_edge_buffer,
                            (1 + REMAP_edge_num - REMAP_edge_buffer_offset*REMAP_EDGE_BUFFER_LEN)*sizeof(convert::edge));

                    delete [] REMAP_edge_buffer;

                }
                else
                {
                    //flush_buffer_to_file(REMAP_edge_fd, (char*)type2_REMAP_edge_buffer,
                    //        (REMAP_EDGE_BUFFER_LEN)*sizeof(convert::type2_edge));
                    flush_buffer_to_file(REMAP_edge_fd, (char*)type2_REMAP_edge_buffer,
                            (1 + REMAP_edge_num - REMAP_edge_buffer_offset*REMAP_EDGE_BUFFER_LEN)*sizeof(convert::type2_edge));

                    delete [] type2_REMAP_edge_buffer;
                }

                flush_buffer_to_file(REMAP_index_fd, (char*)REMAP_vert_buffer,
                        (task_bag_config_vec[bag_id].data_size - REMAP_vert_buffer_offset*REMAP_VERT_BUFFER_LEN)*sizeof(convert::vert_index));
                delete [] REMAP_vert_buffer;

                close(REMAP_edge_fd);
                close(REMAP_index_fd);

                if(with_in_edge)
                {
                    //flush_buffer_to_file(REMAP_in_edge_fd, (char*)REMAP_in_edge_buffer,
                    //        (REMAP_EDGE_BUFFER_LEN)*sizeof(convert::in_edge));
                    flush_buffer_to_file(REMAP_in_edge_fd, (char*)REMAP_in_edge_buffer,
                            (1 + REMAP_in_edge_num - REMAP_in_edge_buffer_offset*REMAP_EDGE_BUFFER_LEN)*sizeof(convert::in_edge));
                    flush_buffer_to_file(REMAP_in_index_fd, (char*)REMAP_in_vert_buffer,
                            (task_bag_config_vec[bag_id].data_size - REMAP_vert_buffer_offset*REMAP_VERT_BUFFER_LEN)*sizeof(convert::vert_index));

                    delete [] REMAP_in_edge_buffer;
                    delete [] REMAP_in_vert_buffer;

                    close(REMAP_in_edge_fd);
                    close(REMAP_in_index_fd);
                }

                PRINT_DEBUG_LOG("REMAP: vert_num:%d   edge_num = %lld,  in_edge_num = %lld\n", task_bag_config_vec[bag_id].data_size, REMAP_edge_num, REMAP_in_edge_num);
                //std::cout<<"REMAP: vert_num: "<<vert_bag_config->data_size<<" edge_num:"<<REMAP_edge_num<<" in_edge_num:"<<REMAP_in_edge_num<<std::endl;

                REMAP_desc_ofstream.open(REMAP_desc_file.c_str());
                REMAP_desc_ofstream << "[description]\n";
                REMAP_desc_ofstream << "min_vertex_id = " << 0 << "\n";
                REMAP_desc_ofstream << "max_vertex_id = " << (task_bag_config_vec[bag_id].data_size-1) << "\n";
                REMAP_desc_ofstream << "num_of_edges = " << REMAP_edge_num << "\n";
                //REMAP_desc_ofstream << "max_out_edges = " <<  << "\n";
                if(with_type1)
                {
                    REMAP_desc_ofstream << "edge_type = " << 1 << "\n";
                }
                else
                {
                    REMAP_desc_ofstream << "edge_type = " << 2 << "\n";
                }
                REMAP_desc_ofstream << "with_in_edge = " << with_in_edge << "\n";
                REMAP_desc_ofstream.close();


                //unmap_vert_remap_file();
                unmap_file(remap_array_map_config);
                std::cout<<"bag_id = "<<bag_id<<", remap over!"<<std::endl;


                //return REMAP_desc_file;

            }
            break;
        }
        case TARGET_UPDATE_VERTICES:
        {
            update_vertices_param * p_update_vertices_param = (update_vertices_param *)state_param;
            u32_t segment_id     = p_update_vertices_param->strip_id;
            u32_t threshold      = p_update_vertices_param->threshold;
            //VA * attr_array_head = (VA *)p_update_vertices_param->attr_array_head;
            VA * attr_buf_head   = (VA *)p_update_vertices_param->attr_buf_head;
            u32_t v_index;


            struct sched_bitmap_manager * my_sched_bitmap_manager = seg_config->per_cpu_info_list[processor_id]->target_sched_manager;
            struct context_data * my_context_data = p_update_vertices_param->PHASE > 0 ? my_sched_bitmap_manager->p_context_data1:
                            my_sched_bitmap_manager->p_context_data0;
            struct bitmap * current_bitmap = my_context_data->p_bitmap;

            u32_t curr_segment_min_vert = processor_id + ( (0==segment_id) ? 0 : segment_id ) * seg_config->segment_cap;
            u32_t curr_segment_max_vert = (seg_config->num_segments-1 == segment_id) ? gen_config.max_vert_id : (segment_id+1) * seg_config->segment_cap - 1;
            u32_t min_vert = my_context_data->per_min_vert_id > curr_segment_min_vert ? my_context_data->per_min_vert_id : curr_segment_min_vert;
            u32_t max_vert = my_context_data->per_max_vert_id < curr_segment_max_vert ? my_context_data->per_max_vert_id : curr_segment_max_vert;
            //u32_t min_vert = curr_segment_min_vert;
            //u32_t max_vert = curr_segment_max_vert;

            /*
            PRINT_DEBUG("processing the %u segment\n", segment_id);
            PRINT_DEBUG("segment_cap = %u\n", seg_config->segment_cap);
            PRINT_DEBUG("min_vert = %u\n", min_vert);
            PRINT_DEBUG("max_vert = %u\n", max_vert);
            */
            //Traversal each vertex in this segment to update its value
            for(u32_t vid = min_vert; vid <= max_vert; vid += gen_config.num_processors){
                if (0==current_bitmap->get_value(vid)){
                    continue;
                }
                if(1==threshold){
                    v_index = vid % seg_config->segment_cap;
                }
                else{
                    v_index = vid;
                }
                alg_ptr->update_vertex(vid, (VA *)&attr_buf_head[v_index], vert_index);
                current_bitmap->clear_value(vid);
                my_context_data->per_bits_true_size--;
            }
            //assert(my_context_data->per_bits_true_size==0);
            break;
        }
        case VOTE_TO_HALT_UPDATE_VERTICES:
        {
            update_vertices_param * p_update_vertices_param = (update_vertices_param *)state_param;
            u32_t segment_id     = p_update_vertices_param->strip_id;
            u32_t threshold      = p_update_vertices_param->threshold;
            //VA * attr_array_head = (VA *)p_update_vertices_param->attr_array_head;
            VA * attr_buf_head   = (VA *)p_update_vertices_param->attr_buf_head;
            u32_t v_index;


            struct sched_bitmap_manager * my_sched_bitmap_manager = seg_config->per_cpu_info_list[processor_id]->target_sched_manager;
            struct context_data * my_context_data = p_update_vertices_param->PHASE > 0 ? my_sched_bitmap_manager->p_context_data1:
                            my_sched_bitmap_manager->p_context_data0;
            struct bitmap * current_bitmap = my_context_data->p_bitmap;

            u32_t curr_segment_min_vert = processor_id + ( (0==segment_id) ? 0 : segment_id ) * seg_config->segment_cap;
            u32_t curr_segment_max_vert = (seg_config->num_segments-1 == segment_id) ? gen_config.max_vert_id : (segment_id+1) * seg_config->segment_cap - 1;
            u32_t min_vert = my_context_data->per_min_vert_id > curr_segment_min_vert ? my_context_data->per_min_vert_id : curr_segment_min_vert;
            u32_t max_vert = my_context_data->per_max_vert_id < curr_segment_max_vert ? my_context_data->per_max_vert_id : curr_segment_max_vert;
            //u32_t min_vert = curr_segment_min_vert;
            //u32_t max_vert = curr_segment_max_vert;

            /*
            PRINT_DEBUG("processing the %u segment\n", segment_id);
            PRINT_DEBUG("segment_cap = %u\n", seg_config->segment_cap);
            PRINT_DEBUG("min_vert = %u\n", min_vert);
            PRINT_DEBUG("max_vert = %u\n", max_vert);
            */
            //Traversal each vertex in this segment to update its value
            for(u32_t vid = min_vert; vid <= max_vert; vid += gen_config.num_processors){
                if (0==current_bitmap->get_value(vid)){
                    continue;
                }
                if(1==threshold){
                    v_index = vid % seg_config->segment_cap;
                }
                else{
                    v_index = vid;
                }
                alg_ptr->update_vertex(vid, (VA *)&attr_buf_head[v_index], vert_index);
                //current_bitmap->clear_value(vid);
                //my_context_data->per_bits_true_size--;
            }
            break;
        }
        default:
        printf( "Unknow fog engine state is encountered\n" );
    }

    sync->wait();
}

    template <typename VA, typename U, typename T>
void cpu_work<VA, U, T>::show_update_map( int processor_id, segment_config<VA>* seg_config, u32_t* map_head )
{
    //print title
    PRINT_SHORT( "--------------- update map of CPU%d begin-----------------\n", processor_id );
    PRINT_SHORT( "\t" );
    for( u32_t i=0; i<gen_config.num_processors; i++ )
        PRINT_SHORT( "\tCPU%d", i );
    PRINT_SHORT( "\n" );

    for( u32_t i=0; i<seg_config->num_segments; i++ ){
        PRINT_SHORT( "Strip%d\t\t", i );
        for( u32_t j=0; j<gen_config.num_processors; j++ )
            PRINT_SHORT( "%d\t", *(map_head+i*(gen_config.num_processors)+j) );
        PRINT_SHORT( "\n" );
    }
    PRINT_SHORT( "--------------- update map of CPU%d end-----------------\n", processor_id );
}

//impletation of cpu_thread
    template <typename VA, typename U, typename T>
    cpu_thread<VA, U, T>::cpu_thread(u32_t processor_id_in, index_vert_array<T> * vert_index_in, segment_config<VA>* seg_config_in, Fog_program<VA,U,T> * alg_ptr )
:processor_id(processor_id_in), vert_index(vert_index_in), seg_config(seg_config_in), m_alg_ptr(alg_ptr)
{
    if(sync == NULL) { //as it is shared, be created for one time
        sync = new barrier(gen_config.num_processors);
    }
}

    template <typename VA, typename U, typename T>
void cpu_thread<VA, U, T>::operator() ()
{
    do{
        sync->wait();
        if(terminate) {
            break;
        }
        else {
            //PRINT_DEBUG("Before operator, this is processor:%ld\n", processor_id);
            sync->wait();
            (*work_to_do)(processor_id, sync, vert_index, seg_config, &status, t_edge, t_in_edge, t_update, m_alg_ptr);

            sync->wait(); // Must synchronize before p0 exits (object is on stack)
        }
    }while(processor_id != 0);
}

    template <typename VA, typename U, typename T>
sched_task* cpu_thread<VA, U, T>::get_sched_task()
{return NULL;}

    template <typename VA, typename U, typename T>
void cpu_thread<VA, U, T>::browse_sched_list()
{}
