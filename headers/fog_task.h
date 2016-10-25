/**************************************************************************************************
 * Authors: 
 *   Huiming Lv 
 *
 * Declaration:
 *
 *************************************************************************************************/

#ifndef __FOG_TASK_H__
#define __FOG_TASK_H__

#include <vector>
#include "fog_program.h"

template<typename VA, typename U ,typename T> 
class Fog_task
{
        Fog_program<VA, U, T> * m_task_alg_ptr;
        int task_id;
        int remap_fd;
        u64_t remap_file_length;
        u32_t * remap_array_header;

    public:
        struct task_config * m_task_config;

        Fog_task();
        std::string create_dataset_sequence(struct bag_config * vert_bag_config, index_vert_array<T> * vertex_index);
        std::string create_dataset(struct bag_config * vert_bag_config, index_vert_array<T> * vertex_index);
        void set_alg_ptr(Fog_program<VA, U, T> * alg_ptr);
        Fog_program<VA, U, T> * get_alg_ptr();
        void set_task_id(int t_id);
        int get_task_id();
        void set_task_config(struct task_config * in_task_config);
        struct task_config * get_task_config();

        char * map_vert_remap_file(std::string file_name);
        void unmap_vert_remap_file(); 
};

template<typename D>
extern u32_t fog_binary_search(D * array, int left, int right, D key);

template<typename D>
u32_t fog_sequence_search(D * array, int left, int right, D key);

#endif

