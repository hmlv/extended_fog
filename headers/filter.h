/**************************************************************************************************
 * Authors: 
 *   Huiming Lv 
 *
 * Declaration:
 *   The object for filter 
 *************************************************************************************************/

#ifndef __FILTER_H__
#define __FILTER_H__

#include "index_vert_array.hpp"
//#include "../fogsrc/index_vert_array.cpp"

//template<typename VA, typename T>
template<typename VA>
class Filter
{
    public:
        //void do_scc_filter(VA * va, index_vert_array<T> * vertex_index, int task_id);
        void do_scc_filter(VA * va, int task_id);
        //void do_trim_filter(VA * va, index_vert_array<T> * vertex_index, int task_id); 
        void do_trim_filter(VA * va, int task_id); 
};

#endif
		
